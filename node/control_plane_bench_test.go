package node

import (
	"context"
	"crypto"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/discovery"
	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/control"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type controlPlaneWorkload struct {
	Services         int `json:"services"`
	Streams          int `json:"streams"`
	ChangesPerSecond int `json:"target_changes_per_second"`
}

type measurementCounts struct {
	Views         int64 `json:"views"`
	Compiles      int   `json:"compiles"`
	Signatures    int64 `json:"signatures"`
	ResponseBytes int64 `json:"response_proto_bytes"`
	Applies       int64 `json:"applies"`
}

func (c measurementCounts) subtract(old measurementCounts) measurementCounts {
	return measurementCounts{c.Views - old.Views, c.Compiles - old.Compiles, c.Signatures - old.Signatures, c.ResponseBytes - old.ResponseBytes, c.Applies - old.Applies}
}

type controlPlaneMeasurements struct {
	Workload               controlPlaneWorkload `json:"workload"`
	Go                     string               `json:"go"`
	Platform               string               `json:"platform"`
	GOMAXPROCS             int                  `json:"gomaxprocs"`
	Started                time.Time            `json:"started"`
	WatchSubscriptions     int64                `json:"watch_subscriptions"`
	PeakActiveWatches      int64                `json:"peak_active_watches"`
	IdleMilliseconds       float64              `json:"idle_milliseconds"`
	Idle                   measurementCounts    `json:"idle"`
	Changes                measurementCounts    `json:"changes"`
	ActualChangesPerSecond float64              `json:"actual_changes_per_second"`
	AppliedMilliseconds    []float64            `json:"change_to_applied_milliseconds"`
	CompileMilliseconds    []float64            `json:"compile_milliseconds"`
}

type measurementRecorder struct {
	mu         sync.Mutex
	compiles   []float64
	signatures atomic.Int64
	bytes      atomic.Int64
	applies    atomic.Int64
}

func (*measurementRecorder) DiscoveryStreamOpened() func()                    { return func() {} }
func (*measurementRecorder) RecordDiscoveryPush(string, error, time.Duration) {}
func (*measurementRecorder) RecordDiscoveryAck(bool)                          {}
func (m *measurementRecorder) RecordPlanCompile(d time.Duration, err error) {
	if err == nil {
		m.mu.Lock()
		m.compiles = append(m.compiles, float64(d)/float64(time.Millisecond))
		m.mu.Unlock()
	}
}

func (m *measurementRecorder) counts(s *measurementStore) measurementCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	return measurementCounts{s.views.Load(), len(m.compiles), m.signatures.Load(), m.bytes.Load(), m.applies.Load()}
}

type measurementSigner struct {
	crypto.Signer
	count *atomic.Int64
}

func (s measurementSigner) Sign(r io.Reader, digest []byte, opts crypto.SignerOpts) ([]byte, error) {
	s.count.Add(1)
	return s.Signer.Sign(r, digest, opts)
}

type measurementStore struct {
	*memory.Store
	views, subscriptions, active, peak atomic.Int64
}

func (s *measurementStore) View(ctx context.Context, ranges []store.Range) (store.View, error) {
	s.views.Add(1)
	return s.Store.View(ctx, ranges)
}

func (s *measurementStore) WatchRevisions(ctx context.Context, tenant string, kinds ...string) (<-chan store.RevisionBatch, error) {
	batches, err := s.Store.WatchRevisions(ctx, tenant, kinds...)
	if err == nil {
		s.subscriptions.Add(1)
		active := s.active.Add(1)
		for old := s.peak.Load(); active > old; old = s.peak.Load() {
			if s.peak.CompareAndSwap(old, active) {
				break
			}
		}
		go func() { <-ctx.Done(); s.active.Add(-1) }()
	}
	return batches, err
}

func (s *measurementStore) WatchObjects(ctx context.Context, tenant string, kinds ...string) (<-chan store.Event, error) {
	events, err := s.Store.WatchObjects(ctx, tenant, kinds...)
	if err == nil {
		s.subscriptions.Add(1)
		active := s.active.Add(1)
		for old := s.peak.Load(); active > old; old = s.peak.Load() {
			if s.peak.CompareAndSwap(old, active) {
				break
			}
		}
		go func() { <-ctx.Done(); s.active.Add(-1) }()
	}
	return events, err
}

type measuredApplied struct {
	nodeID   string
	revision int64
	at       time.Time
}

type measurementPipeline struct {
	apply.Applier
	metrics *measurementRecorder
	done    chan<- measuredApplied
}

func (p measurementPipeline) Apply(ctx context.Context, s *snapshot.Snapshot) error {
	if err := p.Applier.Apply(ctx, s); err != nil {
		return err
	}
	p.metrics.applies.Add(1)
	p.done <- measuredApplied{s.MeshPlan.NodeID, s.MeshPlan.InputRevision, time.Now()}
	return nil
}

func measurementPercentile(samples []float64, percent int) float64 {
	if len(samples) == 0 {
		return 0
	}
	ordered := append([]float64(nil), samples...)
	sort.Float64s(ordered)
	return ordered[(len(ordered)*percent+99)/100-1]
}

// BenchmarkControlPlane measures real signed ADS delivery through the node's
// durable pipeline. Run with -benchtime=1x; setup is excluded from ns/op.
func BenchmarkControlPlane(b *testing.B) {
	workloads := []controlPlaneWorkload{{10, 1, 5}, {100, 1, 5}, {100, 4, 5}, {100, 4, 20}, {1000, 8, 5}}
	for _, workload := range workloads {
		name := fmt.Sprintf("services=%d/streams=%d/hz=%d", workload.Services, workload.Streams, workload.ChangesPerSecond)
		b.Run(name, func(b *testing.B) {
			for range b.N {
				runControlPlaneMeasurement(b, workload)
			}
		})
	}
}

func runControlPlaneMeasurement(b *testing.B, workload controlPlaneWorkload) {
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	issue, _ := lifecycleIssuer(b)
	controllerIdentity, workloadIdentity, ownerIdentity := issue("controller", 2), issue("caller", 3), issue("owner", 4)
	metrics := &measurementRecorder{}
	controllerIdentity.svid.Key = measurementSigner{controllerIdentity.svid.Key, &metrics.signatures}
	backing := &measurementStore{Store: memory.New()}
	// Admission and control-transport TLS are deliberately isolated; plan signing,
	// verification, watches, compiler, ACK flow control and journal remain real.
	c, err := controller.New(controller.Config{ID: "measurement", Objects: backing, Leases: backing, Identity: controllerIdentity, DevInsecure: true, LeaseTTL: time.Hour, EndpointTTL: time.Hour, PlanValidity: time.Hour, ReconcileInterval: time.Hour}, controller.WithDiscovery(discovery.Config{HeartbeatInterval: 200 * time.Millisecond, CoalesceWindow: time.Millisecond, Metrics: metrics, Authorize: func(_ context.Context, id, _ string) (discovery.Session, error) {
		return discovery.Session{Principal: model.Session{Tenant: "default", NodeID: id, Workload: controllerIDToID(workloadIdentity)}, Node: &discoveryv1.Node{Id: id, Capabilities: []string{model.PlanCapability}}}, nil
	}}))
	if err != nil {
		b.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()
	defer func() { cancel(); <-runDone }()
	for !c.Active() {
		select {
		case <-ctx.Done():
			b.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	service := func(i int) model.Service {
		return model.Service{Tenant: "default", Name: fmt.Sprintf("service-%04d", i), Ports: []model.ServicePort{{Name: "api", Number: 8443, Protocol: "http"}}}
	}
	for i := range workload.Services {
		s := service(i)
		record := model.ServiceRecord{Spec: s, Status: model.ServiceStatus{Endpoints: []model.Endpoint{{Tenant: s.Tenant, Service: s.Name, Port: "api", InstanceID: "instance", Owner: controllerIDToID(ownerIdentity), Address: netip.MustParseAddrPort("127.0.0.1:8443"), Ready: true, Serving: true, LeaseExpiresAt: time.Now().Add(time.Hour), Generation: 1}}}}
		data, err := json.Marshal(record)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := backing.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: s.Tenant, Kind: model.ServiceKind, Name: s.Name}, Data: data}); err != nil {
			b.Fatal(err)
		}
		if err := c.PutBindGrant(ctx, s.Name, model.BindGrant{Tenant: s.Tenant, Service: s.Name, Workload: model.Principal{ID: controllerIDToID(ownerIdentity)}}); err != nil {
			b.Fatal(err)
		}
		if err := c.PutServiceAccess(ctx, s.Name, model.ServiceAccess{Tenant: s.Tenant, Service: s.Name, Source: model.Principal{ID: controllerIDToID(workloadIdentity)}}); err != nil {
			b.Fatal(err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	gs := grpc.NewServer()
	c.RegisterServices(gs)
	serveDone := make(chan error, 1)
	go func() { serveDone <- gs.Serve(listener) }()
	defer func() { gs.Stop(); <-serveDone }()
	applied := make(chan measuredApplied, 4096)
	streamErrors := make(chan error, workload.Streams)
	var clients sync.WaitGroup
	defer clients.Wait()
	// Cancel must precede waiting for Recv goroutines, including failure paths.
	defer cancel()
	for i := range workload.Streams {
		id := fmt.Sprintf("node-%d", i)
		n, err := newPlanTestNode(b, Config{Identity: workloadIdentity, Tenant: "default", Controllers: []string{"unused"}, AllowedControllerIDs: []string{controllerIdentity.svid.ID.String()}, TrustDomain: "test", StateDir: b.TempDir()})
		if err != nil {
			b.Fatal(err)
		}
		if err := n.state.SaveNodeID(id); err != nil {
			b.Fatal(err)
		}
		n.runtime.SetRecipient(model.Session{Tenant: "default", NodeID: id, Workload: controllerIDToID(workloadIdentity)})
		pipeline := measurementPipeline{n.pipeline, metrics, applied}
		a, err := control.NewPlanApplier(control.PlanApplierConfig{Recipient: n.runtime.Recipient(), Pipeline: pipeline, HighWater: n.state.InputRevision, Verifier: n.verifier})
		if err != nil {
			b.Fatal(err)
		}
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()
		stream, err := discoveryv1.NewDiscoveryClient(conn).StreamAggregatedResources(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := stream.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: id}, TypeUrl: model.PlanTypeURL}); err != nil {
			b.Fatal(err)
		}
		clients.Add(1)
		go func() {
			defer clients.Done()
			for {
				r, err := stream.Recv()
				if err != nil {
					streamErrors <- err
					return
				}
				metrics.bytes.Add(int64(proto.Size(r)))
				if err := a.Full(ctx, r); err != nil {
					streamErrors <- err
					return
				}
				if err := stream.Send(&discoveryv1.DiscoveryRequest{TypeUrl: r.TypeUrl, VersionInfo: r.VersionInfo, ResponseNonce: r.Nonce}); err != nil {
					streamErrors <- err
					return
				}
			}
		}()
	}
	// Every changed revision must finish on every stream; heartbeat completions
	// with the same revision cannot count twice for a stream (see NodeID below).
	waitRevision := func(revision int64, start time.Time, samples *[]float64) {
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		seen := map[string]bool{}
		for len(seen) < workload.Streams {
			select {
			case event := <-applied:
				if event.revision != revision || seen[event.nodeID] {
					continue
				}
				seen[event.nodeID] = true
				if samples != nil {
					*samples = append(*samples, float64(event.at.Sub(start))/float64(time.Millisecond))
				}
			case err := <-streamErrors:
				b.Fatal(err)
			case <-deadline.C:
				b.Fatalf("revision %d did not converge", revision)
			case <-ctx.Done():
				b.Fatal(ctx.Err())
			}
		}
	}
	view, err := backing.Store.View(ctx, []store.Range{{Tenant: "default"}})
	if err != nil {
		b.Fatal(err)
	}
	waitRevision(view.Revision, time.Now(), nil)
	result := controlPlaneMeasurements{Workload: workload, Go: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), Started: time.Now().UTC()}
	beforeIdle := metrics.counts(backing)
	idleStart := time.Now()
	select {
	case <-time.After(650 * time.Millisecond):
	case <-ctx.Done():
		b.Fatal(ctx.Err())
	}
	result.IdleMilliseconds = float64(time.Since(idleStart)) / float64(time.Millisecond)
	result.Idle = metrics.counts(backing).subtract(beforeIdle)
	beforeChanges := metrics.counts(backing)
	changesStart := time.Now()
	b.StartTimer()
	for i := range 20 {
		start := time.Now()
		s := service(0)
		s.RequiresL7 = i%2 == 0
		key := store.ObjectKey{Tenant: s.Tenant, Kind: model.ServiceKind, Name: s.Name}
		previous, err := backing.GetObject(ctx, key)
		if err != nil {
			b.Fatal(err)
		}
		if err := c.PutService(ctx, s); err != nil {
			b.Fatal(err)
		}
		obj, err := backing.GetObject(ctx, key)
		if err != nil {
			b.Fatal(err)
		}
		if !catalog.TopologyChanged(previous, obj) {
			b.Fatal("measurement write did not change topology")
		}
		var written model.ServiceRecord
		if err := json.Unmarshal(obj.Data, &written); err != nil {
			b.Fatal(err)
		}
		if written.Spec.RequiresL7 != s.RequiresL7 {
			b.Fatal("measurement desired spec was not stored")
		}
		waitRevision(obj.ResourceVersion, start, &result.AppliedMilliseconds)
		if remaining := time.Second/time.Duration(workload.ChangesPerSecond) - time.Since(start); remaining > 0 {
			select {
			case <-time.After(remaining):
			case <-ctx.Done():
				b.Fatal(ctx.Err())
			}
		}
	}
	b.StopTimer()
	result.ActualChangesPerSecond = 20 / time.Since(changesStart).Seconds()
	result.Changes = metrics.counts(backing).subtract(beforeChanges)
	result.WatchSubscriptions, result.PeakActiveWatches = backing.subscriptions.Load(), backing.peak.Load()
	metrics.mu.Lock()
	result.CompileMilliseconds = append([]float64(nil), metrics.compiles[beforeChanges.Compiles:beforeChanges.Compiles+result.Changes.Compiles]...)
	metrics.mu.Unlock()
	for _, percent := range []int{50, 95, 99} {
		b.ReportMetric(measurementPercentile(result.AppliedMilliseconds, percent), fmt.Sprintf("applied-p%d-ms", percent))
	}
	compileP99 := measurementPercentile(result.CompileMilliseconds, 99)
	b.ReportMetric(compileP99, "compile-p99-ms")
	b.ReportMetric(float64(result.Changes.Signatures), "signatures")
	b.ReportMetric(float64(result.Changes.ResponseBytes), "response-bytes")
	b.ReportMetric(float64(result.Changes.Applies), "applies")
	if dir := os.Getenv("FASTMESH_MEASURE_OUT"); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			b.Fatal(err)
		}
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			b.Fatal(err)
		}
		path := filepath.Join(dir, fmt.Sprintf("services-%d-streams-%d-hz-%d.json", workload.Services, workload.Streams, workload.ChangesPerSecond))
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			b.Fatal(err)
		}
	}
	if measurementPercentile(result.AppliedMilliseconds, 99) > 2000 || compileP99 > 500 || result.Changes.Signatures > int64(100*workload.Streams) || result.Changes.Applies > int64(100*workload.Streams) || result.Changes.ResponseBytes > 512<<20 || result.PeakActiveWatches > 16 {
		b.Fatalf("predeclared test budget exceeded: %+v", result.Changes)
	}
}
