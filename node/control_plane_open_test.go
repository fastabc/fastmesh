package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/discovery"
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

type openLoadApplier struct {
	apply.Applier
	metrics *measurementRecorder
}

func (p openLoadApplier) Apply(ctx context.Context, s *snapshot.Snapshot) error {
	if err := p.Applier.Apply(ctx, s); err != nil {
		return err
	}
	p.metrics.applies.Add(1)
	return nil
}

type openLoadObservation struct {
	id       string
	revision int64
	revoked  bool
}

// Writes follow a timer, never waiting for an ACK or an Apply. The slow stream
// holds its initial ACK while both tenants must continue processing updates.
func TestOpenLoadAndSlowACKIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issue, _ := lifecycleIssuer(t)
	signer, identity := issue("controller", 2), issue("owner", 3)
	metrics := &measurementRecorder{}
	signer.svid.Key = measurementSigner{signer.svid.Key, &metrics.signatures}
	backing := &measurementStore{Store: memory.New()}
	streamTenants := map[string]string{"fast-a": "a", "fast-b": "b", "slow-a": "a"}
	c, err := controller.New(controller.Config{ID: "open-load", Objects: backing, Leases: backing, Identity: signer, DevInsecure: true,
		LeaseTTL: time.Hour, EndpointTTL: time.Hour, PlanValidity: time.Hour, ReconcileInterval: time.Hour,
	}, controller.WithDiscovery(discovery.Config{HeartbeatInterval: 200 * time.Millisecond, CoalesceWindow: time.Millisecond, Metrics: metrics,
		Authorize: func(_ context.Context, id, _ string) (discovery.Session, error) {
			return discovery.Session{Principal: model.Session{Tenant: streamTenants[id], NodeID: id, Workload: controllerIDToID(identity)}, Node: &discoveryv1.Node{Id: id, Capabilities: []string{model.PlanCapability}}}, nil
		}}))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()
	defer func() { cancel(); <-runDone }()
	for !c.Active() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	for _, name := range []string{"a", "b"} {

		for i := range 100 {
			service := fmt.Sprintf("service-%d", i)
			record := model.ServiceRecord{Spec: model.Service{Tenant: name, Name: service, Ports: []model.ServicePort{{Name: "api", Protocol: "http"}}},
				Status: model.ServiceStatus{Endpoints: []model.Endpoint{{Tenant: name, Service: service, Port: "api", InstanceID: "one", Owner: controllerIDToID(identity), Address: netip.MustParseAddrPort("127.0.0.1:8443"), Ready: true, Serving: true, Generation: 1, LeaseExpiresAt: time.Now().Add(time.Hour)}}}}
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := backing.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: name, Kind: model.ServiceKind, Name: service}, Data: data}); err != nil {
				t.Fatal(err)
			}
			if err := c.PutBindGrant(ctx, service, model.BindGrant{Tenant: name, Service: service, Workload: model.Principal{ID: controllerIDToID(identity)}}); err != nil {
				t.Fatal(err)
			}
			if err := c.PutServiceAccess(ctx, service, model.ServiceAccess{Tenant: name, Service: service, Source: model.Principal{ID: controllerIDToID(identity)}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	c.RegisterServices(gs)
	serveDone := make(chan error, 1)
	go func() { serveDone <- gs.Serve(lis) }()
	defer func() { gs.Stop(); <-serveDone }()
	observations := make(chan openLoadObservation, 1024)
	streamErrors := make(chan error, len(streamTenants))
	releaseSlow := make(chan struct{})
	var clients sync.WaitGroup
	defer clients.Wait()
	defer cancel()
	for id, name := range streamTenants {
		n, err := newPlanTestNode(t, Config{Identity: identity, Tenant: name, Controllers: []string{"unused"}, AllowedControllerIDs: []string{signer.svid.ID.String()}, TrustDomain: "test", StateDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.state.SaveNodeID(id); err != nil {
			t.Fatal(err)
		}
		n.runtime.SetRecipient(model.Session{Tenant: name, NodeID: id, Workload: controllerIDToID(identity)})
		a, err := control.NewPlanApplier(control.PlanApplierConfig{Recipient: n.runtime.Recipient(), Pipeline: openLoadApplier{n.pipeline, metrics}, HighWater: n.state.InputRevision, Verifier: n.verifier})
		if err != nil {
			t.Fatal(err)
		}
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		stream, err := discoveryv1.NewDiscoveryClient(conn).StreamAggregatedResources(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: id}, TypeUrl: model.PlanTypeURL}); err != nil {
			t.Fatal(err)
		}
		clients.Go(func() {
			first := true
			for {
				resp, err := stream.Recv()
				if err != nil {
					streamErrors <- err
					return
				}
				metrics.bytes.Add(int64(proto.Size(resp)))
				if err := a.Full(ctx, resp); err != nil {
					streamErrors <- err
					return
				}
				p := n.runtime.CurrentPlan()
				observation := openLoadObservation{id: id, revision: p.InputRevision}
				for _, out := range p.Outbound {
					if out.Service == "service-0" {
						observation.revoked = len(out.Owners) == 0 && len(out.Endpoints) == 0
					}
				}
				select {
				case observations <- observation:
				case <-ctx.Done():
					return
				}
				if first && id == "slow-a" {
					select {
					case <-releaseSlow:
					case <-ctx.Done():
						return
					}
				}
				first = false
				if err := stream.Send(&discoveryv1.DiscoveryRequest{TypeUrl: resp.TypeUrl, VersionInfo: resp.VersionInfo, ResponseNonce: resp.Nonce}); err != nil {
					streamErrors <- err
					return
				}
			}
		})
	}
	latest := map[string]openLoadObservation{}
	seen := map[string]map[int64]bool{}
	consume := func(o openLoadObservation) {
		if previous, ok := latest[o.id]; ok && o.revision < previous.revision {
			t.Fatal("applied revision regressed")
		}
		latest[o.id] = o
		if seen[o.id] == nil {
			seen[o.id] = map[int64]bool{}
		}
		seen[o.id][o.revision] = true
	}
	await := func(targets map[string]int64, revoked bool) {
		t.Helper()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			ready := true
			for id, revision := range targets {
				o, exists := latest[id]
				ready = ready && exists && o.revision >= revision && (!revoked || o.revoked)
			}
			if ready {
				return
			}
			select {
			case o := <-observations:
				consume(o)
			case err := <-streamErrors:
				t.Fatal(err)
			case <-deadline.C:
				t.Fatalf("streams did not converge: %+v", latest)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	await(map[string]int64{"fast-a": 0, "fast-b": 0, "slow-a": 0}, false)
	slowInitial := latest["slow-a"].revision
	written := map[string][]int64{"a": {}, "b": {}}
	before := metrics.counts(backing)
	started := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for i := range 40 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		name := []string{"a", "b"}[i%2]
		service := model.Service{Tenant: name, Name: "service-0", RequiresL7: (i/2)%2 == 0, Ports: []model.ServicePort{{Name: "api", Protocol: "http"}}}
		if err := c.PutService(ctx, service); err != nil {
			t.Fatal(err)
		}
		obj, err := backing.GetObject(ctx, store.ObjectKey{Tenant: name, Kind: model.ServiceKind, Name: service.Name})
		if err != nil {
			t.Fatal(err)
		}
		written[name] = append(written[name], obj.ResourceVersion)
	}
	actualRate := 40 / time.Since(started).Seconds()
	revokedAt := time.Now()
	targets := map[string]int64{}
	for _, name := range []string{"a", "b"} {
		if err := c.DeleteBindGrant(ctx, name, "service-0"); err != nil {
			t.Fatal(err)
		}
		v, err := backing.Store.View(ctx, []store.Range{{Tenant: name}})
		if err != nil {
			t.Fatal(err)
		}
		targets["fast-"+name] = v.Revision
	}
	await(targets, true)
	fastMS := float64(time.Since(revokedAt)) / float64(time.Millisecond)
	if latest["slow-a"].revision != slowInitial {
		t.Fatal("slow stream advanced without ACK")
	}
	releasedAt := time.Now()
	close(releaseSlow)
	await(map[string]int64{"slow-a": targets["fast-a"]}, true)
	slowMS := float64(time.Since(releasedAt)) / float64(time.Millisecond)
	coalesced := map[string]int{}
	watermarks := map[string]int64{}
	for id, name := range streamTenants {
		watermarks[id] = latest[id].revision
		for _, revision := range written[name] {
			if !seen[id][revision] {
				coalesced[id]++
			}
		}
	}
	result := struct {
		Writes             int               `json:"writes"`
		ActualHz           float64           `json:"actual_hz"`
		FastRevocationMS   float64           `json:"fast_revocation_ms"`
		SlowCatchupMS      float64           `json:"slow_catchup_ms"`
		FinalRevisions     map[string]int64  `json:"final_revisions"`
		CoalescedRevisions map[string]int    `json:"coalesced_revisions"`
		Counts             measurementCounts `json:"counts"`
	}{40, actualRate, fastMS, slowMS, watermarks, coalesced, metrics.counts(backing).subtract(before)}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
	if path := os.Getenv("FASTMESH_OPEN_MEASURE_OUT"); path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
