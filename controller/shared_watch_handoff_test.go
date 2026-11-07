package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/discovery"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/internal/protocol/planwire"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/election"
	etcdstore "github.com/fastabc/fastmesh/store/etcd"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/fastabc/fastmesh/test/fixture"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type sharedWatchBackend interface {
	store.ObjectStore
	store.AtomicObjectCreator
	store.ConditionalObjectDeleter
	store.Viewer
	store.RevisionWatcher
	store.WatchableStore
	store.LeaseStore
}

type countingStore struct {
	sharedWatchBackend
	views, watches atomic.Int32
}

func (s *countingStore) View(ctx context.Context, ranges []store.Range) (store.View, error) {
	s.views.Add(1)
	return s.sharedWatchBackend.View(ctx, ranges)
}
func (s *countingStore) WatchRevisions(ctx context.Context, tenant string, kinds ...string) (<-chan store.RevisionBatch, error) {
	s.watches.Add(1)
	return s.sharedWatchBackend.WatchRevisions(ctx, tenant, kinds...)
}

type compileCounter struct{ compiles atomic.Int32 }

func (*compileCounter) DiscoveryStreamOpened() func()                    { return func() {} }
func (*compileCounter) RecordDiscoveryPush(string, error, time.Duration) {}
func (*compileCounter) RecordDiscoveryAck(bool)                          {}
func (s *compileCounter) RecordPlanCompile(time.Duration, error)         { s.compiles.Add(1) }

func TestSharedWatchHeartbeatAndFollowerDiscoveryDuringHandoff(t *testing.T) {
	t.Run("memory", func(t *testing.T) { testSharedWatchHandoff(t, memory.New()) })
	t.Run("etcd", func(t *testing.T) {
		endpoints := fixture.EtcdEndpoints(t)
		if endpoints == "" {
			t.Skip("set FASTMESH_ETCD_ENDPOINTS for etcd HA integration")
		}
		client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		prefix := "/fastmesh-s2/" + rand.Text()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = client.Delete(ctx, prefix, clientv3.WithPrefix())
			_ = client.Close()
		})
		testSharedWatchHandoff(t, etcdstore.New(client, prefix))
	})
}
func testSharedWatchHandoff(t *testing.T, backing sharedWatchBackend) {
	signer, identity, _ := lifecycleIdentities(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	owner := spiffeid.RequireFromString(identity.svid.ID.String())
	node := model.Node{ID: "node", Tenant: "acme", SpiffeID: owner.String(), Intent: "active", ManagedLabels: map[string]string{model.CellLabel: "east"}, Status: model.NodeStatus{ReportedCapabilities: []string{model.PlanCapability}}}
	data, _ := json.Marshal(node)
	if _, err := backing.PutObject(ctx, store.Object{Key: model.NodeKey(node.ID), Data: data}); err != nil {
		t.Fatal(err)
	}
	stores := []*countingStore{{sharedWatchBackend: backing}, {sharedWatchBackend: backing}}
	metrics := []*compileCounter{{}, {}}
	controllers := make([]*Controller, 2)
	for n := range controllers {
		elector := election.New(backing, election.Config{Holder: []string{"one", "two"}[n], LeaseName: election.LeaseNameFor("east"), Advertise: []string{"one:9443", "two:9443"}[n], TTL: time.Minute})
		c, err := New(Config{Objects: stores[n], Leases: backing, Identity: signer, TrustDomain: "test", Region: "east", Elector: elector, EndpointTTL: time.Minute, discovery: discovery.Config{Metrics: metrics[n], HeartbeatInterval: 20 * time.Millisecond, CoalesceWindow: time.Millisecond}})
		if err != nil {
			t.Fatal(err)
		}
		active, err := elector.TryAcquire(ctx)
		if err != nil || active != (n == 0) {
			t.Fatalf("election %d: %v %v", n, active, err)
		}
		controllers[n] = c
	}
	t.Cleanup(func() { _ = controllers[1].elector.Release(context.Background()) })
	service := model.Service{Tenant: "acme", Name: "payments", Ports: []model.ServicePort{{Name: "api", Number: 8443, Protocol: "tcp"}}}
	leader := controllers[0]
	if err := leader.PutService(ctx, service); err != nil {
		t.Fatal(err)
	}
	if err := leader.PutBindGrant(ctx, "owner", model.BindGrant{Tenant: "acme", Service: service.Name, Workload: model.Principal{ID: owner}}); err != nil {
		t.Fatal(err)
	}
	if err := leader.PutServiceAccess(ctx, "self", model.ServiceAccess{Tenant: "acme", Service: service.Name, Source: model.Principal{ID: owner}}); err != nil {
		t.Fatal(err)
	}
	manager := endpoint.Manager{Objects: leader.objects, Viewer: backing}
	if _, err := manager.Register(ctx, model.Session{Tenant: "acme", NodeID: node.ID, Workload: owner}, endpoint.Registration{Service: service.Name, Port: "api", InstanceID: "one", Generation: 1, Address: netip.MustParseAddrPort("127.0.0.1:8443"), Ready: true}); err != nil {
		t.Fatal(err)
	}
	clients := make([]discoveryv1.DiscoveryClient, 2)
	connections := make([]*grpc.ClientConn, 2)
	for n, c := range controllers {
		runDone := make(chan error, 1)
		go func() { runDone <- c.Run(ctx) }()
		t.Cleanup(func() { cancel(); <-runDone })
		lis := bufconn.Listen(1 << 20)
		serverTLS, err := spiffe.ServerTLSConfig(ctx, signer, "test", false)
		if err != nil {
			t.Fatal(err)
		}
		gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
		c.RegisterServices(gs)
		go func() { _ = gs.Serve(lis) }()
		t.Cleanup(gs.Stop)
		clientTLS, err := spiffe.ClientTLSConfig(ctx, identity, []string{signer.svid.ID.String()}, "test")
		if err != nil {
			t.Fatal(err)
		}
		conn, err := grpc.NewClient("passthrough:///controller", grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		connections[n] = conn
		clients[n] = discoveryv1.NewDiscoveryClient(conn)
	}
	request := &discoveryv1.FetchSnapshotRequest{Node: &discoveryv1.Node{Id: node.ID}, TypeUrl: model.PlanTypeURL}
	// All read entry points authenticate against the persisted peer binding.
	if _, err := clients[1].FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: request.Node, Tenant: "other", TypeUrl: model.PlanTypeURL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("follower cross-tenant admission: %v", err)
	}
	if _, err := clients[1].FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: &discoveryv1.Node{Id: "forged"}, TypeUrl: model.PlanTypeURL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("follower forged peer: %v", err)
	}
	if _, err := catalogv1.NewCatalogClient(connections[1]).RenewEndpoint(ctx, &catalogv1.EndpointRequest{NodeId: node.ID, Service: service.Name, InstanceId: "one", Generation: 1}); status.Code(err) != codes.Unavailable {
		t.Fatalf("follower accepted write: %v", err)
	}
	for _, client := range clients {
		if _, err := client.FetchSnapshot(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	streams := make([]grpc.BidiStreamingClient[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse], 4)
	recv := func(stream grpc.BidiStreamingClient[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse]) *discoveryv1.DiscoveryResponse {
		t.Helper()
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&discoveryv1.DiscoveryRequest{TypeUrl: resp.TypeUrl, VersionInfo: resp.VersionInfo, ResponseNonce: resp.Nonce}); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	var firstBody []byte
	for n := range streams {
		stream, err := clients[1].StreamAggregatedResources(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&discoveryv1.DiscoveryRequest{Node: request.Node, TypeUrl: model.PlanTypeURL}); err != nil {
			t.Fatal(err)
		}
		streams[n] = stream
		resp := recv(stream)
		if n == 0 {
			firstBody = resp.Resources[0].Resource.Value
		}
	}
	// All streams have delivered their initial build. Read several natural
	// heartbeats on every stream before comparing quiescent counters.
	beforeViews, beforeCompiles := stores[1].views.Load(), metrics[1].compiles.Load()
	for range 3 {
		for _, stream := range streams {
			resp := recv(stream)
			var a, b struct{ Topology json.RawMessage }
			_ = json.Unmarshal(firstBody, &a)
			_ = json.Unmarshal(resp.Resources[0].Resource.Value, &b)
			if !bytes.Equal(a.Topology, b.Topology) {
				t.Fatal("heartbeat rewrote topology")
			}
		}
	}
	if stores[1].views.Load() != beforeViews || metrics[1].compiles.Load() != beforeCompiles {
		t.Fatalf("idle heartbeat did work: views %d -> %d compiles %d -> %d", beforeViews, stores[1].views.Load(), beforeCompiles, metrics[1].compiles.Load())
	}
	for n, s := range stores {
		if s.watches.Load() != 1 {
			t.Fatalf("controller %d watches=%d for four streams", n, s.watches.Load())
		}
	}
	// Hold a renewal after its old-leader authorization check. Its eventual
	// write must still honor the fence after handoff and concurrent revocation.
	checked, resume := make(chan struct{}), make(chan struct{})
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	delayed := *leader.endpoints
	delayed.CanBind = func(ctx context.Context, tenant, service, workload string) (bool, error) {
		allowed, err := leader.endpoints.CanBind(ctx, tenant, service, workload)
		close(checked)
		select {
		case <-resume:
			return allowed, err
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	endpointKey := model.EndpointKey("acme", service.Name, owner, "one")
	beforeRenew, err := backing.GetObject(ctx, endpointKey)
	if err != nil {
		t.Fatal(err)
	}
	renewDone := make(chan struct{})
	var renewErr error
	go func() {
		defer close(renewDone)
		_, renewErr = delayed.Renew(renewCtx, model.Session{Tenant: "acme", NodeID: node.ID, Workload: owner}, service.Name, "one", 1)
	}()
	defer func() { stopRenew(); <-renewDone }()
	select {
	case <-checked:
	case <-ctx.Done():
		t.Fatal("renewal did not reach authorization barrier")
	}
	// The existing stream stays on its connection while leadership changes.
	if err := leader.elector.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if active, err := controllers[1].elector.TryAcquire(ctx); err != nil || !active {
		t.Fatalf("handoff: %v %v", active, err)
	}
	if err := controllers[1].DeleteBindGrant(ctx, "acme", "owner"); err != nil {
		t.Fatal(err)
	}
	close(resume)
	select {
	case <-renewDone:
		if _, fenced := store.LeaderHint(renewErr); !fenced {
			t.Fatalf("old leader renewed after handoff: %v", renewErr)
		}
	case <-ctx.Done():
		t.Fatal("old renewal did not finish")
	}
	afterRenew, err := backing.GetObject(ctx, endpointKey)
	if err != nil || afterRenew.ResourceVersion != beforeRenew.ResourceVersion {
		t.Fatalf("fenced renewal changed endpoint: version=%d, want %d: %v", afterRenew.ResourceVersion, beforeRenew.ResourceVersion, err)
	}
	for _, stream := range streams {
		for {
			resp := recv(stream)
			p, err := planwire.DecodePlan(resp.Resources[0])
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Outbound) == 1 && len(p.Outbound[0].Owners) == 0 && len(p.Outbound[0].Endpoints) == 0 {
				break
			}
		}
	}
	if _, err := controllers[1].endpoints.Renew(ctx, model.Session{Tenant: "acme", NodeID: node.ID, Workload: owner}, service.Name, "one", 1); !errors.Is(err, endpoint.ErrDenied) {
		t.Fatalf("new leader renewed revoked binding: %v", err)
	}
	// The former leader can now serve the same revoked read projection.
	for {
		resp, err := clients[0].FetchSnapshot(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		p, err := planwire.DecodePlanBody(resp.Snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Outbound) == 1 && len(p.Outbound[0].Owners) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func TestOnlyLeaderDiscoveryCanClaimUnassignedRegion(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	node := model.Node{ID: "unclaimed", Tenant: "default", SpiffeID: "spiffe://test/workload", Intent: "active"}
	data, _ := json.Marshal(node)
	if _, err := s.PutObject(ctx, store.Object{Key: model.NodeKey(node.ID), Data: data}); err != nil {
		t.Fatal(err)
	}
	leader, err := New(Config{ID: "leader", Region: "east", Objects: s, Leases: s, DevInsecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if active, err := leader.elector.TryAcquire(ctx); err != nil || !active {
		t.Fatal("leader not acquired")
	}
	defer leader.elector.Release(ctx)
	follower, err := New(Config{ID: "follower", Region: "east", Objects: s, Leases: s, DevInsecure: true})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := s.GetObject(ctx, model.NodeKey(node.ID))
	if _, err := follower.authorizeDiscovery(ctx, node.ID, node.Tenant); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower claimed region: %v", err)
	}
	after, _ := s.GetObject(ctx, model.NodeKey(node.ID))
	if before.ResourceVersion != after.ResourceVersion {
		t.Fatal("follower wrote node")
	}
	if _, err := leader.authorizeDiscovery(ctx, node.ID, node.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := follower.authorizeDiscovery(ctx, node.ID, node.Tenant); err != nil {
		t.Fatalf("follower refused leader-claimed region: %v", err)
	}
}
