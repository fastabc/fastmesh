package node

import (
	"context"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/identity/spiffe/workloadapi"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	workloadpb "github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func waitUntil(t *testing.T, ctx context.Context, check func() bool) {
	t.Helper()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
func waitForPortProtocol(t *testing.T, client, server *Node, ctx context.Context, protocol string) {
	t.Helper()
	waitUntil(t, ctx, func() bool {
		p := server.runtime.CurrentPlan()
		return p != nil && len(p.Inbound) > 0 && p.Inbound[0].Port.Protocol == protocol
	})
	waitUntil(t, ctx, func() bool {
		p := client.runtime.CurrentPlan()
		return p != nil && len(p.Outbound) > 0 && p.Outbound[0].Port.Protocol == protocol
	})
}
func TestGRPCIdentityMethodAuthorizationAndStreams(t *testing.T) {
	for _, safe := range []bool{false, true} {
		t.Run(map[bool]string{false: "manual", true: "safe_constructor"}[safe], func(t *testing.T) { testGRPCAuthorization(t, safe) })
	}
}

type inspectingHealthServer struct {
	healthpb.HealthServer
	inspect func(context.Context)
}

func (s inspectingHealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.inspect(ctx)
	return s.HealthServer.Check(ctx, req)
}
func testGRPCAuthorization(t *testing.T, safe bool) {
	client, server, c, ctx := startNodePair(t, false)
	ref := ServiceRef{Service: "payments", Port: "api"}
	if err := c.PutService(ctx, model.Service{Tenant: "default", Name: ref.Service, RequiresL7: true, Ports: []model.ServicePort{{Name: ref.Port, Protocol: "grpc"}}}); err != nil {
		t.Fatal(err)
	}
	id := spiffeid.RequireFromString("spiffe://test/orders")
	if err := c.PutServiceAccess(ctx, "orders", model.ServiceAccess{Tenant: "default", Service: ref.Service, Source: model.Principal{ID: id}, L7: &model.HTTPMatch{Method: "POST", Path: healthpb.Health_Check_FullMethodName}}); err != nil {
		t.Fatal(err)
	}
	waitForPortProtocol(t, client, server, ctx, "grpc")
	var gs *grpc.Server
	var lis *Listener
	var err error
	if safe {
		gs, lis, err = server.NewGRPCServer(ctx, ref)
	} else {
		lis, err = server.GRPCListen(ctx, ref)
		unary, stream := server.GRPCAuthorizer(ref)
		gs = grpc.NewServer(grpc.Creds(server.GRPCServerCredentials()), grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))
	}
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	var calls atomic.Int32
	hserver := health.NewServer()
	healthpb.RegisterHealthServer(gs, inspectingHealthServer{HealthServer: hserver, inspect: func(ctx context.Context) {
		if peer, ok := PeerFromContext(ctx); !ok || peer != id {
			t.Errorf("unverified gRPC peer %s %v", peer, ok)
		}
		calls.Add(1)
	}})
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	waitUntil(t, ctx, func() bool { return client.Status().Dialable[ref] == 1 })
	conn, err := grpc.NewClient("passthrough:///payments", client.GRPCDialOptions(ref)...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	h := healthpb.NewHealthClient(conn)
	if _, err := h.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	watch, err := h.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = watch.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("denied stream=%v", err)
	}
	// Revoke just Check, preserving the established transport for Watch.
	if err := c.PutServiceAccess(ctx, "orders", model.ServiceAccess{Tenant: "default", Service: ref.Service, Source: model.Principal{ID: id}, L7: &model.HTTPMatch{Methods: []string{"POST"}, PathPrefix: healthpb.Health_Watch_FullMethodName}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, ctx, func() bool {
		in := dataplane.Inbound(server.runtime.CurrentPlan(), ref)
		return in != nil && len(in.Allow) == 1 && in.Allow[0].L7.PathPrefix != ""
	})
	if _, err = h.Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("method bypass: %v", err)
	}
	watch, err = h.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = watch.Recv(); err != nil {
		t.Fatal(err)
	}
	// A stream established while authorized must recheck before its next send.
	if err := c.PutServiceAccess(ctx, "orders", model.ServiceAccess{Tenant: "default", Service: ref.Service, Source: model.Principal{ID: id}, L7: &model.HTTPMatch{Method: "POST", Path: healthpb.Health_Check_FullMethodName}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, ctx, func() bool {
		in := dataplane.Inbound(server.runtime.CurrentPlan(), ref)
		return in != nil && len(in.Allow) == 1 && in.Allow[0].L7.Path == healthpb.Health_Check_FullMethodName
	})
	hserver.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	// grpc/health translates a failed Send into Canceled. Both codes close
	// the stream without delivering the forbidden message.
	if _, err = watch.Recv(); status.Code(err) != codes.PermissionDenied && status.Code(err) != codes.Canceled {
		t.Fatalf("revoked stream: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("denied unary reached handler %d times", calls.Load())
	}
	if _, _, err := server.GRPCServerCredentials().ServerHandshake(nil); err == nil {
		t.Fatal("accepted a connection without mesh TLS")
	}
}

func TestH2CReusesConnectionAndServingKeepsGeneration(t *testing.T) {
	client, server, c, ctx := startNodePair(t, false)
	ref := ServiceRef{Service: "payments", Port: "api"}
	hc, err := client.HTTPClientWithProtocol(ref, "grpc")
	if err != nil {
		t.Fatal(err)
	}
	defer hc.CloseIdleConnections()
	if err := c.PutService(ctx, model.Service{Tenant: "default", Name: ref.Service, Ports: []model.ServicePort{{Name: ref.Port, Protocol: "grpc"}}}); err != nil {
		t.Fatal(err)
	}
	waitForPortProtocol(t, client, server, ctx, "grpc")
	var calls atomic.Int32
	lis, err := server.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("protocol=%s", r.Proto)
		}
		if _, ok := PeerFromContext(r.Context()); !ok {
			t.Error("missing HTTP/2 peer")
		}
		calls.Add(1)
		_, _ = io.WriteString(w, "paid")
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	generation := lis.generation
	waitUntil(t, ctx, func() bool { return client.Status().Dialable[ref] == 1 })
	if err := client.WaitService(ctx, ref); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		r, err := hc.Get("http://payments/pay")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
	}
	connections := client.runtime.ActiveConnections()
	if calls.Load() != 5 || connections != 1 {
		t.Fatalf("calls=%d connections=%d", calls.Load(), connections)
	}
	if err := lis.SetServing(false); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, ctx, func() bool { return client.Status().Dialable[ref] == 0 })
	if lis.generation != generation {
		t.Fatal("readiness changed generation")
	}
	// Existing HTTP/2 transport survives readiness changes.
	r, err := hc.Get("http://payments/pay")
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Body.Close()
	if err := lis.SetServing(true); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, ctx, func() bool { return client.Status().Dialable[ref] == 1 })
	if lis.generation != generation {
		t.Fatal("serving restored with another generation")
	}
}

type fakeWorkloadAPI struct {
	workloadpb.UnimplementedSpiffeWorkloadAPIServer
	svid   spiffe.SVID
	starts atomic.Int32
}

func (s *fakeWorkloadAPI) FetchX509SVID(_ *workloadpb.X509SVIDRequest, stream workloadpb.SpiffeWorkloadAPI_FetchX509SVIDServer) error {
	s.starts.Add(1)
	key, err := x509.MarshalPKCS8PrivateKey(s.svid.Key)
	if err != nil {
		return err
	}
	var chain, bundle []byte
	for _, b := range s.svid.CertChain {
		chain = append(chain, b...)
	}
	for _, b := range s.svid.Bundle {
		bundle = append(bundle, b...)
	}
	if err := stream.Send(&workloadpb.X509SVIDResponse{Svids: []*workloadpb.X509SVID{{SpiffeId: s.svid.ID.String(), X509Svid: chain, X509SvidKey: key, Bundle: bundle}}}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func TestSPIRESourceRebuildsNodeAfterFirstContextCancellation(t *testing.T) {
	client, _, _, ctx := startNodePair(t, false)
	cfg := client.cfg
	svid, err := cfg.Identity.GetSVID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	dir, err := os.MkdirTemp("/tmp", "fm-s3-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "api.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	api := &fakeWorkloadAPI{svid: svid}
	gs := grpc.NewServer()
	workloadpb.RegisterSpiffeWorkloadAPIServer(gs, api)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	source := &workloadapi.SPIRESource{Addr: "unix://" + socket, TrustDomain: "test"}
	defer source.Close()
	cfg.Identity = source
	for range 2 {
		n, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(ctx); err != nil {
			t.Fatal(err)
		}
		_ = n.Close()
		if _, err := source.GetSVID(ctx); err != nil {
			t.Fatalf("node closed its shared source: %v", err)
		}
	}
	if api.starts.Load() != 1 {
		t.Fatalf("node restart recreated shared source: %d", api.starts.Load())
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if api.starts.Load() != 2 {
		t.Fatal("explicit Close/Start failed to recreate source")
	}
}

func TestAdvertiseAddressWithWildcardListen(t *testing.T) {
	_, server, _, ctx := startNodePair(t, false)
	server.cfg.ListenAddress = "0.0.0.0:0"
	server.cfg.AdvertiseAddress = "127.0.0.1:0"
	lis, err := server.Listen(ctx, ServiceRef{Service: "payments", Port: "api"})
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	a, err := netip.ParseAddrPort(lis.advertiseAddress())
	if err != nil || a.Addr().String() != "127.0.0.1" || a.Port() == 0 {
		t.Fatalf("advertised %s %v", a, err)
	}
	cfg := server.cfg
	cfg.AdvertiseAddress = ""
	if _, err := New(cfg); err == nil {
		t.Fatal("wildcard address can be published")
	}
}

func TestDisabledNodeCannotKeepUnackedDiscoveryOrUseCatalog(t *testing.T) {
	client, server, c, ctx := startNodePair(t, false)
	ref := ServiceRef{Service: "payments", Port: "api"}
	lis, err := server.Listen(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	discovery := discoveryv1.NewDiscoveryClient(client.conns[0])
	full, err := discovery.StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := full.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: client.nodeID}, TypeUrl: model.PlanTypeURL}); err != nil {
		t.Fatal(err)
	}
	if _, err := full.Recv(); err != nil {
		t.Fatal(err)
	} // Intentionally never ACK.
	delta, err := discovery.DeltaAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delta.Send(&discoveryv1.DeltaDiscoveryRequest{Node: &discoveryv1.Node{Id: client.nodeID}, TypeUrl: model.PlanTypeURL}); err != nil {
		t.Fatal(err)
	}
	if _, err := delta.Recv(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := c.DisableNode(ctx, client.nodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := full.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unacked full remained admitted: %v", err)
	}
	if _, err := delta.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unacked delta remained admitted: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("revocation missed heartbeat bound")
	}
	if _, err := discovery.FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: &discoveryv1.Node{Id: client.nodeID}, TypeUrl: model.PlanTypeURL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("disabled fetch=%v", err)
	}
	if _, err := catalogv1.NewCatalogClient(client.conns[0]).RenewEndpoint(ctx, &catalogv1.EndpointRequest{NodeId: client.nodeID, Service: ref.Service, InstanceId: "anything", Generation: 1}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("disabled catalog=%v", err)
	}
	// Revoke removes only the server's node-owned endpoints.
	if err := c.RevokeNode(ctx, server.nodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := discoveryv1.NewDiscoveryClient(server.conns[0]).FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: &discoveryv1.Node{Id: server.nodeID}, TypeUrl: model.PlanTypeURL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("revoked fetch=%v", err)
	}
}

func TestObserverAndStatusTrackAppliedPolicyAndRenewal(t *testing.T) {
	old, server, _, ctx := startNodePair(t, false)
	cfg := old.cfg
	_ = old.Close()
	observer := &countingObserver{}
	client, err := New(cfg, WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	observer.inspect = func() { _ = client.Status() }
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ref := ServiceRef{Service: "payments", Port: "api"}
	lis, err := server.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "paid") }))
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	waitUntil(t, ctx, func() bool { return client.Status().Dialable[ref] == 1 })
	s := client.Status()
	if s.NodeID == "" || s.Workload.IsZero() || s.TopologyHash == "" || s.InputRevision == 0 {
		t.Fatalf("incomplete status %+v", s)
	}
	if len(server.Status().Bound) != 1 {
		t.Fatal("bound service absent from status")
	}
	before := observer.applied.Load()
	waitUntil(t, ctx, func() bool {
		return observer.applied.Load() > before && !client.Status().LastControllerContact.IsZero()
	})
	if state := client.Status(); state.ControlState != "connected" || state.LastApplyFailure != "ok" {
		t.Fatalf("control status=%+v", state)
	}
	if observer.changed.Load() >= observer.applied.Load() {
		t.Fatal("deadline renewal reported topology change")
	}
	if _, err := client.Dial(ctx, ServiceRef{Service: "denied", Port: "api"}); err == nil {
		t.Fatal("denied dial succeeded")
	}
	if observer.denied.Load() != 0 || observer.contacts.Load() == 0 {
		t.Fatal("missing contact or unavailable service misclassified as policy denial")
	}
	// Returned status is a copy; callers cannot mutate live maps/slices.
	s.Dialable[ref] = 100
	if client.Status().Dialable[ref] != 1 {
		t.Fatal("status leaked live state")
	}
}

type countingControllerObserver struct {
	streams, compiles, signatures, applied atomic.Int32
	watermark                              atomic.Int64
}

func (o *countingControllerObserver) DiscoveryStreamOpened() func() {
	o.streams.Add(1)
	return func() { o.streams.Add(-1) }
}
func (*countingControllerObserver) RecordDiscoveryPush(string, error, time.Duration) {}
func (*countingControllerObserver) RecordDiscoveryAck(bool)                          {}
func (o *countingControllerObserver) RecordPlanCompile(_ time.Duration, err error) {
	if err == nil {
		o.compiles.Add(1)
	}
}
func (o *countingControllerObserver) RecordPlanSignature(_ time.Duration, err error) {
	if err == nil {
		o.signatures.Add(1)
	}
}
func (o *countingControllerObserver) RecordPlanApplied(id string, revision int64) {
	if id != "" && revision > 0 {
		o.applied.Add(1)
		o.watermark.Store(revision)
	}
}
func TestControllerObserverRecordsSignedPlansAndAuthenticatedACKs(t *testing.T) {
	observer := &countingControllerObserver{}
	_, _, _, ctx := startNodePairWithObserver(t, false, observer)
	waitUntil(t, ctx, func() bool { return observer.applied.Load() >= 2 })
	if observer.streams.Load() != 2 || observer.compiles.Load() < 2 || observer.signatures.Load() < 2 || observer.watermark.Load() == 0 {
		t.Fatalf("observer streams=%d compiles=%d signatures=%d applied=%d", observer.streams.Load(), observer.compiles.Load(), observer.signatures.Load(), observer.applied.Load())
	}
}
