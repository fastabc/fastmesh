package node

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/discovery"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/fastabc/fastmesh/test/fixture"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lifecycleIdentity struct{ svid spiffe.SVID }

func (s lifecycleIdentity) GetSVID(context.Context) (spiffe.SVID, error) { return s.svid, nil }

func lifecycleIssuer(t testing.TB) (func(string, int64) lifecycleIdentity, *x509.CertPool) {
	issue, roots := fixture.Issuer(t)
	return func(path string, serial int64) lifecycleIdentity { return lifecycleIdentity{issue(path, serial)} }, roots
}

func lifecycleIdentities(t *testing.T) (lifecycleIdentity, lifecycleIdentity, *x509.CertPool) {
	issue, roots := lifecycleIssuer(t)
	return issue("controller", 2), issue("payments", 3), roots
}

// Plan-only tests restore/apply without starting a Node. Production ownership
// starts at Start; these fixtures own a locked test store until it is closed before Start.
func newPlanTestNode(t testing.TB, cfg Config, options ...Option) (*Node, error) {
	t.Helper()
	n, err := New(cfg, options...)
	if err == nil && n.composition == nil {
		n.state = fixture.OpenState(t, cfg.StateDir)
		n.pipeline = apply.NewPipeline(apply.NewHolder(), n.state)
		n.pipeline.AddCommitHook(n.runtime)
	}
	return n, err
}

func installPlan(t testing.TB, r *dataplane.Runtime, p model.Plan) {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var cloned model.Plan
	if err := json.Unmarshal(data, &cloned); err != nil {
		t.Fatal(err)
	}
	p = cloned
	for i := range p.Outbound {
		if p.Outbound[i].Owners == nil {
			for _, endpoint := range p.Outbound[i].Endpoints {
				p.Outbound[i].Owners = append(p.Outbound[i].Owners, endpoint.Owner)
			}
		}
	}
	p.Hash = p.ContentHash()
	pipeline := apply.NewPipeline(apply.NewHolder(), nil)
	pipeline.AddCommitHook(r)
	if err := pipeline.Apply(context.Background(), &snapshot.Snapshot{MeshPlan: &p}); err != nil {
		t.Fatal(err)
	}
}

func runtimePlans(clientID, serverID spiffeid.ID) (model.Plan, model.Plan) {
	base := model.Plan{TypeURL: model.PlanTypeURL, Tenant: "default", NodeID: "server", Workload: serverID, InputRevision: 1, NotAfter: time.Now().Add(time.Minute), AccessSet: true, EndpointsSet: true}
	base.Inbound = []model.InboundService{{Service: "payments", Port: model.ServicePort{Name: "api", Number: 8443, Protocol: "tcp"}, Allow: []model.InboundAllow{{Source: clientID}}}}
	client := base
	client.NodeID, client.Workload, client.Inbound = "client", clientID, nil
	return client, base
}

func startNodePair(t *testing.T, preEnrolled bool) (*Node, *Node, *controller.Controller, context.Context) {
	return startNodePairWithObserver(t, preEnrolled, nil)
}

func startNodePairWithObserver(t *testing.T, preEnrolled bool, observer controller.Observer) (*Node, *Node, *controller.Controller, context.Context) {
	t.Helper()
	issue, _ := lifecycleIssuer(t)
	clientIdentity, serverIdentity, controllerIdentity := issue("orders", 2), issue("payments", 3), issue("controller", 4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	backing := memory.New()
	c, err := controller.New(controller.Config{Observer: observer, ID: "drain", Objects: backing, Leases: backing, Identity: controllerIdentity, TrustDomain: "test", EndpointTTL: 3 * time.Second, PlanValidity: 5 * time.Second, ReconcileInterval: 10 * time.Millisecond}, controller.WithDiscovery(discovery.Config{HeartbeatInterval: 50 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Serve(cctx, lis) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	for !c.Active() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	clientID, serverID := spiffeid.RequireFromString(clientIdentity.svid.ID.String()), spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	if err := c.PutService(ctx, model.Service{Tenant: "default", Name: "payments", Ports: []model.ServicePort{{Name: "api", Number: 8443, Protocol: "http"}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutBindGrant(ctx, "payments", model.BindGrant{Tenant: "default", Service: "payments", Workload: model.Principal{ID: serverID}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutServiceAccess(ctx, "orders", model.ServiceAccess{Tenant: "default", Service: "payments", Source: model.Principal{ID: clientID}}); err != nil {
		t.Fatal(err)
	}
	newNode := func(identity lifecycleIdentity) *Node {
		t.Helper()
		token, err := c.CreateJoinToken(ctx, enrollment.CreateRequest{Tenant: "default"})
		if err != nil {
			t.Fatal(err)
		}
		n, err := newPlanTestNode(t, Config{Identity: identity, Controllers: []string{lis.Addr().String()}, AllowedControllerIDs: []string{controllerIdentity.svid.ID.String()}, TrustDomain: "test", Tenant: "default", JoinToken: token.Raw, StateDir: t.TempDir(), ConnectTimeout: 300 * time.Millisecond, DrainTimeout: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		preEnrolledNodeID := ""
		if preEnrolled {
			opts, err := n.dialOptions(ctx)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := grpc.NewClient(lis.Addr().String(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			registration, err := discoveryv1.NewBootstrapClient(conn).Register(ctx, &discoveryv1.RegisterRequest{JoinToken: token.Raw, NodeName: identity.svid.ID.String(), Capabilities: []string{model.PlanCapability}})
			_ = conn.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err := n.state.SaveNodeID(registration.NodeId); err != nil {
				t.Fatal(err)
			}
			preEnrolledNodeID = registration.NodeId
		}
		if err := n.state.Close(); err != nil {
			t.Fatal(err)
		}
		if err := n.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if preEnrolled && n.nodeID != preEnrolledNodeID {
			t.Fatalf("upgrade enrolled a new node: got=%s old=%s", n.nodeID, preEnrolledNodeID)
		}
		t.Cleanup(func() { _ = n.Close() })
		if !n.runtime.CurrentPlan().IsCurrent() {
			t.Fatalf("preEnrolled=%v selected TypeURL=%q", preEnrolled, n.runtime.CurrentPlan().TypeURL)
		}
		return n
	}
	return newNode(clientIdentity), newNode(serverIdentity), c, ctx
}

type countingObserver struct {
	applied, changed, denied, revoked, failed, contacts atomic.Int32
	inspect                                             func()
}

func (o *countingObserver) PlanApplied(_ int64, _ time.Time, changed bool) {
	o.applied.Add(1)
	if changed {
		o.changed.Add(1)
	}
	if o.inspect != nil {
		o.inspect()
	}
}

func (o *countingObserver) ConnDenied(Direction, ServiceRef, spiffeid.ID, string) { o.denied.Add(1) }

func (o *countingObserver) ConnRevoked(Direction, ServiceRef, spiffeid.ID) { o.revoked.Add(1) }

func (o *countingObserver) DialFailed(ServiceRef, netip.AddrPort, error) { o.failed.Add(1) }

func (o *countingObserver) ControlContact(_ string, err error) {
	if err == nil {
		o.contacts.Add(1)
	}
}

type fakeCatalogServer struct {
	catalogv1.UnimplementedCatalogServer
	mu                      sync.Mutex
	attempts, registrations int
	generations             []uint64
	failure                 codes.Code
	done                    chan struct{}
	doneOnce                sync.Once
}

func (s *fakeCatalogServer) RenewEndpoint(_ context.Context, r *catalogv1.EndpointRequest) (*catalogv1.EndpointLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	s.generations = append(s.generations, r.Generation)
	if s.attempts <= 3 {
		return nil, status.Error(s.failure, "injected renewal failure")
	}
	if s.attempts == 4 {
		s.doneOnce.Do(func() { close(s.done) })
	}
	return &catalogv1.EndpointLease{ExpiresAt: timestamppb.New(time.Now().Add(3 * time.Second))}, nil
}

func (s *fakeCatalogServer) RegisterEndpoint(_ context.Context, r *catalogv1.RegisterEndpointRequest) (*catalogv1.EndpointLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registrations++
	s.generations = append(s.generations, r.Generation)
	s.doneOnce.Do(func() { close(s.done) })
	return &catalogv1.EndpointLease{ExpiresAt: timestamppb.New(time.Now().Add(3 * time.Second))}, nil
}
