package discovery

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/protocol/planwire"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestPlanCapabilityGateUsesEnrolledNodeAcrossAllEntrances(t *testing.T) {
	for _, typeURL := range []string{model.PlanTypeURL} {
		t.Run(typeURL, func(t *testing.T) { testPlanCapabilityGate(t, typeURL) })
	}
}

func testPlanCapabilityGate(t *testing.T, typeURL string) {
	capability := model.PlanCapability
	var enrolled atomic.Bool
	var builds atomic.Int64
	server := New(Config{
		DefaultTypeURL: "example.v1.AgentRuntimeSnapshot",
		Authorize: func(_ context.Context, id, tenant string) (Session, error) {
			node := &discoveryv1.Node{Id: id}
			if enrolled.Load() {
				node.Capabilities = append(node.Capabilities, capability)
			}
			return Session{Principal: model.Session{Tenant: "default", NodeID: id, Workload: spiffeid.RequireFromString("spiffe://test/orders")}, Node: node}, nil
		},
		Watch: func(context.Context, Session, string) (<-chan struct{}, error) { return nil, nil },
		Builder: BuilderFunc(func(_ context.Context, r BuildRequest) (Artifact, error) {
			builds.Add(1)
			p := model.Plan{TypeURL: model.PlanTypeURL, Tenant: "default", NodeID: r.Session.Principal.NodeID, Workload: r.Session.Principal.Workload, InputRevision: 1, NotAfter: time.Now().Add(time.Minute), AccessSet: true, EndpointsSet: true}
			p.Hash = p.ContentHash()
			res, err := planwire.PackPlan(p)
			return Artifact{Revision: p.Hash, Resources: []*discoveryv1.Resource{res}}, err
		}),
	})
	gs := grpc.NewServer()
	discoveryv1.RegisterDiscoveryServer(gs, server)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///plans", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := discoveryv1.NewDiscoveryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	forged := &discoveryv1.Node{Id: "orders", Capabilities: []string{model.PlanCapability}}
	full, err := client.StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = full.Send(&discoveryv1.DiscoveryRequest{Node: forged, TypeUrl: typeURL}); err != nil {
		t.Fatal(err)
	}
	if _, err = full.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unenrolled full received plan: %v", err)
	}
	delta, err := client.DeltaAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = delta.Send(&discoveryv1.DeltaDiscoveryRequest{Node: forged, TypeUrl: typeURL}); err != nil {
		t.Fatal(err)
	}
	if _, err = delta.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unenrolled delta received plan: %v", err)
	}
	if _, err = client.FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: forged, TypeUrl: typeURL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unenrolled fetch received plan: %v", err)
	}
	if builds.Load() != 0 {
		t.Fatal("built a plan for an ineligible node")
	}
	enrolled.Store(true)
	full, err = client.StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = full.Send(&discoveryv1.DiscoveryRequest{Node: forged, TypeUrl: typeURL}); err != nil {
		t.Fatal(err)
	}
	response, err := full.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if response.TypeUrl != typeURL || len(response.Resources) != 1 {
		t.Fatalf("wrong envelope: %+v", response)
	}
	_, err = planwire.DecodePlan(response.Resources[0])
	if err != nil {
		t.Fatal(err)
	}
	delta, err = client.DeltaAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = delta.Send(&discoveryv1.DeltaDiscoveryRequest{Node: forged, TypeUrl: typeURL}); err != nil {
		t.Fatal(err)
	}
	if response, err := delta.Recv(); err != nil || response.TypeUrl != typeURL {
		t.Fatalf("enrolled delta: %+v %v", response, err)
	}
	if response, err := client.FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: forged, TypeUrl: typeURL}); err != nil || response.TypeUrl != typeURL {
		t.Fatalf("enrolled fetch: %+v %v", response, err)
	}
}
