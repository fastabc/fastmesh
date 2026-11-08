package discovery

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	dv "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestOversizedEnvelopeRejectedOnAllEntrances(t *testing.T) {
	payload := make([]byte, model.MaxPlanBodyBytes)
	s := New(Config{DefaultTypeURL: model.PlanTypeURL,
		Authorize: func(context.Context, string, string) (Session, error) {
			return Session{Principal: model.Session{Tenant: "default", NodeID: "node", Workload: spiffeid.RequireFromString("spiffe://test/app")}, Node: &dv.Node{Id: "node", Capabilities: []string{model.PlanCapability}}}, nil
		},
		Watch: func(context.Context, Session, string) (<-chan struct{}, error) { return nil, nil },
		Builder: BuilderFunc(func(context.Context, BuildRequest) (Artifact, error) {
			return Artifact{Revision: "r1", FetchPayload: payload, Resources: []*dv.Resource{{Resource: &anypb.Any{TypeUrl: model.PlanTypeURL, Value: payload}}}, Signature: &dv.SnapshotSignature{CertChain: [][]byte{make([]byte, 2<<20)}}}, nil
		}),
	})
	gs := grpc.NewServer()
	dv.RegisterDiscoveryServer(gs, s)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///size", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20)), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := dv.NewDiscoveryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check := func(err error) {
		t.Helper()
		if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), model.ErrPlanTooLarge.Error()) {
			t.Fatalf("limit rejection: %v", err)
		}
	}
	full, err := client.StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := full.Send(&dv.DiscoveryRequest{Node: &dv.Node{Id: "node"}}); err != nil {
		t.Fatal(err)
	}
	_, err = full.Recv()
	check(err)
	delta, err := client.DeltaAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delta.Send(&dv.DeltaDiscoveryRequest{Node: &dv.Node{Id: "node"}}); err != nil {
		t.Fatal(err)
	}
	_, err = delta.Recv()
	check(err)
	_, err = client.FetchSnapshot(ctx, &dv.FetchSnapshotRequest{Node: &dv.Node{Id: "node"}})
	check(err)
}
