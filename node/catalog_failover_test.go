package node

import (
	"context"
	"net"
	"testing"
	"time"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestLeaderGenerationErrorIsNotHiddenByFollower(t *testing.T) {
	n := &Node{cfg: Config{ConnectTimeout: time.Second}}
	servers := []*fakeCatalogServer{{failure: codes.FailedPrecondition, done: make(chan struct{})}, {failure: codes.Unavailable, done: make(chan struct{})}}
	for _, server := range servers {
		lis := bufconn.Listen(1 << 20)
		gs := grpc.NewServer()
		catalogv1.RegisterCatalogServer(gs, server)
		go func() { _ = gs.Serve(lis) }()
		t.Cleanup(gs.Stop)
		conn, err := grpc.NewClient("passthrough:///catalog", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		n.conns = append(n.conns, conn)
	}
	_, err := n.lease(context.Background(), func(ctx context.Context, client catalogv1.CatalogClient) (*catalogv1.EndpointLease, error) {
		return client.RenewEndpoint(ctx, &catalogv1.EndpointRequest{Generation: 1})
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("leader's lost-generation result hidden: %v", err)
	}
	servers[1].mu.Lock()
	defer servers[1].mu.Unlock()
	if len(servers[1].generations) != 0 {
		t.Fatal("semantic leader response retried on follower")
	}
}
