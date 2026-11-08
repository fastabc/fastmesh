package node

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	dv "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/admission"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/state"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/fastabc/fastmesh/test/fixture"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestEnrollmentResponseLossSurvivesRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backing := memory.New()
	tokens := enrollment.New(backing)
	token, err := tokens.Create(ctx, enrollment.CreateRequest{MaxUses: 1})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gs := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		result, err := handler(ctx, req)
		if err == nil && calls.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "lost committed response")
		}
		return result, err
	}))
	dv.RegisterBootstrapServer(gs, &admission.Bootstrap{Objects: backing, Tokens: tokens, DevInsecure: true})
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })}
	conn, err := grpc.NewClient("passthrough:///enroll", opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	dir := t.TempDir()
	st := fixture.OpenState(t, dir)
	n := &Node{state: st, cfg: Config{JoinToken: token.Raw, ConnectTimeout: time.Second}, conns: []*grpc.ClientConn{conn}}
	if err := n.enroll(ctx, spiffeid.RequireFromString("spiffe://test/app"), opts); status.Code(err) != codes.Unavailable {
		t.Fatalf("first attempt: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	n = &Node{state: st, cfg: n.cfg, conns: []*grpc.ClientConn{conn}}
	if err := n.enroll(ctx, spiffeid.RequireFromString("spiffe://test/app"), opts); err != nil {
		t.Fatal(err)
	}
	nodes, err := backing.ListObjects(ctx, model.SystemTenant, model.NodeKind)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes=%d err=%v", len(nodes), err)
	}
	if id, err := st.LoadNodeID(); err != nil || id != nodes[0].Key.Name {
		t.Fatalf("id=%q err=%v", id, err)
	}
	tok, err := tokens.Get(ctx, token.Token.ID)
	if err != nil || tok.Uses != 1 {
		t.Fatalf("token=%+v err=%v", tok, err)
	}
}
