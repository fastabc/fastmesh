package admission

import (
	"context"
	"testing"

	dv "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/store/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegistrationRetryPreservesControllerRegion(t *testing.T) {
	ctx := context.Background()
	tokens := enrollment.New(memory.New())
	token, err := tokens.Create(ctx, enrollment.CreateRequest{MaxUses: 1})
	if err != nil {
		t.Fatal(err)
	}
	s := &Bootstrap{Tokens: tokens, DevInsecure: true, Region: "east", CellLabel: "deployment-cell", ControllerAddrs: []string{"east"}}
	req := &dv.RegisterRequest{JoinToken: token.Raw, RequestId: "0123456789abcdef0123456789abcdef"}
	first, err := s.Register(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	other := *s
	other.Region, other.ControllerAddrs = "west", []string{"west"}
	if _, err := other.Register(ctx, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-cell retry redirected existing registration: %v", err)
	}
	again, err := s.Register(ctx, req)
	if err != nil || again.GetNodeId() != first.NodeId || again.GetControllerAddresses()[0] != "east" {
		t.Fatalf("same-cell recovery: %v %v", again, err)
	}
	tok, err := tokens.Get(ctx, token.Token.ID)
	if err != nil || tok.Uses != 1 {
		t.Fatalf("uses=%d err=%v", tok.Uses, err)
	}
}
