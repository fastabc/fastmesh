package control

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/apply"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fullStream struct {
	grpc.ClientStream
	sent []*discoveryv1.DiscoveryRequest
}

func (s *fullStream) Send(r *discoveryv1.DiscoveryRequest) error {
	s.sent = append(s.sent, r)
	return nil
}
func (s *fullStream) Recv() (*discoveryv1.DiscoveryResponse, error) { return nil, io.EOF }

type deltaStream struct {
	grpc.BidiStreamingClient[discoveryv1.DeltaDiscoveryRequest, discoveryv1.DeltaDiscoveryResponse]
	sent []*discoveryv1.DeltaDiscoveryRequest
}

func (s *deltaStream) Send(r *discoveryv1.DeltaDiscoveryRequest) error {
	s.sent = append(s.sent, r)
	return nil
}

func TestTypeDispatchAndContactFollowSuccessfulApply(t *testing.T) {
	var applied, contacts int
	var applyErr error
	client := New(Config{TypeURL: model.PlanTypeURL, Node: func() *discoveryv1.Node { return &discoveryv1.Node{Id: "orders"} }, OnContact: func() { contacts++ }, Appliers: map[string]Applier{model.PlanTypeURL: {
		Loaded: func() bool { return true }, Current: func() string { return "old" },
		Full: func(context.Context, *discoveryv1.DiscoveryResponse) error { applied++; return applyErr },
	}}})
	stream := &fullStream{}
	response := &discoveryv1.DiscoveryResponse{TypeUrl: "unknown", VersionInfo: "new", Nonce: "nonce"}
	if err := client.HandleFull(context.Background(), stream, response); err != nil {
		t.Fatal(err)
	}
	if applied != 0 || contacts != 0 || stream.sent[0].ErrorDetail == nil {
		t.Fatal("unknown type applied or contacted")
	}
	response.TypeUrl = model.PlanTypeURL
	if err := client.HandleFull(context.Background(), stream, response); err != nil {
		t.Fatal(err)
	}
	ack := stream.sent[1]
	if applied != 1 || contacts != 1 || ack.TypeUrl != model.PlanTypeURL || ack.ResponseNonce != "nonce" || ack.VersionInfo != "new" || ack.ErrorDetail != nil {
		t.Fatalf("bad ACK: %+v", ack)
	}
	applyErr = errors.New("verification rejected")
	if err := client.HandleFull(context.Background(), stream, response); err != nil {
		t.Fatal(err)
	}
	if contacts != 1 || stream.sent[2].ErrorDetail == nil || stream.sent[2].VersionInfo != "old" {
		t.Fatal("rejected plan refreshed contact or advanced version")
	}
	applyErr = apply.ErrRecoveryRequired
	if err := client.HandleFull(context.Background(), stream, response); !errors.Is(err, apply.ErrRecoveryRequired) {
		t.Fatalf("recovery decision: %v", err)
	}
	if len(stream.sent) != 3 {
		t.Fatal("durable but unpublished decision was ACKed/NACKed")
	}
}

func TestDeltaAfterApplyAndRecovery(t *testing.T) {
	var applyErr, hookErr error
	var hooks, contacts int
	node := &discoveryv1.Node{Id: "orders"}
	response := &discoveryv1.DeltaDiscoveryResponse{SystemVersionInfo: "new", Nonce: "nonce", Node: node}
	client := New(Config{
		TypeURL: model.PlanTypeURL,
		Node:    func() *discoveryv1.Node { return node },
		Appliers: map[string]Applier{model.PlanTypeURL: {
			Delta: func(context.Context, *discoveryv1.DeltaDiscoveryResponse) error { return applyErr },
		}},
		AfterApply: func(_ context.Context, version string, gotNode *discoveryv1.Node, err error) error {
			hooks++
			if version != response.SystemVersionInfo || gotNode != node || err != applyErr {
				t.Fatalf("unexpected apply result: version=%q node=%v err=%v", version, gotNode, err)
			}
			return hookErr
		},
		OnContact: func() { contacts++ },
	})
	stream := &deltaStream{}
	ctx := context.Background()
	if err := client.HandleDelta(ctx, stream, response); err != nil {
		t.Fatal(err)
	}
	if hooks != 1 || contacts != 1 || len(stream.sent) != 1 || stream.sent[0].ResponseNonce != response.Nonce || stream.sent[0].ErrorDetail != nil {
		t.Fatal("successful apply must record contact and ACK the response nonce")
	}

	applyErr = errors.New("rejected by applier")
	hookErr = ErrUnsignedDelta
	if err := client.HandleDelta(ctx, stream, response); status.Code(err) != codes.Unimplemented {
		t.Fatalf("delta fallback: %v", err)
	}
	if hooks != 2 || contacts != 1 || len(stream.sent) != 2 || stream.sent[1].ErrorDetail.GetMessage() != hookErr.Error() {
		t.Fatal("expected a NACK with the hook's error before fallback")
	}

	applyErr, hookErr = apply.ErrRecoveryRequired, nil
	if err := client.HandleDelta(ctx, stream, response); !errors.Is(err, apply.ErrRecoveryRequired) {
		t.Fatalf("recovery: %v", err)
	}
	if hooks != 2 || contacts != 1 || len(stream.sent) != 2 {
		t.Fatal("recovery must exit before callbacks or a response")
	}
}

func TestReconnectDelayStaysInsideJitterWindow(t *testing.T) {
	for _, backoff := range []time.Duration{time.Nanosecond, 100 * time.Millisecond, time.Second, 30 * time.Second} {
		for range 100 {
			delay := reconnectDelay(backoff)
			if delay < backoff/2 || delay >= backoff {
				t.Fatalf("delay=%s outside [%s,%s)", delay, backoff/2, backoff)
			}
		}
	}
}
