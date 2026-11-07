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
