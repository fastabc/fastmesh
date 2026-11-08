package discovery

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	dv "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
)

type coalesceStream[Req, Resp any] struct {
	grpc.ServerStream
	ctx       context.Context
	requests  chan *Req
	responses chan *Resp
}

func (s *coalesceStream[Req, Resp]) Context() context.Context { return s.ctx }
func (s *coalesceStream[Req, Resp]) Recv() (*Req, error) {
	select {
	case req := <-s.requests:
		return req, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}
func (s *coalesceStream[Req, Resp]) Send(resp *Resp) error {
	select {
	case s.responses <- resp:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func TestContinuousChangesReachStreamsAndPreservePendingAck(t *testing.T) {
	for _, nack := range []bool{false, true} {
		t.Run(fmt.Sprintf("full/nack=%v", nack), func(t *testing.T) {
			checkCoalesceStream(t, fullCodec(), &dv.DiscoveryRequest{Node: &dv.Node{Id: "n"}}, func(r *dv.DiscoveryResponse) *dv.DiscoveryRequest {
				ack := &dv.DiscoveryRequest{ResponseNonce: r.Nonce, VersionInfo: r.VersionInfo}
				if nack {
					ack.ErrorDetail = &rpcstatus.Status{Message: "rejected"}
				}
				return ack
			})
		})
	}
	t.Run("delta", func(t *testing.T) {
		checkCoalesceStream(t, deltaCodec(), &dv.DeltaDiscoveryRequest{Node: &dv.Node{Id: "n"}}, func(r *dv.DeltaDiscoveryResponse) *dv.DeltaDiscoveryRequest {
			return &dv.DeltaDiscoveryRequest{ResponseNonce: r.Nonce}
		})
	})
}

func checkCoalesceStream[Req, Resp any](t *testing.T, codec streamCodec[Req, Resp], first *Req, ack func(*Resp) *Req) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream := &coalesceStream[Req, Resp]{ctx: ctx, requests: make(chan *Req, 2), responses: make(chan *Resp, 8)}
		changes := make(chan struct{}, 1)
		var revision atomic.Int64
		s := New(Config{HeartbeatInterval: time.Hour, CoalesceWindow: 50 * time.Millisecond,
			Authorize: func(context.Context, string, string) (Session, error) {
				return Session{Principal: model.Session{Tenant: "default", NodeID: "n"}, Node: &dv.Node{Id: "n"}}, nil
			},
			Watch: func(context.Context, Session, string) (<-chan struct{}, error) { return changes, nil },
			Builder: BuilderFunc(func(context.Context, BuildRequest) (Artifact, error) {
				return Artifact{Revision: fmt.Sprint(revision.Load())}, nil
			}),
		})
		stream.requests <- first
		done := make(chan error, 1)
		go func() { done <- runSession(s, stream, codec, "test") }()
		initial := <-stream.responses
		stream.requests <- ack(initial)
		go func() {
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					revision.Add(1)
					select {
					case changes <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		time.Sleep(70 * time.Millisecond)
		synctest.Wait()
		var next *Resp
		select {
		case next = <-stream.responses:
		default:
			t.Fatal("continuous churn postponed first window")
		}
		// Multiple subsequent windows become dirty while this response is pending.
		time.Sleep(120 * time.Millisecond)
		synctest.Wait()
		select {
		case <-stream.responses:
			t.Fatal("sent past pending ACK")
		default:
		}
		stream.requests <- ack(next)
		synctest.Wait()
		select {
		case <-stream.responses:
		default:
			t.Fatal("dirty revision not sent after ACK/NACK")
		}
		cancel()
		<-done
	})
}
