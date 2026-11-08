package discovery

import (
	"context"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Returning from the RPC cancels the underlying gRPC Recv. The buffered result
// lets its goroutine exit even when the deadline wins the select.
func firstRequest[T any](ctx context.Context, recv func() (*T, error), timeout time.Duration) (*T, error) {
	type result struct {
		value *T
		err   error
	}
	done := make(chan result, 1)
	go func() { value, err := recv(); done <- result{value, err} }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.value, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, status.Error(codes.DeadlineExceeded, "discovery: first request timeout")
	}
}

func sendResponse(ctx context.Context, send func() error, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- send() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "discovery: send timeout")
	}
}

func (s *Server) reserveStream() (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams >= s.cfg.MaxStreams {
		return nil, status.Error(codes.ResourceExhausted, "discovery: stream capacity reached")
	}
	s.streams++
	return func() { s.mu.Lock(); s.streams--; s.mu.Unlock() }, nil
}

func (s *Server) reserveNode(node model.Session) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodes[node] >= s.cfg.MaxStreamsPerNode {
		return nil, status.Error(codes.ResourceExhausted, "discovery: node stream capacity reached")
	}
	s.nodes[node]++
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.nodes[node]--
		if s.nodes[node] == 0 {
			delete(s.nodes, node)
		}
	}, nil
}

// Optional metrics use a fixed vocabulary; identities and nonces are not labels.
func streamEndReason(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	switch status.Convert(err).Message() {
	case "discovery: first request timeout":
		return "first_request_timeout"
	case "discovery: send timeout":
		return "send_timeout"
	case "discovery: acknowledgement timeout":
		return "ack_timeout"
	case "discovery: stream capacity reached":
		return "stream_capacity"
	case "discovery: node stream capacity reached":
		return "node_capacity"
	default:
		return "error"
	}
}
func (s *Server) recordAckWait(result string, since time.Time) {
	if s.observer != nil {
		s.observer.RecordDiscoveryAckWait(result, time.Since(since))
	}
}

type coalescer struct {
	window time.Duration
	timer  *time.Timer
	ch     <-chan time.Time
}

func newCoalescer(window time.Duration) *coalescer {
	if window <= 0 {
		window = defaultCoalesceWindow
	}
	return &coalescer{window: window}
}

func (c *coalescer) schedule() {
	// Bound batching from the first change, even during continuous churn.
	if c.ch != nil {
		return
	}
	if c.timer == nil {
		c.timer = time.NewTimer(c.window)
	} else {
		c.timer.Reset(c.window)
	}
	c.ch = c.timer.C
}

func (c *coalescer) fired() {
	c.ch = nil
}

func (c *coalescer) stop() {
	if c.timer == nil {
		return
	}
	if !c.timer.Stop() {
		select {
		case <-c.timer.C:
		default:
		}
	}
}

func (s *Server) recordApplied(nodeID string, revision int64) {
	if revision > 0 {
		if s.applied != nil {
			s.applied.RecordPlanApplied(nodeID, revision)
		}
	}
}

func (a *Artifact) sign(ctx context.Context) error {
	if a.Sign == nil {
		return nil
	}
	var err error
	a.Signature, err = a.Sign(ctx)
	return err
}
