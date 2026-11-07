package observability

import (
	"fmt"
	"log/slog"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/node"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

type blockedWriter struct{ entered, release chan struct{} }

func (w blockedWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}

func TestSlowReporterDoesNotBlockCallbacks(t *testing.T) {
	var n NodeObserver
	var c ControllerObserver
	writer := blockedWriter{make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		slog.New(slog.NewJSONHandler(writer, nil)).Info("snapshot", "node", n.Snapshot())
	}()
	defer func() { close(writer.release); <-done }()
	<-writer.entered
	callbacks := make(chan struct{})
	go func() {
		defer close(callbacks)
		for i := range 100 {
			err := fmt.Errorf("unique endpoint failure %d", i)
			n.ConnDenied(node.Outbound, node.ServiceRef{}, spiffeid.ID{}, err.Error())
			n.DialFailed(node.ServiceRef{}, netip.AddrPort{}, err)
			c.RecordPlanCompile(time.Millisecond, err)
		}
	}()
	select {
	case <-callbacks:
	case <-time.After(time.Second):
		t.Fatal("callback waited for blocked log output")
	}
	if n.Snapshot().Denied.Outbound != 100 || n.Snapshot().DialFailures != 100 || c.Snapshot().Compile.Errors != 100 {
		t.Fatal("events were lost while reporter was blocked")
	}
}

func TestConcurrentCountersAndIdempotentStreamClose(t *testing.T) {
	var n NodeObserver
	var c ControllerObserver
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				closeStream := c.DiscoveryStreamOpened()
				n.PlanApplied(1, time.Time{}, true)
				n.ControlContact("ignored", nil)
				c.RecordPlanSignature(time.Millisecond, nil)
				c.RecordDiscoveryAck(true)
				c.RecordPlanApplied("ignored", 1)
				_ = n.Snapshot()
				_ = c.Snapshot()
				closeStream()
				closeStream()
			}
		})
	}
	wg.Wait()
	if s := n.Snapshot(); s.PlanApplied != 4000 || s.Contacts != 4000 || s.TopologyChanged != 4000 {
		t.Fatalf("lost node events: %+v", s)
	}
	if s := c.Snapshot(); s.Streams != 0 || s.Sign.Count != 4000 || s.Sign.Seconds != 4 || s.ACK != 4000 || s.AppliedReports != 4000 {
		t.Fatalf("lost controller events: %+v", s)
	}
}

func TestInspectDoesNotInventAvailability(t *testing.T) {
	now := time.Now()
	ref := node.ServiceRef{Service: "payments", Port: "api"}
	for _, tt := range []struct {
		status node.Status
		state  string
	}{
		{node.Status{}, "no_plan"},
		{node.Status{TopologyHash: "hash", NotAfter: now, Dialable: map[node.ServiceRef]int{ref: 1}}, "expired_plan"},
		{node.Status{TopologyHash: "hash", NotAfter: now.Add(time.Minute)}, "no_selectable_endpoint"},
		{node.Status{TopologyHash: "hash", NotAfter: now.Add(time.Minute), Dialable: map[node.ServiceRef]int{ref: 1}}, "selectable"},
	} {
		if got := Inspect(tt.status, ref, now); got.State != tt.state || got.ValidityRemainingSeconds < 0 {
			t.Fatalf("got %+v, want %s", got, tt.state)
		}
	}
}

func TestStreamReasonsHaveFixedCardinality(t *testing.T) {
	var observer ControllerObserver
	observer.RecordDiscoveryStreamEnd("ack_timeout", time.Second)
	observer.RecordDiscoveryStreamEnd("unexpected-node-id", time.Second)
	observer.RecordDiscoveryAckWait("timeout", time.Second)
	s := observer.Snapshot()
	if len(s.StreamEnds) != 8 || s.StreamEnds["ack_timeout"] != 1 || s.StreamEnds["error"] != 1 || s.TimeoutWait.Count != 1 {
		t.Fatalf("unbounded or missing metrics: %+v", s)
	}
}
