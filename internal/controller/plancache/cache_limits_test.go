package plancache

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
)

func waitCache(t *testing.T, c *Cache, check func(Stats) bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if check(c.Stats()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cache did not settle: %+v", c.Stats())
}

func TestCacheCanceledWaiterDoesNotCancelOtherWaiters(t *testing.T) {
	f, snapshots := newCacheFixture(t, 1, 1)
	s := snapshots[0]
	c := &Cache{Parallelism: 1}
	recipient := model.Session{Tenant: s.Tenant, NodeID: "warm", Workload: f.owner}
	if _, _, err := c.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	c.slots <- struct{}{} // Hold compilation admission, not the cache mutex.
	recipient.NodeID = "shared"
	firstCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	var observed atomic.Int64
	go func() {
		_, _, err := c.Resource(firstCtx, s, recipient, model.PlanTypeURL, f.now, time.Minute, func(time.Duration, error) { observed.Add(1) })
		first <- err
	}()
	waitCache(t, c, func(s Stats) bool { return s.Pending == 1 })
	go func() {
		_, _, err := c.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, nil)
		second <- err
	}()
	waitCache(t, c, func(Stats) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.entries[cacheKey{s, recipient}].waiters == 2
	})
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled caller blocked")
	}
	<-c.slots
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("remaining caller blocked")
	}
	waitCache(t, c, func(s Stats) bool { return s.Pending == 0 && observed.Load() == 1 })
}

func TestCacheAdmissionCancellationAndChurn(t *testing.T) {
	f, snapshots := newCacheFixture(t, 6, 1)
	c := &Cache{MaxEntries: 3, MaxPending: 1, Parallelism: 1}
	call := func(ctx context.Context, tenant int, id string) error {
		s := snapshots[tenant]
		_, _, err := c.Resource(ctx, s, model.Session{Tenant: s.Tenant, NodeID: id, Workload: f.owner}, model.PlanTypeURL, f.now, time.Minute, nil)
		return err
	}
	if err := call(context.Background(), 0, "warm"); err != nil {
		t.Fatal(err)
	}
	c.slots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- call(ctx, 0, "queued") }()
	waitCache(t, c, func(s Stats) bool { return s.Pending == 1 })
	if err := call(context.Background(), 1, "rejected"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	if c.Stats().Rejected != 1 {
		t.Fatal("capacity rejection not observable")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	waitCache(t, c, func(s Stats) bool { return s.Pending == 0 })
	<-c.slots
	for i := range 30 {
		if err := call(context.Background(), i%6, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		if s := c.Stats(); s.Entries > 3 || s.Pending != 0 || s.Running != 0 {
			t.Fatalf("budget: %+v", s)
		}
	}
	if c.Stats().Evictions < 27 {
		t.Fatalf("no reclamation: %+v", c.Stats())
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	before := c.Stats()
	if err := call(canceled, 0, "unused"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if c.Stats() != before {
		t.Fatal("canceled call changed cache")
	}
}

func TestCacheTopologyByteBudget(t *testing.T) {
	f, snapshots := newCacheFixture(t, 3, 10)
	call := func(c *Cache, tenant int, id string) {
		t.Helper()
		s := snapshots[tenant]
		if _, _, err := c.Resource(context.Background(), s, model.Session{Tenant: s.Tenant, NodeID: id, Workload: f.owner}, model.PlanTypeURL, f.now, time.Minute, nil); err != nil {
			t.Fatal(err)
		}
	}
	probe := &Cache{}
	call(probe, 0, "probe")
	budget := probe.Stats().TopologyBytes
	if budget == 0 {
		t.Fatal("missing serialized topology accounting")
	}
	c := &Cache{MaxEntries: 100, MaxTopologyBytes: budget}
	for i := range 12 {
		call(c, i%3, fmt.Sprint(i))
		if s := c.Stats(); s.TopologyBytes > budget || s.Entries > 1 {
			t.Fatalf("byte budget exceeded: %+v", s)
		}
	}
	if c.Stats().Evictions == 0 {
		t.Fatal("byte limit did not evict")
	}
	// A valid response larger than retention capacity still serves its caller.
	tiny := &Cache{MaxTopologyBytes: 1}
	call(tiny, 0, "uncached")
	if s := tiny.Stats(); s.TopologyBytes != 0 || s.Entries != 0 || s.Pending != 0 {
		t.Fatalf("oversized entry retained: %+v", s)
	}
}
