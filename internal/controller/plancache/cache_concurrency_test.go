package plancache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

func TestFailedCompileCanRetryFromObserver(t *testing.T) {
	f, snapshots := newCacheFixture(t, 1, 1)
	s := snapshots[0]
	recipient := model.Session{Tenant: s.Tenant, NodeID: "node", Workload: f.owner}
	var cache Cache
	done := make(chan error, 1)
	go func() {
		_, _, err := cache.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, 0, func(_ time.Duration, compileErr error) {
			if !errors.Is(compileErr, model.ErrInvalid) {
				done <- fmt.Errorf("expected failed compile, got %v", compileErr)
				return
			}
			_, _, retryErr := cache.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, nil)
			done <- retryErr
		})
		if !errors.Is(err, model.ErrInvalid) {
			done <- fmt.Errorf("outer compile returned %v", err)
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed compile retained a slot or cached its error")
	}
}

func TestConcurrentColdRecipientsShareEachProjection(t *testing.T) {
	f, snapshots := newCacheFixture(t, 2, 100)
	var cache Cache
	var compiles atomic.Int32
	start := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			<-start
			s := snapshots[i%2]
			typeURL := model.PlanTypeURL
			_, p, err := cache.Resource(context.Background(), s, model.Session{Tenant: s.Tenant, NodeID: fmt.Sprintf("node-%d", i%4), Workload: f.owner}, typeURL, f.now, time.Minute, func(time.Duration, error) { compiles.Add(1) })
			if err == nil && (!p.Valid() || len(p.Outbound) != 100 || !p.IsCurrent()) {
				err = model.ErrInvalid
			}
			errs <- err
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if compiles.Load() != 4 {
		t.Fatalf("four recipient keys compiled %d times", compiles.Load())
	}
}

func TestCompileObserverCanReenterSameCacheEntry(t *testing.T) {
	f, snapshots := newCacheFixture(t, 1, 1)
	s := snapshots[0]
	recipient := model.Session{Tenant: s.Tenant, NodeID: "node", Workload: f.owner}
	var cache Cache
	done := make(chan error, 1)
	go func() {
		_, _, err := cache.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, func(time.Duration, error) {
			_, _, nestedErr := cache.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, nil)
			done <- nestedErr
		})
		if err != nil {
			done <- err
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("compile observer cannot reenter published cache entry")
	}
}

func TestConcurrentRecipientCompilesOnceAndObserverDoesNotBlockPeers(t *testing.T) {
	f, snapshots := newCacheFixture(t, 2, 100)
	s := snapshots[0]
	recipient := model.Session{Tenant: s.Tenant, NodeID: "node", Workload: f.owner}
	var cache Cache
	var compiles atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, _, err := cache.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, func(time.Duration, error) {
			compiles.Add(1)
			close(started)
			<-release
		})
		first <- err
	}()
	defer func() { close(release); <-first }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("compile did not reach observer")
	}
	var wg sync.WaitGroup
	errs := make(chan error, 9)
	for range 8 {
		wg.Go(func() {
			_, p, err := cache.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, func(time.Duration, error) { compiles.Add(1) })
			if err == nil && (!p.Valid() || len(p.Outbound) != 100) {
				err = model.ErrInvalid
			}
			errs <- err
		})
	}
	wg.Go(func() {
		_, _, err := cache.Resource(context.Background(), snapshots[1], model.Session{Tenant: snapshots[1].Tenant, NodeID: "other", Workload: f.owner}, model.PlanTypeURL, f.now, time.Minute, nil)
		errs <- err
	})
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("observer blocked another request")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if compiles.Load() != 1 {
		t.Fatalf("same recipient compiled %d times", compiles.Load())
	}
}

func TestOldSnapshotCannotDisplaceNewLeaseOrRevokedCache(t *testing.T) {
	f, snapshots := newCacheFixture(t, 1, 1)
	old := snapshots[0]
	recipient := model.Session{Tenant: old.Tenant, NodeID: "node", Workload: f.owner}
	var cache Cache
	compiles := 0
	record := func(time.Duration, error) { compiles++ }
	e := f.endpoint(old.Tenant, "service-0")
	e.LeaseExpiresAt = e.LeaseExpiresAt.Add(time.Minute)
	revision := f.put(e.Tenant, model.EndpointKind, model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID).Name, model.EndpointRecord{Endpoint: e})
	current := f.capture(e.Tenant, revision)
	for _, revoke := range []bool{false, true} {
		if revoke {
			if err := f.objects.DeleteObject(context.Background(), store.ObjectKey{Tenant: e.Tenant, Kind: model.BindGrantKind, Name: e.Service}); err != nil {
				t.Fatal(err)
			}
			current = f.capture(e.Tenant, revision+1)
		}
		if _, _, err := cache.Resource(context.Background(), current, recipient, model.PlanTypeURL, f.now, time.Minute, record); err != nil {
			t.Fatal(err)
		}
		if _, p, err := cache.Resource(context.Background(), old, recipient, model.PlanTypeURL, f.now, time.Minute, record); err != nil || p.InputRevision != old.Revision || !p.Outbound[0].Endpoints[0].LeaseExpiresAt.Equal(e.LeaseExpiresAt.Add(-time.Minute)) {
			t.Fatalf("old captured view was rewritten: %+v %v", p, err)
		}
		before := compiles
		_, p, err := cache.Resource(context.Background(), current, recipient, model.PlanTypeURL, f.now, time.Minute, record)
		if err != nil || compiles != before || p.InputRevision != current.Revision {
			t.Fatalf("old request displaced current cache: compiles %d -> %d, error %v", before, compiles, err)
		}
		if revoke {
			if len(p.Outbound[0].Owners) != 0 || len(p.Outbound[0].Endpoints) != 0 {
				t.Fatal("old request reintroduced revoked authorization")
			}
		} else if !p.Outbound[0].Endpoints[0].LeaseExpiresAt.Equal(e.LeaseExpiresAt) {
			t.Fatal("renewed signed lease was lost")
		}
	}
}
