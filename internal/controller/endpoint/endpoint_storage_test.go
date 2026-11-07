package endpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
)

type endpointCountingStore struct {
	*memory.Store
	views         atomic.Int64
	serviceWrites atomic.Int64
	conflicts     atomic.Int64
}

func (s *endpointCountingStore) View(ctx context.Context, ranges []store.Range) (store.View, error) {
	s.views.Add(1)
	return s.Store.View(ctx, ranges)
}
func (s *endpointCountingStore) PutObject(ctx context.Context, obj store.Object) (store.Object, error) {
	if obj.Key.Kind == model.ServiceKind {
		s.serviceWrites.Add(1)
	}
	saved, err := s.Store.PutObject(ctx, obj)
	if errors.Is(err, store.ErrConflict) {
		s.conflicts.Add(1)
	}
	return saved, err
}

func TestThousandInstancesRenewIndependentObjectsAtTTLThird(t *testing.T) {
	backing := memory.New()
	session, req := seed(t, backing)
	s := &endpointCountingStore{Store: backing}
	now := time.Now().UTC()
	ttl := 30 * time.Second
	m := endpoint.Manager{Objects: s, Viewer: s, TTL: ttl, Now: func() time.Time { return now }, CanBind: func(_ context.Context, tenant, service, workload string) (bool, error) {
		return tenant == session.Tenant && service == req.Service && workload == session.Workload.String(), nil
	}}
	original, err := backing.GetObject(ctx, store.ObjectKey{Tenant: session.Tenant, Kind: model.ServiceKind, Name: req.Service})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		r := req
		r.InstanceID = fmt.Sprintf("instance-%d", i)
		if _, err := m.Register(ctx, session, r); err != nil {
			t.Fatal(err)
		}
	}
	for round := range 3 {
		now = now.Add(ttl / 3)
		var wg sync.WaitGroup
		errs := make(chan error, 1000)
		for i := range 1000 {
			wg.Go(func() {
				e, err := m.Renew(ctx, session, req.Service, fmt.Sprintf("instance-%d", i), req.Generation)
				if err == nil && (e.Generation != req.Generation || !e.LeaseExpiresAt.Equal(now.Add(ttl))) {
					err = fmt.Errorf("round %d changed generation or deadline: %+v", round, e)
				}
				errs <- err
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	service, err := backing.GetObject(ctx, original.Key)
	if err != nil {
		t.Fatal(err)
	}
	if service.ResourceVersion != original.ResourceVersion || s.serviceWrites.Load() != 0 || s.views.Load() != 0 || s.conflicts.Load() != 0 {
		t.Fatalf("shared renewal work: serviceVersion=%d original=%d views=%d serviceWrites=%d conflicts=%d", service.ResourceVersion, original.ResourceVersion, s.views.Load(), s.serviceWrites.Load(), s.conflicts.Load())
	}
	objects, err := backing.ListObjects(ctx, session.Tenant, model.EndpointKind)
	if err != nil || len(objects) != 1000 {
		t.Fatalf("endpoint count=%d error=%v", len(objects), err)
	}
}

func TestEndpointExpiryVersionFenceAndTombstoneRetention(t *testing.T) {
	s := memory.New()
	session, req := seed(t, s)
	now := time.Now().UTC()
	m := endpoint.Manager{Objects: s, Viewer: s, TTL: 30 * time.Second, Now: func() time.Time { return now }}
	if _, err := m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	key := model.EndpointKey(session.Tenant, req.Service, session.Workload, req.InstanceID)
	scheduled, err := s.GetObject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Second)
	if _, err := m.Renew(ctx, session, req.Service, req.InstanceID, req.Generation); err != nil {
		t.Fatal(err)
	}
	now = now.Add(21 * time.Second)
	if err := m.ExpireObject(ctx, scheduled); err != nil {
		t.Fatal(err)
	}
	current, _ := s.GetObject(ctx, key)
	active, err := catalog.DecodeEndpoint(current)
	if err != nil || !active.RemovedAt.IsZero() {
		t.Fatalf("old expiry invalidated renewal: %+v %v", active, err)
	}
	now = now.Add(10 * time.Second)
	if err := m.ExpireObject(ctx, current); err != nil {
		t.Fatal(err)
	}
	tombstone, _ := s.GetObject(ctx, key)
	record, err := catalog.DecodeEndpoint(tombstone)
	if err != nil || record.RemovedAt.IsZero() || record.RetainUntil.Sub(record.RemovedAt) < 2*m.TTL+m.RPCDeadline {
		t.Fatalf("retention=%+v error=%v", record, err)
	}
	if _, err := m.Register(ctx, session, req); !errors.Is(err, endpoint.ErrGeneration) {
		t.Fatalf("late generation resurrected: %v", err)
	}
	now = record.RetainUntil.Add(-time.Nanosecond)
	if err := m.ExpireObject(ctx, tombstone); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject(ctx, key); err != nil {
		t.Fatal("tombstone removed too early")
	}
	now = record.RetainUntil
	if err := m.ExpireObject(ctx, tombstone); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("tombstone not collected: %v", err)
	}
}

func TestEndpointTopologyIgnoresLeaseAndTombstoneOnlyUpdates(t *testing.T) {
	s := memory.New()
	session, req := seed(t, s)
	now := time.Now().UTC()
	m := endpoint.Manager{Objects: s, Viewer: s, Now: func() time.Time { return now }}
	if _, err := m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	key := model.EndpointKey(session.Tenant, req.Service, session.Workload, req.InstanceID)
	before, _ := s.GetObject(ctx, key)
	now = now.Add(time.Second)
	if _, err := m.Renew(ctx, session, req.Service, req.InstanceID, req.Generation); err != nil {
		t.Fatal(err)
	}
	after, _ := s.GetObject(ctx, key)
	if catalog.TopologyChanged(before, after) {
		t.Fatal("lease-only update rebuilt topology")
	}
	if err := m.Deregister(ctx, session, req.Service, req.InstanceID, req.Generation); err != nil {
		t.Fatal(err)
	}
	removed, _ := s.GetObject(ctx, key)
	if !catalog.TopologyChanged(after, removed) {
		t.Fatal("removal was not a topology change")
	}
	record, err := catalog.DecodeEndpoint(removed)
	if err != nil {
		t.Fatal(err)
	}
	record.Endpoint.Generation++
	record.RetainUntil = record.RetainUntil.Add(time.Minute)
	removed.Data, _ = json.Marshal(record)
	updated, err := s.PutObject(ctx, removed)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.TopologyChanged(removed, updated) {
		t.Fatal("inactive fence rebuilt topology")
	}
}
