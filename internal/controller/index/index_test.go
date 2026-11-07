package index

import (
	"context"
	"encoding/json"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func object(t *testing.T, kind, name string, spec any) store.Object {
	t.Helper()
	data, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatal(err)
	}
	return store.Object{Key: store.ObjectKey{Tenant: "default", Kind: kind, Name: name}, Data: data}
}
func TestCompleteRevisionPublishesAtomicCatalogAndKeepsOldSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backing := memory.New()
	owner := spiffeid.RequireFromString("spiffe://test/owner")
	caller := spiffeid.RequireFromString("spiffe://test/caller")
	service := object(t, model.ServiceKind, "api", model.Service{Tenant: "default", Name: "api", Ports: []model.ServicePort{{Name: "tcp", Number: 80, Protocol: "tcp"}}})
	grant := object(t, model.BindGrantKind, "grant", model.BindGrant{Tenant: "default", Service: "api", Workload: model.Principal{ID: owner}})
	access := object(t, model.ServiceAccessKind, "access", model.ServiceAccess{Tenant: "default", Service: "api", Source: model.Principal{ID: caller}})
	for _, obj := range []store.Object{service, grant, access} {
		if _, err := backing.PutObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	v, err := backing.View(ctx, catalog.Ranges(""))
	if err != nil {
		t.Fatal(err)
	}
	i := New(backing, backing)
	i.reset(v)
	old, err := i.Snapshot(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	events, err := i.Subscribe(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	// One transaction removes the old owner and grants a replacement. Readers
	// must see the whole revision, never an intermediate union of authorities.
	replacement := object(t, model.BindGrantKind, "new", model.BindGrant{Tenant: "default", Service: "api", Workload: model.Principal{ID: caller}})
	e := model.Endpoint{Tenant: "default", Service: "api", Port: "tcp", InstanceID: "new", Owner: owner, Address: netip.MustParseAddrPort("127.0.0.1:80"), Generation: 1, Ready: true, Serving: true, LeaseExpiresAt: time.Now().Add(time.Minute)}
	data, err := json.Marshal(model.EndpointRecord{Endpoint: e})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := store.Object{Key: model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID), Data: data}
	i.apply(store.RevisionBatch{Revision: v.Revision + 1, Events: []store.Event{{Type: store.EventDelete, Object: grant}, {Type: store.EventPut, Object: replacement}, {Type: store.EventPut, Object: endpoint}}})
	select {
	case <-events:
	default:
		t.Fatal("no revision notification")
	}
	next, err := i.Snapshot(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	nextCat, err := next.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	oldCat, err := old.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	if next.Revision != v.Revision+1 || nextCat.CanBind("default", "api", owner.String()) || !nextCat.CanBind("default", "api", caller.String()) || !oldCat.CanBind("default", "api", owner.String()) {
		t.Fatal("partial transaction or mutated previous snapshot")
	}
	if next.CanBind("api", owner.String()) || !next.CanBind("api", caller.String()) || !old.CanBind("api", owner.String()) {
		t.Fatal("lease admission index mixed grant revisions")
	}
	p, err := catalog.Compile(nextCat, next.Revision, model.Session{Tenant: "default", NodeID: "node", Workload: caller}, time.Now(), time.Minute)
	if err != nil || len(p.Outbound) != 1 || len(p.Outbound[0].Endpoints) != 0 || len(p.Outbound[0].Owners) != 1 || p.Outbound[0].Owners[0] != caller {
		t.Fatalf("new endpoint combined with revoked grant: %+v %v", p, err)
	}
	// Replayed events covered by the initial View cannot overwrite newer state.
	i.apply(store.RevisionBatch{Revision: v.Revision, Events: []store.Event{{Type: store.EventPut, Object: grant}}})
	same, _ := i.Snapshot(ctx, "default")
	if same != next {
		t.Fatal("replayed revision replaced catalog")
	}
	view := next.View()
	for r, objects := range view.Objects {
		if len(objects) > 0 && len(objects[0].Data) > 0 {
			objects[0].Data[0] = '!'
			view.Objects[r] = objects
		}
	}
	cat, err := catalog.Decode(next.View(), "default")
	if err != nil || len(cat.Services) != 1 {
		t.Fatalf("view alias mutated index: %v", err)
	}
}

type restartingStore struct {
	*memory.Store
	starts  atomic.Int32
	streams chan chan store.RevisionBatch
	rebuild chan struct{}
}

func (s *restartingStore) View(ctx context.Context, ranges []store.Range) (store.View, error) {
	if s.starts.Load() > 1 {
		select {
		case <-s.rebuild:
		case <-ctx.Done():
			return store.View{}, ctx.Err()
		}
	}
	return s.Store.View(ctx, ranges)
}

func (s *restartingStore) WatchRevisions(context.Context, string, ...string) (<-chan store.RevisionBatch, error) {
	s.starts.Add(1)
	ch := make(chan store.RevisionBatch, 1)
	s.streams <- ch
	return ch, nil
}
func TestWatchLossRebuildsFromCoherentView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s := &restartingStore{Store: memory.New(), streams: make(chan chan store.RevisionBatch, 4), rebuild: make(chan struct{})}
	i := New(s, s)
	done := make(chan error, 1)
	go func() { done <- i.Run(ctx) }()
	defer func() { cancel(); <-done }()
	first := <-s.streams
	initial, err := i.Snapshot(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	obj := object(t, model.ServiceKind, "api", model.Service{Tenant: "default", Name: "api", Ports: []model.ServicePort{{Name: "tcp", Number: 80, Protocol: "tcp"}}})
	saved, err := s.PutObject(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate overflow: the write is committed but its batch was lost.
	close(first)
	select {
	case <-s.streams:
	case <-ctx.Done():
		t.Fatal("watch not re-established")
	}
	staleCtx, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	if _, err := i.Snapshot(staleCtx, "default"); err == nil {
		t.Fatal("lost watch continued serving stale snapshots before rebuild")
	}
	stop()
	close(s.rebuild)
	for {
		snap, err := i.Snapshot(ctx, "default")
		if err != nil {
			t.Fatal(err)
		}
		if snap.Revision >= saved.ResourceVersion {
			cat, err := snap.Catalog()
			if err != nil || len(cat.Services) != 1 {
				t.Fatal("rebuild lost committed object")
			}
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("index did not catch up")
		}
	}
	if s.starts.Load() != 2 || initial.Revision >= saved.ResourceVersion {
		t.Fatal("invalid watch recovery")
	}
}
