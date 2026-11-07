package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
)

func TestFencedWriteRejectedAfterTakeover(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := memory.New()
	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	first, ok, err := st.AcquireLease(ctx, store.LeaseRequest{
		Name:   "controller",
		Holder: "c1",
		TTL:    time.Second,
		Now:    now,
	})
	if err != nil || !ok {
		t.Fatalf("AcquireLease c1 ok=%v err=%v, want acquired", ok, err)
	}
	c1 := store.Fenced(st, st, "controller", "c1", first.Token)
	key := store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Route", Name: "home"}
	written, err := c1.PutObject(ctx, store.Object{Key: key, Data: []byte(`{"path":"/"}`)})
	if err != nil {
		t.Fatalf("fenced PutObject before takeover returned error: %v", err)
	}

	_, ok, err = st.AcquireLease(ctx, store.LeaseRequest{
		Name:       "controller",
		Holder:     "c2",
		LeaderHint: "controller-b:7443",
		TTL:        time.Second,
		Now:        now.Add(2 * time.Second),
	})
	if err != nil || !ok {
		t.Fatalf("AcquireLease c2 ok=%v err=%v, want takeover", ok, err)
	}

	if _, err := c1.PutObject(ctx, store.Object{
		Key:             key,
		ResourceVersion: written.ResourceVersion,
		Data:            []byte(`{"path":"/stale"}`),
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale fenced PutObject err=%v want ErrConflict", err)
	} else if hint, ok := store.LeaderHint(err); !ok || hint != "controller-b:7443" {
		t.Fatalf("leader hint=(%q, %v), want controller-b:7443", hint, ok)
	}
	got, err := st.GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject after rejected write returned error: %v", err)
	}
	if string(got.Data) != `{"path":"/"}` {
		t.Fatalf("object data=%s want original", got.Data)
	}
	if err := c1.DeleteObject(ctx, key); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale fenced DeleteObject err=%v want ErrConflict", err)
	}
}

func TestFencedWatchObjectsDelegates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := memory.New()
	lease, ok, err := st.AcquireLease(ctx, store.LeaseRequest{
		Name:   "controller",
		Holder: "c1",
		TTL:    time.Second,
		Now:    time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC),
	})
	if err != nil || !ok {
		t.Fatalf("AcquireLease ok=%v err=%v, want acquired", ok, err)
	}
	fenced := store.Fenced(st, st, "controller", "c1", lease.Token)
	watchable, ok := fenced.(store.WatchableStore)
	if !ok {
		t.Fatal("fenced store does not implement WatchableStore")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, err := watchable.WatchObjects(watchCtx, store.DefaultTenant, "Route")
	if err != nil {
		t.Fatalf("WatchObjects: %v", err)
	}
	if _, err := fenced.PutObject(ctx, store.Object{
		Key:  store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Route", Name: "home"},
		Data: []byte(`{"path":"/"}`),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	select {
	case ev := <-events:
		if ev.Object.Key.Name != "home" {
			t.Fatalf("watch event key=%+v, want home", ev.Object.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}
