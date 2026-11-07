package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/fastabc/fastmesh/store/storetest"
)

func TestRevisionWatchWrappers(t *testing.T) {
	t.Run("metered", func(t *testing.T) {
		recorder := &fakeRecorder{}
		storetest.RunRevisionWatchConformance(t, func(*testing.T) store.RevisionWatcher { return store.NewMetered(memory.New(), recorder) })
		if recorder.count("watch_revisions", "ok") != 2 {
			t.Fatalf("watch measurements: %+v", recorder.ops)
		}
	})
	t.Run("fenced", func(t *testing.T) {
		backend := memory.New()
		ctx := context.Background()
		lease, ok, err := backend.AcquireLease(ctx, store.LeaseRequest{Name: "controller", Holder: "owner"})
		if err != nil || !ok {
			t.Fatalf("lease: %v %v", ok, err)
		}
		storetest.RunRevisionWatchConformance(t, func(*testing.T) store.RevisionWatcher {
			return store.Fenced(backend, backend, "controller", "owner", lease.Token).(store.RevisionWatcher)
		})
	})
}

func TestRevisionWatchUnsupportedWrappers(t *testing.T) {
	backend := struct{ store.ObjectStore }{memory.New()}
	metered := store.NewMetered(backend, nil)
	if _, err := metered.WatchRevisions(context.Background(), ""); !errors.Is(err, store.ErrWatchUnsupported) {
		t.Fatalf("metered unsupported: %v", err)
	}
	fenced := store.Fenced(backend, memory.New(), "", "", 0).(store.RevisionWatcher)
	if _, err := fenced.WatchRevisions(context.Background(), ""); !errors.Is(err, store.ErrWatchUnsupported) {
		t.Fatalf("fenced unsupported: %v", err)
	}
}
