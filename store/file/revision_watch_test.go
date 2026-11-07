package file

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func revisionBackend(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}
func TestRevisionWatchConformance(t *testing.T) {
	storetest.RunRevisionWatchConformance(t, func(t *testing.T) store.RevisionWatcher { return revisionBackend(t) })
}
func TestRevisionWatchOverflowCloses(t *testing.T) {
	st := revisionBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches, err := st.WatchRevisions(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, err := st.PutObject(ctx, store.Object{Key: store.ObjectKey{Kind: "Service", Name: fmt.Sprint(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-batches:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("overflow did not close watch")
		}
	}
}

func TestRevisionWatchBulkDeleteIsOneBatch(t *testing.T) {
	st := revisionBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	keys := []store.ObjectKey{{Kind: "Service", Name: "a"}, {Kind: "Service", Name: "b"}, {Kind: "Ignored", Name: "c"}}
	for _, key := range keys {
		if _, err := st.PutObject(ctx, store.Object{Key: key}); err != nil {
			t.Fatal(err)
		}
	}
	batches, err := st.WatchRevisions(ctx, "", "Service")
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := st.DeleteObjects(ctx, keys); err != nil || removed != 3 {
		t.Fatalf("bulk delete: %d %v", removed, err)
	}
	select {
	case batch := <-batches:
		if len(batch.Events) != 2 {
			t.Fatalf("partial transaction: %+v", batch)
		}
		for _, event := range batch.Events {
			if event.Type != store.EventDelete || event.Object.ResourceVersion != batch.Revision {
				t.Fatalf("invalid batch: %+v", batch)
			}
		}
	default:
		t.Fatal("missing batch")
	}
	select {
	case batch := <-batches:
		t.Fatalf("duplicate batch: %+v", batch)
	default:
	}
}
func TestRevisionWatchUncertainCommitCloses(t *testing.T) {
	st := revisionBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches, err := st.WatchRevisions(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	st.syncDirectory = func(string) error { return errors.New("injected directory sync failure") }
	if _, err := st.PutObject(ctx, store.Object{Key: store.ObjectKey{Kind: "Service", Name: "a"}}); !errors.Is(err, store.ErrCommitUncertain) {
		t.Fatalf("error: %v", err)
	}
	select {
	case _, ok := <-batches:
		if ok {
			t.Fatal("uncertain batch published")
		}
	default:
		t.Fatal("uncertain commit kept watch open")
	}
}
