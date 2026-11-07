package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
)

func seed(t *testing.T, objects store.ObjectStore, names ...string) []store.ObjectKey {
	t.Helper()
	keys := make([]store.ObjectKey, 0, len(names))
	for _, name := range names {
		key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: name}
		if _, err := objects.PutObject(context.Background(), store.Object{Key: key, Data: []byte("{}")}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		keys = append(keys, key)
	}
	return keys
}

func TestDeleteObjectsFallsBackToPerKeyDeletes(t *testing.T) {
	t.Parallel()

	objects := memory.New()
	keys := seed(t, objects, "a", "b", "c")

	removed, err := store.DeleteObjects(context.Background(), objects, keys)
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != 3 {
		t.Errorf("removed = %d, want 3", removed)
	}
	for _, key := range keys {
		if _, err := objects.GetObject(context.Background(), key); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s survived the delete: %v", key.Name, err)
		}
	}
}

// A key that is already gone is not an error. The prune tool is resumable: an
// interrupted run leaves some keys deleted, and the next run must not fail on
// the ones it already removed.
func TestDeleteObjectsIgnoresKeysThatAreAlreadyGone(t *testing.T) {
	t.Parallel()

	objects := memory.New()
	keys := seed(t, objects, "a", "b")
	absent := store.ObjectKey{Tenant: "default", Kind: "Route", Name: "gone"}

	removed, err := store.DeleteObjects(context.Background(), objects, append(keys, absent))
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != 2 {
		t.Errorf("removed = %d, want 2 (the absent key must not be counted)", removed)
	}
}

type bulkStore struct {
	store.ObjectStore
	calls int
	batch []int
}

func (s *bulkStore) DeleteObjects(ctx context.Context, keys []store.ObjectKey) (int, error) {
	s.calls++
	s.batch = append(s.batch, len(keys))
	removed := 0
	for _, key := range keys {
		switch err := s.ObjectStore.DeleteObject(ctx, key); {
		case err == nil:
			removed++
		case errors.Is(err, store.ErrNotFound):
		default:
			return removed, err
		}
	}
	return removed, nil
}

// The whole point of the primitive is that a backend which rewrites its entire
// state per write pays that cost once rather than once per key. A helper that
// looped over DeleteObject even when the backend offered a bulk path would
// reintroduce exactly the O(store)-per-write behaviour the prune tool exists to
// clean up after.
func TestDeleteObjectsUsesTheBulkPrimitiveInOneCall(t *testing.T) {
	t.Parallel()

	objects := &bulkStore{ObjectStore: memory.New()}
	keys := seed(t, objects, "a", "b", "c")

	removed, err := store.DeleteObjects(context.Background(), objects, keys)
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != 3 {
		t.Errorf("removed = %d, want 3", removed)
	}
	if objects.calls != 1 {
		t.Errorf("bulk primitive called %d times, want 1 (batches=%v)", objects.calls, objects.batch)
	}
}

func TestDeleteObjectsWithoutKeysLeavesTheStoreAlone(t *testing.T) {
	t.Parallel()

	objects := &bulkStore{ObjectStore: memory.New()}
	removed, err := store.DeleteObjects(context.Background(), objects, nil)
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != 0 {
		t.Errorf("removed = %d, want 0", removed)
	}
	if objects.calls != 0 {
		t.Errorf("bulk primitive called %d times for an empty key set, want 0", objects.calls)
	}
}
