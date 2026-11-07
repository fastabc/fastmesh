package file

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
)

func seedObjects(t *testing.T, s *Store, names ...string) []store.ObjectKey {
	t.Helper()
	keys := make([]store.ObjectKey, 0, len(names))
	for _, name := range names {
		key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: name}
		if _, err := s.PutObject(context.Background(), store.Object{Key: key, Data: []byte(`{}`)}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		keys = append(keys, key)
	}
	return keys
}

// One bulk delete is one durable transition. This store rewrites and fsyncs its
// whole contents per write, so a bulk delete that saved once per key would cost
// the caller a full rewrite for every object removed - which is the cost the
// primitive exists to avoid.
func TestDeleteObjectsSavesOnce(t *testing.T) {
	t.Parallel()

	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	keys := seedObjects(t, s, "a", "b", "c", "d")

	before := s.state.Version
	removed, err := s.DeleteObjects(context.Background(), keys)
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != len(keys) {
		t.Errorf("removed = %d, want %d", removed, len(keys))
	}
	if got := s.state.Version - before; got != 1 {
		t.Errorf("version advanced by %d for one bulk delete of %d keys, want 1", got, len(keys))
	}
}

func TestDeleteObjectsRemovesOnlyTheKeysGiven(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	keys := seedObjects(t, s, "a", "b", "c")
	if _, err := s.DeleteObjects(context.Background(), keys[:2]); err != nil {
		t.Fatalf("delete objects: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	for _, key := range keys[:2] {
		if _, err := reopened.GetObject(context.Background(), key); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s survived the delete: %v", key.Name, err)
		}
	}
	if _, err := reopened.GetObject(context.Background(), keys[2]); err != nil {
		t.Errorf("c was removed but was not asked for: %v", err)
	}
}

// Deleting an absent key is not an error: an interrupted prune is resumed by
// running it again with the same key set.
func TestDeleteObjectsIgnoresAbsentKeys(t *testing.T) {
	t.Parallel()

	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	keys := seedObjects(t, s, "a")
	keys = append(keys, store.ObjectKey{Tenant: "default", Kind: "Route", Name: "gone"})

	removed, err := s.DeleteObjects(context.Background(), keys)
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed = %d, want 1", removed)
	}
}

// A watcher must hear about every object that went away. A bulk delete that
// saved the file but told nobody would leave every subscriber - the ADS push
// path included - believing the objects are still there until the next
// heartbeat rebuild.
func TestDeleteObjectsBroadcastsOneEventPerObject(t *testing.T) {
	t.Parallel()

	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	keys := seedObjects(t, s, "a", "b")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := s.WatchObjects(ctx, "default")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if _, err := s.DeleteObjects(ctx, keys); err != nil {
		t.Fatalf("delete objects: %v", err)
	}

	seen := map[string]bool{}
	for range keys {
		select {
		case event := <-events:
			if event.Type != store.EventDelete {
				t.Errorf("event type = %v, want delete", event.Type)
			}
			seen[event.Object.Key.Name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d delete events arrived, want %d", len(seen), len(keys))
		}
	}
	for _, key := range keys {
		if !seen[key.Name] {
			t.Errorf("no delete event for %s", key.Name)
		}
	}
}

func TestFileStoreImplementsBulkObjectDeleter(t *testing.T) {
	t.Parallel()

	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, ok := any(s).(store.BulkObjectDeleter); !ok {
		t.Error("file store does not implement store.BulkObjectDeleter")
	}
}
