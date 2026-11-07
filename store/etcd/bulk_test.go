package etcd

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fastabc/fastmesh/store"
)

func TestEtcdStoreImplementsBulkObjectDeleter(t *testing.T) {
	t.Parallel()

	if _, ok := any((*Store)(nil)).(store.BulkObjectDeleter); !ok {
		t.Error("etcd store does not implement store.BulkObjectDeleter")
	}
}

func TestDeleteObjectsRemovesOnlyTheKeysGiven(t *testing.T) {
	s := openEtcd(t)
	ctx := context.Background()

	keep := store.ObjectKey{Tenant: "default", Kind: "Route", Name: "keep"}
	var doomed []store.ObjectKey
	for i := range 3 {
		doomed = append(doomed, store.ObjectKey{Tenant: "default", Kind: "Route", Name: fmt.Sprintf("drop-%d", i)})
	}
	for _, key := range append([]store.ObjectKey{keep}, doomed...) {
		if _, err := s.PutObject(ctx, store.Object{Key: key, Data: []byte(`{}`)}); err != nil {
			t.Fatalf("seed %s: %v", key.Name, err)
		}
	}

	removed, err := s.DeleteObjects(ctx, doomed)
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != len(doomed) {
		t.Errorf("removed = %d, want %d", removed, len(doomed))
	}
	for _, key := range doomed {
		if _, err := s.GetObject(ctx, key); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s survived the delete: %v", key.Name, err)
		}
	}
	if _, err := s.GetObject(ctx, keep); err != nil {
		t.Errorf("keep was removed but was not asked for: %v", err)
	}
}

// etcd caps the number of operations in one transaction, so a prune of a large
// store has to be split. Every batch must still land, and keys already gone
// from an earlier interrupted run must not fail the run that follows.
func TestDeleteObjectsSpansMoreKeysThanOneTransactionHolds(t *testing.T) {
	s := openEtcd(t)
	ctx := context.Background()

	const count = txnOpLimit + 17
	keys := make([]store.ObjectKey, 0, count)
	for i := range count {
		key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: fmt.Sprintf("bulk-%03d", i)}
		if _, err := s.PutObject(ctx, store.Object{Key: key, Data: []byte(`{}`)}); err != nil {
			t.Fatalf("seed %s: %v", key.Name, err)
		}
		keys = append(keys, key)
	}
	keys = append(keys, store.ObjectKey{Tenant: "default", Kind: "Route", Name: "never-existed"})

	removed, err := s.DeleteObjects(ctx, keys)
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != count {
		t.Errorf("removed = %d, want %d (absent keys must not be counted)", removed, count)
	}
	left, err := s.ListObjects(ctx, "default", "Route")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("%d Route objects survived the bulk delete", len(left))
	}
}
