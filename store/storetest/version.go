package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/store"
)

// RunObjectVersionConformance verifies that non-zero resource versions are
// compare-and-swap preconditions for updates and deletes.
func RunObjectVersionConformance(t *testing.T, open func(t *testing.T) store.ObjectStore) {
	t.Helper()
	t.Run("unprovisioned-namespace", func(t *testing.T) {
		s := open(t)
		key := store.ObjectKey{Tenant: "unprovisioned", Kind: "mesh.Service", Name: "one"}
		if _, err := s.PutObject(context.Background(), store.Object{Key: key, Data: []byte(`{}`)}); err != nil {
			t.Fatalf("namespace is not product tenant provisioning: %v", err)
		}
	})

	t.Run("atomic-create", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		creator, ok := st.(store.AtomicObjectCreator)
		if !ok {
			t.Fatalf("%T does not implement AtomicObjectCreator", st)
		}
		key := store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Service", Name: "web"}
		first, err := creator.CreateObject(context.Background(), store.Object{
			Key: key, Data: []byte(`{"value":1}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := creator.CreateObject(context.Background(), store.Object{
			Key: key, Data: []byte(`{"value":2}`),
		}); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("duplicate CreateObject err=%v want ErrConflict", err)
		}
		got, err := st.GetObject(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if got.ResourceVersion != first.ResourceVersion || string(got.Data) != `{"value":1}` {
			t.Fatalf("object=%+v want first create", got)
		}
	})

	t.Run("stale-put-conflicts", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		key := store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Service", Name: "web"}
		first, err := st.PutObject(context.Background(), store.Object{Key: key, Data: []byte(`{"value":1}`)})
		if err != nil {
			t.Fatal(err)
		}
		second, err := st.PutObject(context.Background(), store.Object{
			Key: key, ResourceVersion: first.ResourceVersion, Data: []byte(`{"value":2}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.PutObject(context.Background(), store.Object{
			Key: key, ResourceVersion: first.ResourceVersion, Data: []byte(`{"value":3}`),
		}); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("stale PutObject err=%v want ErrConflict", err)
		}
		got, err := st.GetObject(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if got.ResourceVersion != second.ResourceVersion || string(got.Data) != `{"value":2}` {
			t.Fatalf("object=%+v want second write", got)
		}
	})

	t.Run("conditional-delete", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		deleter, ok := st.(store.ConditionalObjectDeleter)
		if !ok {
			t.Fatalf("%T does not implement ConditionalObjectDeleter", st)
		}
		key := store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Service", Name: "web"}
		first, err := st.PutObject(context.Background(), store.Object{Key: key, Data: []byte(`{"value":1}`)})
		if err != nil {
			t.Fatal(err)
		}
		current, err := st.PutObject(context.Background(), store.Object{
			Key: key, ResourceVersion: first.ResourceVersion, Data: []byte(`{"value":2}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := deleter.DeleteObjectIfVersion(context.Background(), key, first.ResourceVersion); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("stale DeleteObjectIfVersion err=%v want ErrConflict", err)
		}
		if err := deleter.DeleteObjectIfVersion(context.Background(), key, current.ResourceVersion); err != nil {
			t.Fatalf("matched DeleteObjectIfVersion: %v", err)
		}
		if _, err := st.GetObject(context.Background(), key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetObject after delete err=%v want ErrNotFound", err)
		}
	})
}
