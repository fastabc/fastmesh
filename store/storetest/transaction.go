package storetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
)

func RunTransactionConformance(t *testing.T, open func(*testing.T) store.ObjectStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := open(t)
	a, err := st.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "Profile", Name: "a"}, Data: []byte("old")})
	if err != nil {
		t.Fatal(err)
	}
	b := store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "Profile", Name: "b"}, Data: []byte("new")}
	watch, err := st.(store.RevisionWatcher).WatchRevisions(ctx, "default", "Profile")
	if err != nil {
		t.Fatal(err)
	}
	a.Data = []byte("new")
	if err := store.ApplyObjects(ctx, st, []store.ObjectChange{{Object: a}, {Object: b}}); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-watch:
		if len(batch.Events) != 2 || batch.Events[0].Object.ResourceVersion != batch.Events[1].Object.ResourceVersion {
			t.Fatalf("not one transaction: %+v", batch)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing transaction watch event")
	}
	before, err := st.ListObjects(ctx, "default", "Profile")
	if err != nil {
		t.Fatal(err)
	}
	a, err = st.GetObject(ctx, a.Key)
	if err != nil {
		t.Fatal(err)
	}
	// b still claims absence: neither a's update nor b's create may happen.
	a.Data = []byte("must not appear")
	if err := store.ApplyObjects(ctx, st, []store.ObjectChange{{Object: a}, {Object: b}}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	after, err := st.ListObjects(ctx, "default", "Profile")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("conflict changed objects: %v", err)
	}
	select {
	case batch := <-watch:
		t.Fatalf("conflict emitted event: %+v", batch)
	default:
	}
	b, err = st.GetObject(ctx, b.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyObjects(ctx, st, []store.ObjectChange{{Object: a, Delete: true}, {Object: b}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetObject(ctx, a.Key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("baseline not retired: %v", err)
	}
	// A metered, fenced production writer must not lose its atomic contract.
	leases := st.(store.LeaseStore)
	lease, acquired, err := leases.AcquireLease(ctx, store.LeaseRequest{Name: "transaction-test", Holder: "leader", TTL: time.Minute})
	if err != nil || !acquired {
		t.Fatalf("lease: %v", err)
	}
	writer := store.Fenced(store.NewMetered(st, nil), leases, lease.Name, lease.Holder, lease.Token)
	b, err = st.GetObject(ctx, b.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyObjects(ctx, writer, []store.ObjectChange{{Object: b}}); err != nil {
		t.Fatal(err)
	}
	if err := leases.ReleaseLease(ctx, store.LeaseRequest{Name: lease.Name, Holder: lease.Holder, Token: lease.Token}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := leases.AcquireLease(ctx, store.LeaseRequest{Name: lease.Name, Holder: "new-leader", TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	b, err = st.GetObject(ctx, b.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyObjects(ctx, writer, []store.ObjectChange{{Object: b, Delete: true}}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale leader accepted: %v", err)
	}
	if _, err := st.GetObject(ctx, b.Key); err != nil {
		t.Fatalf("stale leader changed data: %v", err)
	}
	if err := store.ApplyObjects(ctx, st, []store.ObjectChange{{Object: b}, {Object: b, Delete: true}}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate key accepted: %v", err)
	}
}

func RunConcurrentTransactionView(t *testing.T, open func(*testing.T) store.ObjectStore) {
	ctx := context.Background()
	st := open(t)
	keys := []store.ObjectKey{{Tenant: "default", Kind: "Pair", Name: "a"}, {Tenant: "default", Kind: "Pair", Name: "b"}}
	for _, key := range keys {
		if _, err := st.PutObject(ctx, store.Object{Key: key, Data: []byte("0")}); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			var changes []store.ObjectChange
			for _, key := range keys {
				obj, err := st.GetObject(ctx, key)
				if err != nil {
					done <- err
					return
				}
				obj.Data = []byte{byte(i)}
				changes = append(changes, store.ObjectChange{Object: obj})
			}
			if err := store.ApplyObjects(ctx, st, changes); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	r := store.Range{Tenant: "default", Kind: "Pair"}
	for {
		view, err := st.(store.Viewer).View(ctx, []store.Range{r})
		if err != nil {
			t.Fatal(err)
		}
		pair := view.Objects[r]
		if len(pair) != 2 || !reflect.DeepEqual(pair[0].Data, pair[1].Data) {
			t.Fatalf("half transaction visible: %+v", pair)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
	}
}
