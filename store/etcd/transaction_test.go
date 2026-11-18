package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func transactionBackend(t *testing.T) store.ObjectStore { return openEtcd(t) }
func TestTransactionConformance(t *testing.T) {
	storetest.RunTransactionConformance(t, transactionBackend)
}
func TestConcurrentTransactionView(t *testing.T) {
	storetest.RunConcurrentTransactionView(t, transactionBackend)
}

type fenceMetrics map[string]int

func (m fenceMetrics) RecordStoreOperation(op, result string, _ time.Duration) {
	m[op+"/"+result]++
}

func TestMeteredEtcdKeepsNativeFence(t *testing.T) {
	s := openEtcd(t)
	ctx := context.Background()
	lease, ok, err := s.AcquireLease(ctx, store.LeaseRequest{Name: "controller", Holder: "active", TTL: time.Minute})
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	metrics := fenceMetrics{}
	for _, rec := range []store.OperationRecorder{nil, metrics} {
		writer := store.Fenced(store.NewMetered(s, rec), s, lease.Name, lease.Holder, lease.Token)
		metered, ok := writer.(*store.MeteredStore)
		if !ok {
			t.Fatalf("fenced writer = %T", writer)
		}
		if _, ok := metered.Unwrap().(fencedObjectStore); !ok {
			t.Fatalf("backend writer = %T", metered.Unwrap())
		}
		obj, err := store.CreateObject(ctx, writer, store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "test", Name: "one"}})
		if err != nil {
			t.Fatal(err)
		}
		obj, err = writer.PutObject(ctx, obj)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteObjectIfVersion(ctx, writer, obj.Key, obj.ResourceVersion); err != nil {
			t.Fatal(err)
		}
		if err := store.ApplyObjects(ctx, writer, []store.ObjectChange{{Object: store.Object{Key: obj.Key}}}); err != nil {
			t.Fatal(err)
		}
		if n, err := store.DeleteObjects(ctx, writer, []store.ObjectKey{obj.Key}); err != nil || n != 1 {
			t.Fatalf("removed=%d err=%v", n, err)
		}
	}
	for _, op := range []string{"create", "put", "delete_if_version", "apply_objects", "delete_objects"} {
		if metrics[op+"/ok"] != 1 {
			t.Errorf("%s: metrics=%v", op, metrics)
		}
	}

	if err := s.ReleaseLease(ctx, store.LeaseRequest{Name: lease.Name, Holder: lease.Holder, Token: lease.Token}); err != nil {
		t.Fatal(err)
	}
	next, ok, err := s.AcquireLease(ctx, store.LeaseRequest{Name: lease.Name, Holder: "next", TTL: time.Minute})
	if err != nil || !ok {
		t.Fatalf("handoff: %v %v", ok, err)
	}
	current := store.Fenced(s, s, next.Name, next.Holder, next.Token)
	obj, err := current.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "test", Name: "survivor"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []store.ObjectStore{s, store.NewMetered(s, metrics)} {
		stale := store.Fenced(backend, s, lease.Name, lease.Holder, lease.Token)
		if n, err := store.DeleteObjects(ctx, stale, []store.ObjectKey{obj.Key}); n != 0 || !errors.Is(err, store.ErrConflict) {
			t.Errorf("%T after handoff: removed=%d err=%v", backend, n, err)
		}
	}
	got, err := s.GetObject(ctx, obj.Key)
	if err != nil || got.ResourceVersion != obj.ResourceVersion {
		t.Fatalf("object after stale writes: version=%d err=%v", got.ResourceVersion, err)
	}
}
