package etcd

import (
	"context"
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
}
