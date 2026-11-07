package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func revisionBackend(t *testing.T) *Store { t.Helper(); return openEtcd(t) }
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

func watchPut(t *testing.T, revision int64, kind, name string) *clientv3.Event {
	t.Helper()
	data, err := json.Marshal(storedObject{Data: []byte(name)})
	if err != nil {
		t.Fatal(err)
	}
	return &clientv3.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/test/objects/default/" + kind + "/" + name), Value: data, ModRevision: revision}}
}
func TestRevisionBatchesUseModRevision(t *testing.T) {
	events := []*clientv3.Event{watchPut(t, 10, "Service", "a"), watchPut(t, 10, "Service", "b"), watchPut(t, 11, "Ignored", "c"), watchPut(t, 20, "Service", "d")}
	input := make(chan clientv3.WatchResponse, 1)
	out := make(chan store.RevisionBatch, 16)
	input <- clientv3.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 99}, Events: events}
	close(input)
	streamRevisionBatches(context.Background(), input, out, 9, "", store.NewKindFilter([]string{"Service"}))
	first := <-out
	second := <-out
	if first.Revision != 10 || len(first.Events) != 2 || second.Revision != 20 || len(second.Events) != 1 {
		t.Fatalf("batches: %+v %+v", first, second)
	}
	if _, ok := <-out; ok {
		t.Fatal("unexpected batch")
	}
}
func TestRevisionWatchMalformedClosesWithoutPartialBatch(t *testing.T) {
	input := make(chan clientv3.WatchResponse, 1)
	out := make(chan store.RevisionBatch, 16)
	malformed := watchPut(t, 10, "Service", "b")
	malformed.Kv.Value = []byte("broken")
	input <- clientv3.WatchResponse{Events: []*clientv3.Event{watchPut(t, 10, "Service", "a"), malformed}}
	close(input)
	streamRevisionBatches(context.Background(), input, out, 9, "", nil)
	if _, ok := <-out; ok {
		t.Fatal("partial malformed transaction published")
	}
}
func TestRevisionWatchTransaction(t *testing.T) {
	st := revisionBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches, err := st.WatchRevisions(ctx, "", "Service")
	if err != nil {
		t.Fatal(err)
	}
	value, _ := json.Marshal(storedObject{Data: []byte("value")})
	txn, err := st.client.Txn(ctx).Then(clientv3.OpPut(st.objectKey(store.ObjectKey{Tenant: "default", Kind: "Service", Name: "a"}), string(value)), clientv3.OpPut(st.objectKey(store.ObjectKey{Tenant: "default", Kind: "Service", Name: "b"}), string(value))).Commit()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-batches:
		if batch.Revision != txn.Header.Revision || len(batch.Events) != 2 {
			t.Fatalf("partial transaction: %+v", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("missing transaction")
	}
}

func TestRevisionWatchLossClosesStream(t *testing.T) {
	for name, response := range map[string]clientv3.WatchResponse{
		"canceled":                  {Canceled: true},
		"compacted":                 {Canceled: true, CompactRevision: 10},
		"non_increasing":            {Events: []*clientv3.Event{watchPut(t, 9, "Service", "a")}},
		"regresses_within_response": {Events: []*clientv3.Event{watchPut(t, 10, "Service", "a"), watchPut(t, 9, "Service", "b")}},
	} {
		t.Run(name, func(t *testing.T) {
			input := make(chan clientv3.WatchResponse, 1)
			out := make(chan store.RevisionBatch, 16)
			input <- response
			close(input)
			streamRevisionBatches(context.Background(), input, out, 9, "", nil)
			if _, ok := <-out; ok {
				t.Fatal("lost watch published a batch")
			}
		})
	}
}
