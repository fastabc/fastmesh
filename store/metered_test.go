package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
)

type recordedOp struct {
	op     string
	result string
}

type fakeRecorder struct {
	ops []recordedOp
}

func (r *fakeRecorder) RecordStoreOperation(op, result string, _ time.Duration) {
	r.ops = append(r.ops, recordedOp{op: op, result: result})
}

func (r *fakeRecorder) count(op, result string) int {
	total := 0
	for _, got := range r.ops {
		if got.op == op && got.result == result {
			total++
		}
	}
	return total
}

func TestMeteredStoreRecordsEachOperationAndItsResult(t *testing.T) {
	t.Parallel()

	recorder := &fakeRecorder{}
	metered := store.NewMetered(memory.New(), recorder)
	ctx := context.Background()
	key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: "api"}

	if _, err := metered.PutObject(ctx, store.Object{Key: key, Data: []byte("{}")}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := metered.GetObject(ctx, key); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := metered.GetObject(ctx, store.ObjectKey{Tenant: "default", Kind: "Route", Name: "absent"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get absent: %v", err)
	}

	if got := recorder.count("put", "ok"); got != 1 {
		t.Errorf("put ok = %d, want 1 (ops=%v)", got, recorder.ops)
	}
	if got := recorder.count("get", "ok"); got != 1 {
		t.Errorf("get ok = %d, want 1 (ops=%v)", got, recorder.ops)
	}
	if got := recorder.count("get", "error"); got != 1 {
		t.Errorf("get error = %d, want 1 (ops=%v)", got, recorder.ops)
	}
}

// The store a caller receives is type-asserted for atomic create, conditional
// delete and watch in three production call sites. A decorator that answers
// only ObjectStore would turn every one of those into its "unsupported"
// fallback - watch-driven ADS push would quietly become heartbeat-only, and
// the tests for those features would still pass because they hold the
// undecorated backend.
func TestMeteredStoreForwardsEveryOptionalStoreInterface(t *testing.T) {
	t.Parallel()

	metered := store.NewMetered(memory.New(), &fakeRecorder{})

	if _, ok := any(metered).(store.AtomicObjectCreator); !ok {
		t.Error("metered store does not implement AtomicObjectCreator")
	}
	if _, ok := any(metered).(store.ConditionalObjectDeleter); !ok {
		t.Error("metered store does not implement ConditionalObjectDeleter")
	}
	if _, ok := any(metered).(store.WatchableStore); !ok {
		t.Error("metered store does not implement WatchableStore")
	}
	if _, ok := any(metered).(store.BulkObjectDeleter); !ok {
		t.Error("metered store does not implement BulkObjectDeleter")
	}
}

func TestMeteredStoreBulkDeleteReachesTheBackend(t *testing.T) {
	t.Parallel()

	recorder := &fakeRecorder{}
	metered := store.NewMetered(memory.New(), recorder)
	ctx := context.Background()
	key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: "api"}
	if _, err := metered.PutObject(ctx, store.Object{Key: key, Data: []byte("{}")}); err != nil {
		t.Fatalf("put: %v", err)
	}

	removed, err := store.DeleteObjects(ctx, metered, []store.ObjectKey{key})
	if err != nil {
		t.Fatalf("delete objects: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed = %d, want 1", removed)
	}
	if _, err := metered.GetObject(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("object survived the bulk delete: %v", err)
	}
	if got := recorder.count("delete_objects", "ok"); got != 1 {
		t.Errorf("delete_objects ok = %d, want 1 (ops=%v)", got, recorder.ops)
	}
}

func TestMeteredStoreAtomicCreateStillRejectsDuplicates(t *testing.T) {
	t.Parallel()

	recorder := &fakeRecorder{}
	metered := store.NewMetered(memory.New(), recorder)
	ctx := context.Background()
	obj := store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "Route", Name: "api"}, Data: []byte("{}")}

	if _, err := store.CreateObject(ctx, metered, obj); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := store.CreateObject(ctx, metered, obj); err == nil {
		t.Fatal("second create succeeded; atomic create was not forwarded")
	}
	if got := recorder.count("create", "ok"); got != 1 {
		t.Errorf("create ok = %d, want 1 (ops=%v)", got, recorder.ops)
	}
}

func TestMeteredStoreWatchStillDeliversEvents(t *testing.T) {
	t.Parallel()

	metered := store.NewMetered(memory.New(), &fakeRecorder{})
	watchable, ok := any(metered).(store.WatchableStore)
	if !ok {
		t.Fatal("metered store is not watchable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := watchable.WatchObjects(ctx, "", "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: "api"}
	if _, err := metered.PutObject(ctx, store.Object{Key: key, Data: []byte("{}")}); err != nil {
		t.Fatalf("put: %v", err)
	}
	select {
	case event := <-events:
		if event.Object.Key != key {
			t.Errorf("event key = %v, want %v", event.Object.Key, key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch delivered no event through the metered store")
	}
}

// A nil recorder has to be safe: the assembly builds this store before it
// knows whether metrics are configured.
func TestMeteredStoreWithoutARecorderStillWorks(t *testing.T) {
	t.Parallel()

	metered := store.NewMetered(memory.New(), nil)
	key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: "api"}
	if _, err := metered.PutObject(context.Background(), store.Object{Key: key, Data: []byte("{}")}); err != nil {
		t.Fatalf("put: %v", err)
	}
}
