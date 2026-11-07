package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
)

// RunRevisionWatchConformance verifies revision filtering and payload ownership.
func RunRevisionWatchConformance(t *testing.T, open func(*testing.T) store.RevisionWatcher) {
	t.Helper()
	backend := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := backend.WatchRevisions(ctx, "", "Service")
	if err != nil {
		t.Fatal(err)
	}
	second, err := backend.WatchRevisions(ctx, "", "Service")
	if err != nil {
		t.Fatal(err)
	}
	obj := store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "Service", Name: "a"}, Data: []byte("original"), Labels: map[string]string{"label": "original"}}
	stored, err := backend.PutObject(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}
	obj.Data[0] = 'x'
	obj.Labels["label"] = "changed"
	batch := receiveRevision(t, first)
	if batch.Revision != stored.ResourceVersion || len(batch.Events) != 1 || batch.Events[0].Object.ResourceVersion != batch.Revision {
		t.Fatalf("invalid batch: %+v", batch)
	}
	if string(batch.Events[0].Object.Data) != "original" || batch.Events[0].Object.Labels["label"] != "original" {
		t.Fatalf("writer mutated batch: %+v", batch)
	}
	batch.Events[0].Object.Data[0] = 'y'
	batch.Events[0].Object.Labels["label"] = "mutated"
	other := receiveRevision(t, second)
	if string(other.Events[0].Object.Data) != "original" || other.Events[0].Object.Labels["label"] != "original" {
		t.Fatalf("watchers share payload: %+v", other)
	}
	got, err := backend.GetObject(ctx, stored.Key)
	if err != nil || string(got.Data) != "original" || got.Labels["label"] != "original" {
		t.Fatalf("watch mutated backend: %+v %v", got, err)
	}
	if _, err := backend.PutObject(ctx, store.Object{Key: store.ObjectKey{Kind: "Unrelated", Name: "other"}}); err != nil {
		t.Fatal(err)
	}
	if err := backend.DeleteObject(ctx, stored.Key); err != nil {
		t.Fatal(err)
	}
	deletion := receiveRevision(t, first)
	if deletion.Revision <= batch.Revision+1 || len(deletion.Events) != 1 || deletion.Events[0].Type != store.EventDelete {
		t.Fatalf("unrelated revision not filtered: %+v", deletion)
	}
	cancel()
	select {
	case _, ok := <-first:
		if ok {
			t.Fatal("unexpected batch after cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not close watch")
	}
}
func receiveRevision(t *testing.T, batches <-chan store.RevisionBatch) store.RevisionBatch {
	t.Helper()
	select {
	case batch, ok := <-batches:
		if !ok {
			t.Fatal("watch closed before batch")
		}
		return batch
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for batch")
		return store.RevisionBatch{}
	}
}
