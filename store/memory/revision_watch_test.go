package memory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func revisionBackend(t *testing.T) *Store { t.Helper(); return New() }
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
