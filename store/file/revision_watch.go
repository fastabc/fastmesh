package file

import (
	"context"
	"os"

	"github.com/fastabc/fastmesh/internal/storeutil"

	"github.com/fastabc/fastmesh/store"
)

type revisionWatcher struct {
	tenant string
	kinds  store.KindFilter
	ch     chan store.RevisionBatch
	done   chan struct{}
}

// WatchRevisions implements store.RevisionWatcher.
func (s *Store) WatchRevisions(ctx context.Context, tenant string, kinds ...string) (<-chan store.RevisionBatch, error) {
	ch := make(chan store.RevisionBatch, 16)
	if ctx.Err() != nil {
		close(ch)
		return ch, nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		close(ch)
		return ch, os.ErrClosed
	}
	id := s.nextWatcherID
	s.nextWatcherID++
	if s.revisionWatchers == nil {
		s.revisionWatchers = make(map[uint64]revisionWatcher)
	}
	watcher := revisionWatcher{tenant: defaultTenantFilter(tenant), kinds: store.NewKindFilter(kinds), ch: ch, done: make(chan struct{})}
	s.revisionWatchers[id] = watcher
	s.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.closeRevisionWatchLocked(id)
			s.mu.Unlock()
		case <-watcher.done:
		}
	}()
	return ch, nil
}

func (s *Store) closeRevisionWatchLocked(id uint64) {
	if watcher, ok := s.revisionWatchers[id]; ok {
		delete(s.revisionWatchers, id)
		close(watcher.ch)
		close(watcher.done)
	}
}

func (s *Store) broadcastRevisionLocked(batch store.RevisionBatch) {
	for id, watcher := range s.revisionWatchers {
		filtered := store.RevisionBatch{Revision: batch.Revision}
		for _, event := range batch.Events {
			if (watcher.tenant == "" || event.Object.Key.Tenant == watcher.tenant) && watcher.kinds.Matches(event.Object.Key.Kind) {
				event.Object = storeutil.CloneObject(event.Object)
				filtered.Events = append(filtered.Events, event)
			}
		}
		if len(filtered.Events) == 0 {
			continue
		}
		select {
		case watcher.ch <- filtered:
		default:
			s.droppedWatchEvents++
			s.closeRevisionWatchLocked(id)
		}
	}
}

func (s *Store) invalidateRevisionWatchesLocked() {
	for id := range s.revisionWatchers {
		s.closeRevisionWatchLocked(id)
	}
}
