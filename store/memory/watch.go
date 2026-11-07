package memory

import (
	"context"

	"github.com/fastabc/fastmesh/internal/storeutil"
	"github.com/fastabc/fastmesh/store"
)

// WatchObjects satisfies store.WatchableStore.
func (s *Store) WatchObjects(ctx context.Context, tenantName string, kinds ...string) (<-chan store.Event, error) {
	ch := make(chan store.Event, 16)
	if err := ctx.Err(); err != nil {
		close(ch)
		return ch, nil
	}
	s.mu.Lock()
	id := s.nextWatcherID
	s.nextWatcherID++
	if s.watchers == nil {
		s.watchers = make(map[uint64]objectWatcher)
	}
	s.watchers[id] = objectWatcher{
		tenant: defaultTenantFilter(tenantName),
		kinds:  store.NewKindFilter(kinds),
		ch:     ch,
	}
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if watcher, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(watcher.ch)
		}
		s.mu.Unlock()
	}()
	return ch, nil
}

func (s *Store) broadcastObjectEventLocked(ev store.Event) {
	s.broadcastRevisionLocked(store.RevisionBatch{Revision: ev.Object.ResourceVersion, Events: []store.Event{ev}})
	s.broadcastSingleObjectEventLocked(ev)
}

func (s *Store) broadcastSingleObjectEventLocked(ev store.Event) {
	if len(s.watchers) == 0 {
		return
	}
	ev.Object = storeutil.CloneObject(ev.Object)
	for _, watcher := range s.watchers {
		if watcher.tenant != "" && ev.Object.Key.Tenant != watcher.tenant {
			continue
		}
		if !watcher.kinds.Matches(ev.Object.Key.Kind) {
			continue
		}
		select {
		case watcher.ch <- ev:
		default:
			s.droppedWatchEvents++
		}
	}
}
