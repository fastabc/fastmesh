package memory

import (
	"encoding/json"
	"sync"

	"github.com/fastabc/fastmesh/store"
)

// Store is a deterministic in-memory backend for tests and standalone mode.
type Store struct {
	extensions         map[string]json.RawMessage
	mu                 sync.Mutex
	version            int64
	nextWatcherID      uint64
	watchers           map[uint64]objectWatcher
	revisionWatchers   map[uint64]revisionWatcher
	droppedWatchEvents int64
	objects            map[store.ObjectKey]store.Object
	leases             map[string]store.Lease
}

var (
	_ store.ObjectStore              = (*Store)(nil)
	_ store.AtomicObjectCreator      = (*Store)(nil)
	_ store.ConditionalObjectDeleter = (*Store)(nil)
	_ store.WatchableStore           = (*Store)(nil)
	_ store.RevisionWatcher          = (*Store)(nil)
	_ store.LeaseStore               = (*Store)(nil)
)

type objectWatcher struct {
	tenant string
	kinds  store.KindFilter
	ch     chan store.Event
}

// New returns a product-neutral in-memory store.
func New() *Store {
	return &Store{objects: map[store.ObjectKey]store.Object{}, watchers: map[uint64]objectWatcher{}, leases: map[string]store.Lease{}}
}

func normalizeObjectKey(key store.ObjectKey) store.ObjectKey {
	key.Tenant = defaultTenant(key.Tenant)
	return key
}

func defaultTenant(name string) string {
	if name == "" {
		return store.DefaultTenant
	}
	return name
}

func defaultTenantFilter(name string) string {
	if name == "" {
		return ""
	}
	return defaultTenant(name)
}
