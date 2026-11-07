package file

import (
	"encoding/json"
	"errors"

	"os"
	"path/filepath"
	"sync"

	"github.com/fastabc/fastmesh/internal/fslock"
	"github.com/fastabc/fastmesh/store"
)

var (
	ErrMissingPath = errors.New("file store: missing path")
	ErrLocked      = fslock.ErrLocked
)

// Store persists controller state as a JSON document.
type Store struct {
	mu                 sync.Mutex
	path               string
	lock               *fslock.Lock
	closed             bool
	done               chan struct{}
	nextWatcherID      uint64
	watchers           map[uint64]objectWatcher
	revisionWatchers   map[uint64]revisionWatcher
	droppedWatchEvents int64
	state              state
	syncDirectory      func(string) error
	syncFile           func(*os.File) error
}

var (
	_ store.ObjectStore              = (*Store)(nil)
	_ store.AtomicObjectCreator      = (*Store)(nil)
	_ store.ConditionalObjectDeleter = (*Store)(nil)
	_ store.BulkObjectDeleter        = (*Store)(nil)
	_ store.WatchableStore           = (*Store)(nil)
	_ store.RevisionWatcher          = (*Store)(nil)
	_ store.LeaseStore               = (*Store)(nil)
)

type objectWatcher struct {
	tenant string
	kinds  store.KindFilter
	ch     chan store.Event
}

type state struct {
	Extensions map[string]json.RawMessage `json:"-"`
	Version    int64                      `json:"version"`
	Objects    []store.Object             `json:"objects,omitempty"`
	Leases     []store.Lease              `json:"leases,omitempty"`
}

// Open exclusively owns a file-backed store until Close. Do not remove the
// adjacent .lock file: its stable inode coordinates owners across state renames.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, ErrMissingPath
	}
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	path = filepath.Join(dir, filepath.Base(path))
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	lock, err := fslock.Acquire(path + ".lock")
	if err != nil {
		return nil, err
	}
	s := &Store{path: path, lock: lock, done: make(chan struct{}), watchers: make(map[uint64]objectWatcher)}
	opened := false
	defer func() {
		if !opened {
			_ = lock.Release()
		}
	}()
	if err := s.load(); err != nil {
		return nil, err
	}
	opened = true
	return s, nil
}

// Close releases ownership and terminates watches. It is idempotent. The
// closed store rejects writes; callers must open a new instance to resume.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.done)
	s.invalidateRevisionWatchesLocked()
	for id, watcher := range s.watchers {
		delete(s.watchers, id)
		close(watcher.ch)
	}
	return s.lock.Release()
}

func (s *Store) load() error {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.state = state{}
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) == 0 {
		s.state = state{}
		return nil
	}
	return json.Unmarshal(b, &s.state)
}

// saveLocked rewrites the state file atomically and durably. This store is the
// control plane's record for a single-node deployment, so a rename that lands
// before its data reaches the disk would leave an empty or truncated file after
// an unclean shutdown - losing every tenant, role and object. Both the file and
// the parent directory are fsynced, matching agent state's writeAtomic.
func (s *Store) saveLocked() error { return s.persistLocked(s.state) }

// commitStateLocked publishes a candidate only after persistence. A successful
// rename with uncertain directory durability is already visible and must not
// be rolled back in memory. The caller owns any slices it changes in next.
func (s *Store) commitStateLocked(next state) error {
	err := s.persistLocked(next)
	if err == nil || errors.Is(err, store.ErrCommitUncertain) {
		s.state = next
	}
	return err
}

func (s *Store) persistLocked(next state) error {
	if s.closed {
		return os.ErrClosed
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// No indentation. This file is written in full on every store write and
	// read only by this package; indenting it cost about a third of its bytes
	// and the marshal time to produce them, to format something nothing reads.
	// `jq .` on the file is the answer for the rare human who wants to look.
	enc := json.NewEncoder(tmp)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(next); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	syncFile := s.syncFile
	if syncFile == nil {
		syncFile = (*os.File).Sync
	}
	if err := syncFile(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	sync := s.syncDirectory
	if sync == nil {
		sync = syncDir
	}
	if err := sync(dir); err != nil {
		s.invalidateRevisionWatchesLocked()
		return errors.Join(store.ErrCommitUncertain, err)
	}
	return nil
}

// syncDir flushes the rename itself, so the new name survives a crash too.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *Store) objectIndex(key store.ObjectKey) int {
	for i, obj := range s.state.Objects {
		if obj.Key == key {
			return i
		}
	}
	return -1
}

func (s *Store) leaseIndex(name string) int {
	for i, lease := range s.state.Leases {
		if lease.Name == name {
			return i
		}
	}
	return -1
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
