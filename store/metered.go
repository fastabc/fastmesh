package store

import (
	"context"
	"time"
)

// OperationRecorder receives one call per object-store operation.
//
// It is declared here rather than imported from the observability package so
// the dependency points the way the layout expects: the store says what it
// needs measured, and something else decides where that goes.
type OperationRecorder interface {
	// RecordStoreOperation reports one completed operation. op is a bounded
	// verb ("get", "put", ...) and result is "ok" or "error"; neither carries
	// tenant, kind or name, because a metric series lives as long as the
	// process and those are unbounded.
	RecordStoreOperation(op, result string, took time.Duration)
}

// Operation names. They are constants because they are metric label values:
// a typo would silently create a second series rather than fail.
const (
	opGet             = "get"
	opList            = "list"
	opPut             = "put"
	opCreate          = "create"
	opDelete          = "delete"
	opDeleteIfVersion = "delete_if_version"
	opDeleteObjects   = "delete_objects"
	opWatch           = "watch"
	opWatchRevisions  = "watch_revisions"
)

// MeteredStore times every operation on the store it wraps.
//
// Optional interfaces are forwarded explicitly so wrappers retain watches,
// views, bulk operations and native fenced transactions. Omitting a method
// would make callers fall back to a slower or unsupported operation.
type MeteredStore struct {
	next     ObjectStore
	recorder OperationRecorder
}

var (
	_ ObjectStore              = (*MeteredStore)(nil)
	_ AtomicObjectCreator      = (*MeteredStore)(nil)
	_ ConditionalObjectDeleter = (*MeteredStore)(nil)
	_ BulkObjectDeleter        = (*MeteredStore)(nil)
	_ WatchableStore           = (*MeteredStore)(nil)
	_ RevisionWatcher          = (*MeteredStore)(nil)
)

// NewMetered wraps next. A nil recorder is allowed and makes every
// measurement a no-op, so the assembly can build this before it knows whether
// metrics are configured.
func NewMetered(next ObjectStore, recorder OperationRecorder) *MeteredStore {
	return &MeteredStore{next: next, recorder: recorder}
}

// Unwrap returns the wrapped store, for callers that need to reach an
// interface this decorator does not forward.
func (s *MeteredStore) Unwrap() ObjectStore { return s.next }

func (s *MeteredStore) observe(op string, started time.Time, err error) {
	if s.recorder == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	s.recorder.RecordStoreOperation(op, result, time.Since(started))
}

// GetObject implements ObjectStore.
func (s *MeteredStore) GetObject(ctx context.Context, key ObjectKey) (Object, error) {
	started := time.Now()
	obj, err := s.next.GetObject(ctx, key)
	s.observe(opGet, started, err)
	return obj, err
}

// ListObjects implements ObjectStore.
func (s *MeteredStore) ListObjects(ctx context.Context, tenant, kind string) ([]Object, error) {
	started := time.Now()
	objs, err := s.next.ListObjects(ctx, tenant, kind)
	s.observe(opList, started, err)
	return objs, err
}

// PutObject implements ObjectStore.
func (s *MeteredStore) PutObject(ctx context.Context, obj Object) (Object, error) {
	started := time.Now()
	stored, err := s.next.PutObject(ctx, obj)
	s.observe(opPut, started, err)
	return stored, err
}

// DeleteObject implements ObjectStore.
func (s *MeteredStore) DeleteObject(ctx context.Context, key ObjectKey) error {
	started := time.Now()
	err := s.next.DeleteObject(ctx, key)
	s.observe(opDelete, started, err)
	return err
}

// CreateObject implements AtomicObjectCreator by delegating to the wrapped
// store's own atomic primitive.
func (s *MeteredStore) CreateObject(ctx context.Context, obj Object) (Object, error) {
	started := time.Now()
	created, err := CreateObject(ctx, s.next, obj)
	s.observe(opCreate, started, err)
	return created, err
}

// DeleteObjectIfVersion implements ConditionalObjectDeleter.
func (s *MeteredStore) DeleteObjectIfVersion(ctx context.Context, key ObjectKey, resourceVersion int64) error {
	started := time.Now()
	err := DeleteObjectIfVersion(ctx, s.next, key, resourceVersion)
	s.observe(opDeleteIfVersion, started, err)
	return err
}

// DeleteObjects implements BulkObjectDeleter, timing the whole batch as one
// operation because that is what it costs the backend.
func (s *MeteredStore) DeleteObjects(ctx context.Context, keys []ObjectKey) (int, error) {
	started := time.Now()
	removed, err := DeleteObjects(ctx, s.next, keys)
	s.observe(opDeleteObjects, started, err)
	return removed, err
}

// WatchObjects implements WatchableStore. Only establishing the watch is
// timed; the stream it returns is not this decorator's business.
func (s *MeteredStore) WatchObjects(ctx context.Context, tenant string, kinds ...string) (<-chan Event, error) {
	started := time.Now()
	watchable, ok := s.next.(WatchableStore)
	if !ok {
		s.observe(opWatch, started, ErrWatchUnsupported)
		return nil, ErrWatchUnsupported
	}
	events, err := watchable.WatchObjects(ctx, tenant, kinds...)
	s.observe(opWatch, started, err)
	return events, err
}

// WatchRevisions times establishment and preserves the backend's batches.
func (s *MeteredStore) WatchRevisions(ctx context.Context, tenant string, kinds ...string) (<-chan RevisionBatch, error) {
	started := time.Now()
	watcher, ok := s.next.(RevisionWatcher)
	if !ok {
		s.observe(opWatchRevisions, started, ErrWatchUnsupported)
		return nil, ErrWatchUnsupported
	}
	batches, err := watcher.WatchRevisions(ctx, tenant, kinds...)
	s.observe(opWatchRevisions, started, err)
	return batches, err
}
