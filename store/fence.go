package store

import "context"

type fencedStore struct {
	inner  ObjectStore
	leases LeaseStore
	source FenceTokenSource
}

// FenceToken is the current lease identity used to authorize writes.
type FenceToken struct {
	Name   string
	Holder string
	Token  int64
}

// FenceTokenSource returns the lease identity to validate for one write.
type FenceTokenSource func() FenceToken

// Fenced returns an ObjectStore view that rejects writes unless the configured
// lease is still held by holder/token. Reads pass through unchanged.
func Fenced(inner ObjectStore, leases LeaseStore, name, holder string, token int64) ObjectStore {
	return FencedFunc(inner, leases, func() FenceToken {
		return FenceToken{Name: name, Holder: holder, Token: token}
	})
}

// FencedFunc is like Fenced but reads the lease token dynamically before each
// write. Controllers use this so a standby promoted by renewLoop can write with
// its newly acquired token without rebuilding the server.
func FencedFunc(inner ObjectStore, leases LeaseStore, source FenceTokenSource) ObjectStore {
	if native, ok := inner.(nativeFencer); ok {
		return native.FencedObjectStore(leases, source)
	}
	return fencedStore{
		inner:  inner,
		leases: leases,
		source: source,
	}
}

type nativeFencer interface {
	FencedObjectStore(LeaseStore, FenceTokenSource) ObjectStore
}

func (s fencedStore) GetObject(ctx context.Context, key ObjectKey) (Object, error) {
	return s.inner.GetObject(ctx, key)
}

func (s fencedStore) ListObjects(ctx context.Context, tenant, kind string) ([]Object, error) {
	return s.inner.ListObjects(ctx, tenant, kind)
}

func (s fencedStore) PutObject(ctx context.Context, obj Object) (Object, error) {
	if err := s.check(ctx); err != nil {
		return Object{}, err
	}
	return s.inner.PutObject(ctx, obj)
}

func (s fencedStore) CreateObject(ctx context.Context, obj Object) (Object, error) {
	if err := s.check(ctx); err != nil {
		return Object{}, err
	}
	return CreateObject(ctx, s.inner, obj)
}

func (s fencedStore) DeleteObject(ctx context.Context, key ObjectKey) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.inner.DeleteObject(ctx, key)
}

func (s fencedStore) DeleteObjectIfVersion(ctx context.Context, key ObjectKey, resourceVersion int64) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	return DeleteObjectIfVersion(ctx, s.inner, key, resourceVersion)
}

func (s fencedStore) WatchObjects(ctx context.Context, tenant string, kinds ...string) (<-chan Event, error) {
	watchable, ok := s.inner.(WatchableStore)
	if !ok {
		return nil, ErrWatchUnsupported
	}
	return watchable.WatchObjects(ctx, tenant, kinds...)
}

func (s fencedStore) WatchRevisions(ctx context.Context, tenant string, kinds ...string) (<-chan RevisionBatch, error) {
	watcher, ok := s.inner.(RevisionWatcher)
	if !ok {
		return nil, ErrWatchUnsupported
	}
	return watcher.WatchRevisions(ctx, tenant, kinds...)
}

func (s fencedStore) check(ctx context.Context) error {
	if s.source == nil {
		return ErrConflict
	}
	token := s.source()
	lease, err := s.leases.GetLease(ctx, token.Name)
	if err != nil {
		return err
	}
	if lease.Holder != token.Holder || lease.Token != token.Token {
		return &NotLeaderError{Hint: lease.LeaderHint}
	}
	return nil
}
