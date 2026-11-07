package store

import (
	"context"
	"errors"
	"time"
)

var ErrTransactionUnsupported = errors.New("store: atomic object transaction unsupported")

// ObjectChange compares Object.ResourceVersion before replacing or deleting it.
// Version zero requires absence and creates an object; deletion requires a
// positive version. A transaction publishes every change at one revision.
type ObjectChange struct {
	Object Object
	Delete bool
}

type ObjectTransactor interface {
	ApplyObjects(context.Context, []ObjectChange) error
}

// FencedObjectTransactor compares the lease and objects in the same commit.
// Implementations must use their own lease storage, never a preflight check.
type FencedObjectTransactor interface {
	ApplyObjectsFenced(context.Context, []ObjectChange, FenceToken) error
}

func ApplyObjects(ctx context.Context, objects ObjectStore, changes []ObjectChange) error {
	if err := ValidateObjectChanges(changes); err != nil {
		return err
	}
	txn, ok := objects.(ObjectTransactor)
	if !ok {
		return ErrTransactionUnsupported
	}
	return txn.ApplyObjects(ctx, changes)
}

// ValidateObjectChanges rejects ambiguous compare/write requests.
func ValidateObjectChanges(changes []ObjectChange) error {
	seen := make(map[ObjectKey]bool, len(changes))
	for _, change := range changes {
		key := change.Object.Key
		if key.Tenant == "" {
			key.Tenant = "default"
		}
		if seen[key] || change.Object.ResourceVersion < 0 || (change.Delete && change.Object.ResourceVersion == 0) {
			return ErrConflict
		}
		seen[key] = true
	}
	return nil
}

func (s fencedStore) ApplyObjects(ctx context.Context, changes []ObjectChange) error {
	if s.source == nil {
		return ErrConflict
	}
	txn, ok := s.inner.(FencedObjectTransactor)
	if !ok {
		return ErrTransactionUnsupported
	}
	return txn.ApplyObjectsFenced(ctx, changes, s.source())
}

func (s *MeteredStore) ApplyObjects(ctx context.Context, changes []ObjectChange) error {
	started := time.Now()
	err := ApplyObjects(ctx, s.next, changes)
	s.observe("apply_objects", started, err)
	return err
}

func (s *MeteredStore) ApplyObjectsFenced(ctx context.Context, changes []ObjectChange, token FenceToken) error {
	txn, ok := s.next.(FencedObjectTransactor)
	if !ok {
		return ErrTransactionUnsupported
	}
	started := time.Now()
	err := txn.ApplyObjectsFenced(ctx, changes, token)
	s.observe("apply_objects", started, err)
	return err
}
