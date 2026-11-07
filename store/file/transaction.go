package file

import (
	"context"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/internal/storeutil"

	"github.com/fastabc/fastmesh/store"
)

func (s *Store) ApplyObjects(ctx context.Context, changes []store.ObjectChange) error {
	return s.applyObjects(ctx, changes, nil)
}

func (s *Store) ApplyObjectsFenced(ctx context.Context, changes []store.ObjectChange, token store.FenceToken) error {
	return s.applyObjects(ctx, changes, &token)
}

func (s *Store) applyObjects(ctx context.Context, changes []store.ObjectChange, token *store.FenceToken) error {
	if err := store.ValidateObjectChanges(changes); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if token != nil {
		idx := s.leaseIndex(token.Name)
		if idx < 0 {
			return store.ErrConflict
		}
		lease := s.state.Leases[idx]
		if token.Token == 0 || lease.Holder != token.Holder || lease.Token != token.Token || !lease.ExpiresAt.After(time.Now()) {
			return &store.NotLeaderError{Hint: lease.LeaderHint}
		}
	}
	if len(changes) == 0 {
		return nil
	}
	changed := make(map[store.ObjectKey]bool, len(changes))
	for _, change := range changes {
		key := normalizeObjectKey(change.Object.Key)
		idx := s.objectIndex(key)
		var version int64
		if idx >= 0 {
			version = s.state.Objects[idx].ResourceVersion
		}
		if version != change.Object.ResourceVersion {
			return store.ErrConflict
		}
		changed[key] = true
	}
	previous, previousVersion := s.state.Objects, s.state.Version
	s.state.Objects = make([]store.Object, 0, len(previous)+len(changes))
	for _, obj := range previous {
		if !changed[obj.Key] {
			s.state.Objects = append(s.state.Objects, obj)
		}
	}
	now := time.Now().UTC()
	batch := store.RevisionBatch{Revision: previousVersion + 1}
	for _, change := range changes {
		obj := storeutil.CloneObject(change.Object)
		obj.Key = normalizeObjectKey(obj.Key)
		obj.ResourceVersion, obj.UpdatedAt = batch.Revision, now
		eventType := store.EventDelete
		if !change.Delete {
			s.state.Objects = append(s.state.Objects, obj)
			eventType = store.EventPut
		}
		batch.Events = append(batch.Events, store.Event{Type: eventType, Object: obj})
	}
	s.state.Version = batch.Revision
	if err := s.saveLocked(); err != nil {
		if !errors.Is(err, store.ErrCommitUncertain) {
			s.state.Objects, s.state.Version = previous, previousVersion
		}
		return err
	}
	for _, event := range batch.Events {
		s.broadcastSingleObjectEventLocked(event)
	}
	s.broadcastRevisionLocked(batch)
	return nil
}
