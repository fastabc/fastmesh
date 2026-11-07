package memory

import (
	"context"
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
		lease, ok := s.leases[token.Name]
		if !ok || token.Token == 0 || lease.Holder != token.Holder || lease.Token != token.Token || !lease.ExpiresAt.After(time.Now()) {
			return &store.NotLeaderError{Hint: lease.LeaderHint}
		}
	}
	if len(changes) == 0 {
		return nil
	}
	for _, change := range changes {
		key := normalizeObjectKey(change.Object.Key)
		current := s.objects[key]
		if current.ResourceVersion != change.Object.ResourceVersion {
			return store.ErrConflict
		}
	}
	previous := s.objects
	s.objects = make(map[store.ObjectKey]store.Object, len(previous)+len(changes))
	for key, obj := range previous {
		s.objects[key] = obj
	}
	// Remove compared objects before checking the replacement set's quotas.
	for _, change := range changes {
		delete(s.objects, normalizeObjectKey(change.Object.Key))
	}
	now := time.Now().UTC()
	batch := store.RevisionBatch{Revision: s.version + 1}
	for _, change := range changes {
		obj := storeutil.CloneObject(change.Object)
		obj.Key = normalizeObjectKey(obj.Key)
		obj.ResourceVersion, obj.UpdatedAt = batch.Revision, now
		eventType := store.EventDelete
		if !change.Delete {
			s.objects[obj.Key] = obj
			eventType = store.EventPut
		}
		batch.Events = append(batch.Events, store.Event{Type: eventType, Object: obj})
	}
	s.version = batch.Revision
	for _, event := range batch.Events {
		s.broadcastSingleObjectEventLocked(event)
	}
	s.broadcastRevisionLocked(batch)
	return nil
}
