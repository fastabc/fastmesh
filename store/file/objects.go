package file

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/fastabc/fastmesh/internal/storeutil"
	"github.com/fastabc/fastmesh/store"
)

// GetObject satisfies store.ObjectStore.
func (s *Store) GetObject(_ context.Context, key store.ObjectKey) (store.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key = normalizeObjectKey(key)
	idx := s.objectIndex(key)
	if idx < 0 {
		return store.Object{}, store.ErrNotFound
	}
	return storeutil.CloneObject(s.state.Objects[idx]), nil
}

// ListObjects satisfies store.ObjectStore.
func (s *Store) ListObjects(_ context.Context, tenantName, kind string) ([]store.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []store.Object
	for _, obj := range s.state.Objects {
		if tenantName != "" && obj.Key.Tenant != tenantName {
			continue
		}
		if kind != "" && obj.Key.Kind != kind {
			continue
		}
		out = append(out, storeutil.CloneObject(obj))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.Tenant != out[j].Key.Tenant {
			return out[i].Key.Tenant < out[j].Key.Tenant
		}
		if out[i].Key.Kind != out[j].Key.Kind {
			return out[i].Key.Kind < out[j].Key.Kind
		}
		return out[i].Key.Name < out[j].Key.Name
	})
	return out, nil
}

// PutObject satisfies store.ObjectStore.
func (s *Store) PutObject(_ context.Context, obj store.Object) (store.Object, error) {
	return s.putObject(obj, false)
}

// CreateObject satisfies store.AtomicObjectCreator.
func (s *Store) CreateObject(_ context.Context, obj store.Object) (store.Object, error) {
	return s.putObject(obj, true)
}

func (s *Store) putObject(obj store.Object, createOnly bool) (store.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	previousVersion := s.state.Version
	previousObjects := append([]store.Object(nil), s.state.Objects...)

	obj.Key = normalizeObjectKey(obj.Key)
	if createOnly && s.objectIndex(obj.Key) >= 0 {
		return store.Object{}, store.ErrConflict
	}
	if obj.ResourceVersion != 0 {
		idx := s.objectIndex(obj.Key)
		if idx < 0 || s.state.Objects[idx].ResourceVersion != obj.ResourceVersion {
			return store.Object{}, store.ErrConflict
		}
	}
	s.state.Version++
	obj.ResourceVersion = s.state.Version
	obj.UpdatedAt = time.Now().UTC()
	idx := s.objectIndex(obj.Key)
	if idx >= 0 {
		s.state.Objects[idx] = storeutil.CloneObject(obj)
	} else {
		s.state.Objects = append(s.state.Objects, storeutil.CloneObject(obj))
	}
	if err := s.saveLocked(); err != nil {
		if !errors.Is(err, store.ErrCommitUncertain) {
			s.state.Version = previousVersion
			s.state.Objects = previousObjects
		}
		return store.Object{}, err
	}
	s.broadcastObjectEventLocked(store.Event{Type: store.EventPut, Object: obj})
	return storeutil.CloneObject(obj), nil
}

// DeleteObject satisfies store.ObjectStore.
func (s *Store) DeleteObject(_ context.Context, key store.ObjectKey) error {
	return s.deleteObject(key, 0)
}

// DeleteObjectIfVersion satisfies store.ConditionalObjectDeleter.
func (s *Store) DeleteObjectIfVersion(_ context.Context, key store.ObjectKey, resourceVersion int64) error {
	return s.deleteObject(key, resourceVersion)
}

func (s *Store) deleteObject(key store.ObjectKey, resourceVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	previousVersion := s.state.Version
	previousObjects := append([]store.Object(nil), s.state.Objects...)

	key = normalizeObjectKey(key)
	idx := s.objectIndex(key)
	if idx < 0 {
		return store.ErrNotFound
	}
	if resourceVersion != 0 && s.state.Objects[idx].ResourceVersion != resourceVersion {
		return store.ErrConflict
	}
	s.state.Objects = append(s.state.Objects[:idx], s.state.Objects[idx+1:]...)
	s.state.Version++
	deleted := store.Object{Key: key, ResourceVersion: s.state.Version, UpdatedAt: time.Now().UTC()}
	if err := s.saveLocked(); err != nil {
		if !errors.Is(err, store.ErrCommitUncertain) {
			s.state.Version = previousVersion
			s.state.Objects = previousObjects
		}
		return err
	}
	s.broadcastObjectEventLocked(store.Event{Type: store.EventDelete, Object: deleted})
	return nil
}

// DeleteObjects satisfies store.BulkObjectDeleter.
//
// Every key is removed under one lock and persisted by one save. That matters
// here more than anywhere else: saveLocked rewrites and fsyncs the entire file,
// so deleting keys one at a time would cost one full rewrite per key of a store
// whose size is the reason the keys are being deleted.
//
// Keys that are not present are skipped rather than reported, so an interrupted
// run is resumed by repeating it.
func (s *Store) DeleteObjects(_ context.Context, keys []store.ObjectKey) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	previousVersion := s.state.Version
	previousObjects := append([]store.Object(nil), s.state.Objects...)

	doomed := make(map[store.ObjectKey]bool, len(keys))
	for _, key := range keys {
		doomed[normalizeObjectKey(key)] = true
	}
	kept := s.state.Objects[:0]
	removed := make([]store.ObjectKey, 0, len(keys))
	for _, obj := range s.state.Objects {
		if doomed[obj.Key] {
			removed = append(removed, obj.Key)
			continue
		}
		kept = append(kept, obj)
	}
	if len(removed) == 0 {
		return 0, nil
	}
	s.state.Objects = kept
	s.state.Version++
	if err := s.saveLocked(); err != nil {
		if !errors.Is(err, store.ErrCommitUncertain) {
			s.state.Version = previousVersion
			s.state.Objects = previousObjects
		}
		return 0, err
	}
	now := time.Now().UTC()
	batch := store.RevisionBatch{Revision: s.state.Version, Events: make([]store.Event, 0, len(removed))}
	for _, key := range removed {
		event := store.Event{Type: store.EventDelete, Object: store.Object{Key: key, ResourceVersion: s.state.Version, UpdatedAt: now}}
		batch.Events = append(batch.Events, event)
		s.broadcastSingleObjectEventLocked(event)
	}
	s.broadcastRevisionLocked(batch)
	return len(removed), nil
}
