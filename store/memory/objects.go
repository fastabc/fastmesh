package memory

import (
	"context"
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
	obj, ok := s.objects[key]
	if !ok {
		return store.Object{}, store.ErrNotFound
	}
	return storeutil.CloneObject(obj), nil
}

// ListObjects satisfies store.ObjectStore.
func (s *Store) ListObjects(_ context.Context, tenantName, kind string) ([]store.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []store.Object
	for _, obj := range s.objects {
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

	obj.Key = normalizeObjectKey(obj.Key)
	if _, exists := s.objects[obj.Key]; createOnly && exists {
		return store.Object{}, store.ErrConflict
	}
	if obj.ResourceVersion != 0 {
		current, ok := s.objects[obj.Key]
		if !ok || current.ResourceVersion != obj.ResourceVersion {
			return store.Object{}, store.ErrConflict
		}
	}
	s.version++
	obj.ResourceVersion = s.version
	obj.UpdatedAt = time.Now().UTC()
	s.objects[obj.Key] = storeutil.CloneObject(obj)
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

	key = normalizeObjectKey(key)
	current, ok := s.objects[key]
	if !ok {
		return store.ErrNotFound
	}
	if resourceVersion != 0 && current.ResourceVersion != resourceVersion {
		return store.ErrConflict
	}
	s.version++
	deleted := store.Object{Key: key, ResourceVersion: s.version, UpdatedAt: time.Now().UTC()}
	delete(s.objects, key)
	s.broadcastObjectEventLocked(store.Event{Type: store.EventDelete, Object: deleted})
	return nil
}
