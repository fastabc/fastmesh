package etcd

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fastabc/fastmesh/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// GetObject satisfies store.ObjectStore.
func (s *Store) GetObject(ctx context.Context, key store.ObjectKey) (store.Object, error) {
	key = normalizeObjectKey(key)
	resp, err := s.client.Get(ctx, s.objectKey(key))
	if err != nil {
		return store.Object{}, err
	}
	if len(resp.Kvs) == 0 {
		return store.Object{}, store.ErrNotFound
	}
	return decodeObjectKV(resp.Kvs[0])
}

// ListObjects satisfies store.ObjectStore.
func (s *Store) ListObjects(ctx context.Context, tenantName, kind string) ([]store.Object, error) {
	prefix := s.objectListPrefix(tenantName, kind)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]store.Object, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		obj, err := decodeObjectKV(kv)
		if err != nil {
			return nil, err
		}
		if !objectMatches(obj, defaultTenantFilter(tenantName), store.NewKindFilter([]string{kind})) {
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}

// PutObject satisfies store.ObjectStore.
func (s *Store) PutObject(ctx context.Context, obj store.Object) (store.Object, error) {
	obj.Key = normalizeObjectKey(obj.Key)
	obj.UpdatedAt = time.Now().UTC()
	value, err := json.Marshal(storedObject{
		UpdatedAt: obj.UpdatedAt,
		Labels:    obj.Labels,
		Data:      obj.Data,
	})
	if err != nil {
		return store.Object{}, err
	}
	key := s.objectKey(obj.Key)
	var revision int64
	if obj.ResourceVersion != 0 {
		txn, err := s.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", obj.ResourceVersion)).
			Then(clientv3.OpPut(key, string(value))).
			Commit()
		if err != nil {
			return store.Object{}, err
		}
		if !txn.Succeeded {
			return store.Object{}, store.ErrConflict
		}
		revision = txn.Header.Revision
	} else {
		resp, err := s.client.Put(ctx, key, string(value))
		if err != nil {
			return store.Object{}, err
		}
		revision = resp.Header.Revision
	}
	obj.ResourceVersion = revision
	return cloneObject(obj), nil
}

// CreateObject satisfies store.AtomicObjectCreator.
func (s *Store) CreateObject(ctx context.Context, obj store.Object) (store.Object, error) {
	if obj.ResourceVersion != 0 {
		return store.Object{}, store.ErrConflict
	}
	obj.Key = normalizeObjectKey(obj.Key)
	obj.UpdatedAt = time.Now().UTC()
	value, err := json.Marshal(storedObject{
		UpdatedAt: obj.UpdatedAt,
		Labels:    obj.Labels,
		Data:      obj.Data,
	})
	if err != nil {
		return store.Object{}, err
	}
	key := s.objectKey(obj.Key)
	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return store.Object{}, err
	}
	if !txn.Succeeded {
		return store.Object{}, store.ErrConflict
	}
	obj.ResourceVersion = txn.Header.Revision
	return cloneObject(obj), nil
}

func (s fencedObjectStore) PutObject(ctx context.Context, obj store.Object) (store.Object, error) {
	token, err := s.fenceToken(ctx)
	if err != nil {
		return store.Object{}, err
	}
	obj.Key = normalizeObjectKey(obj.Key)
	obj.UpdatedAt = time.Now().UTC()
	value, err := json.Marshal(storedObject{
		UpdatedAt: obj.UpdatedAt,
		Labels:    obj.Labels,
		Data:      obj.Data,
	})
	if err != nil {
		return store.Object{}, err
	}
	key := s.objectKey(obj.Key)
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.LeaseValue(s.leaseKey(token.Name)), "=", token.Token),
	}
	if obj.ResourceVersion != 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", obj.ResourceVersion))
	}
	txn, err := s.client.Txn(ctx).
		If(cmps...).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return store.Object{}, err
	}
	if !txn.Succeeded {
		return store.Object{}, s.conflictError(ctx, token)
	}
	obj.ResourceVersion = txn.Header.Revision
	return cloneObject(obj), nil
}

func (s fencedObjectStore) CreateObject(ctx context.Context, obj store.Object) (store.Object, error) {
	token, err := s.fenceToken(ctx)
	if err != nil {
		return store.Object{}, err
	}
	if obj.ResourceVersion != 0 {
		return store.Object{}, store.ErrConflict
	}
	obj.Key = normalizeObjectKey(obj.Key)
	obj.UpdatedAt = time.Now().UTC()
	value, err := json.Marshal(storedObject{
		UpdatedAt: obj.UpdatedAt,
		Labels:    obj.Labels,
		Data:      obj.Data,
	})
	if err != nil {
		return store.Object{}, err
	}
	key := s.objectKey(obj.Key)
	txn, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.LeaseValue(s.leaseKey(token.Name)), "=", token.Token),
			clientv3.Compare(clientv3.CreateRevision(key), "=", 0),
		).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return store.Object{}, err
	}
	if !txn.Succeeded {
		return store.Object{}, s.conflictError(ctx, token)
	}
	obj.ResourceVersion = txn.Header.Revision
	return cloneObject(obj), nil
}

// DeleteObject satisfies store.ObjectStore.
func (s *Store) DeleteObject(ctx context.Context, key store.ObjectKey) error {
	key = normalizeObjectKey(key)
	resp, err := s.client.Delete(ctx, s.objectKey(key))
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return store.ErrNotFound
	}
	return nil
}

// DeleteObjects satisfies store.BulkObjectDeleter.
//
// Batches are transactions only to save round trips - the deletes are
// independent and idempotent, so a run interrupted between batches is resumed
// by repeating it. Keys that are not present are skipped rather than reported.
func (s *Store) DeleteObjects(ctx context.Context, keys []store.ObjectKey) (int, error) {
	removed := 0
	for start := 0; start < len(keys); start += txnOpLimit {
		end := min(start+txnOpLimit, len(keys))
		ops := make([]clientv3.Op, 0, end-start)
		for _, key := range keys[start:end] {
			ops = append(ops, clientv3.OpDelete(s.objectKey(normalizeObjectKey(key))))
		}
		txn, err := s.client.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return removed, err
		}
		for _, resp := range txn.Responses {
			removed += int(resp.GetResponseDeleteRange().GetDeleted())
		}
	}
	return removed, nil
}

// DeleteObjectIfVersion satisfies store.ConditionalObjectDeleter.
func (s *Store) DeleteObjectIfVersion(ctx context.Context, key store.ObjectKey, resourceVersion int64) error {
	key = normalizeObjectKey(key)
	objectKey := s.objectKey(key)
	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(objectKey), "=", resourceVersion)).
		Then(clientv3.OpDelete(objectKey)).
		Else(clientv3.OpGet(objectKey)).
		Commit()
	if err != nil {
		return err
	}
	if !txn.Succeeded {
		if len(txn.Responses) > 0 && len(txn.Responses[0].GetResponseRange().GetKvs()) == 0 {
			return store.ErrNotFound
		}
		return store.ErrConflict
	}
	if len(txn.Responses) == 0 || txn.Responses[0].GetResponseDeleteRange().GetDeleted() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s fencedObjectStore) DeleteObject(ctx context.Context, key store.ObjectKey) error {
	token, err := s.fenceToken(ctx)
	if err != nil {
		return err
	}
	key = normalizeObjectKey(key)
	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.LeaseValue(s.leaseKey(token.Name)), "=", token.Token)).
		Then(clientv3.OpDelete(s.objectKey(key))).
		Commit()
	if err != nil {
		return err
	}
	if !txn.Succeeded {
		return s.conflictError(ctx, token)
	}
	if len(txn.Responses) == 0 || txn.Responses[0].GetResponseDeleteRange().GetDeleted() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s fencedObjectStore) DeleteObjectIfVersion(ctx context.Context, key store.ObjectKey, resourceVersion int64) error {
	token, err := s.fenceToken(ctx)
	if err != nil {
		return err
	}
	key = normalizeObjectKey(key)
	objectKey := s.objectKey(key)
	txn, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.LeaseValue(s.leaseKey(token.Name)), "=", token.Token),
			clientv3.Compare(clientv3.ModRevision(objectKey), "=", resourceVersion),
		).
		Then(clientv3.OpDelete(objectKey)).
		Commit()
	if err != nil {
		return err
	}
	if !txn.Succeeded {
		return s.conflictError(ctx, token)
	}
	if len(txn.Responses) == 0 || txn.Responses[0].GetResponseDeleteRange().GetDeleted() == 0 {
		return store.ErrNotFound
	}
	return nil
}
