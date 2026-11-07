package etcd

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fastabc/fastmesh/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (s *Store) ApplyObjects(ctx context.Context, changes []store.ObjectChange) error {
	return s.applyObjects(ctx, changes, nil)
}

func (s *Store) ApplyObjectsFenced(ctx context.Context, changes []store.ObjectChange, token store.FenceToken) error {
	return s.applyObjects(ctx, changes, &token)
}

func (s fencedObjectStore) ApplyObjects(ctx context.Context, changes []store.ObjectChange) error {
	token, err := s.fenceToken(ctx)
	if err != nil {
		return err
	}
	return s.Store.ApplyObjectsFenced(ctx, changes, token)
}

func (s *Store) applyObjects(ctx context.Context, changes []store.ObjectChange, token *store.FenceToken) error {
	if err := store.ValidateObjectChanges(changes); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var cmps []clientv3.Cmp
	if token != nil {
		if token.Name == "" || token.Holder == "" || token.Token == 0 {
			return store.ErrConflict
		}
		lease, revision, err := s.getLeaseWithRevision(ctx, s.leaseKey(token.Name))
		if err != nil {
			return err
		}
		if lease.Holder != token.Holder || lease.Token != token.Token {
			return &store.NotLeaderError{Hint: lease.LeaderHint}
		}
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(s.leaseKey(token.Name)), "=", revision), clientv3.Compare(clientv3.LeaseValue(s.leaseKey(token.Name)), "=", token.Token))
	}
	if len(changes) == 0 {
		return nil
	}
	ops := make([]clientv3.Op, 0, len(changes))
	now := time.Now().UTC()
	for _, change := range changes {
		obj := change.Object
		key := s.objectKey(normalizeObjectKey(obj.Key))
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", obj.ResourceVersion))
		if change.Delete {
			ops = append(ops, clientv3.OpDelete(key))
			continue
		}
		data, err := json.Marshal(storedObject{UpdatedAt: now, Labels: obj.Labels, Data: obj.Data})
		if err != nil {
			return err
		}
		ops = append(ops, clientv3.OpPut(key, string(data)))
	}
	result, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return store.ErrConflict
	}
	return nil
}
