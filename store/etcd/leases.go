package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// GetLease satisfies store.LeaseStore.
func (s *Store) GetLease(ctx context.Context, name string) (store.Lease, error) {
	resp, err := s.client.Get(ctx, s.leaseKey(name))
	if err != nil {
		return store.Lease{}, err
	}
	if len(resp.Kvs) == 0 {
		return store.Lease{}, store.ErrNotFound
	}
	return decodeLeaseValue(resp.Kvs[0].Value)
}

// AcquireLease satisfies store.LeaseStore.
func (s *Store) AcquireLease(ctx context.Context, req store.LeaseRequest) (store.Lease, bool, error) {
	now := requestNow(req)
	key := s.leaseKey(req.Name)
	current, modRevision, err := s.getLeaseWithRevision(ctx, key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return store.Lease{}, false, err
	}
	if err == nil && current.Holder != req.Holder && current.ExpiresAt.After(now) {
		return current, false, nil
	}
	oldToken := current.Token

	grant, err := s.client.Grant(ctx, ttlSeconds(req.TTL))
	if err != nil {
		return store.Lease{}, false, err
	}
	next := store.Lease{
		Name:       req.Name,
		Holder:     req.Holder,
		LeaderHint: req.LeaderHint,
		Token:      int64(grant.ID),
		ExpiresAt:  now.Add(req.TTL),
		UpdatedAt:  now,
	}
	value, err := json.Marshal(storedLease(next))
	if err != nil {
		_, _ = s.client.Revoke(ctx, grant.ID)
		return store.Lease{}, false, err
	}
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	if modRevision != 0 {
		cmp = clientv3.Compare(clientv3.ModRevision(key), "=", modRevision)
	}
	txn, err := s.client.Txn(ctx).If(cmp).Then(clientv3.OpPut(key, string(value), clientv3.WithLease(grant.ID))).Commit()
	if err != nil {
		_, _ = s.client.Revoke(ctx, grant.ID)
		return store.Lease{}, false, err
	}
	if !txn.Succeeded {
		_, _ = s.client.Revoke(ctx, grant.ID)
		return store.Lease{}, false, store.ErrConflict
	}
	if oldToken != 0 && oldToken != next.Token {
		_, _ = s.client.Revoke(ctx, clientv3.LeaseID(oldToken))
	}
	return next, true, nil
}

// RenewLease satisfies store.LeaseStore.
func (s *Store) RenewLease(ctx context.Context, req store.LeaseRequest) (store.Lease, bool, error) {
	now := requestNow(req)
	key := s.leaseKey(req.Name)
	current, modRevision, err := s.getLeaseWithRevision(ctx, key)
	if err != nil {
		return store.Lease{}, false, err
	}
	if current.Holder != req.Holder || current.Token != req.Token {
		return current, false, nil
	}
	if _, err := s.client.KeepAliveOnce(ctx, clientv3.LeaseID(req.Token)); err != nil {
		return store.Lease{}, false, err
	}
	current.ExpiresAt = now.Add(req.TTL)
	current.UpdatedAt = now
	current.LeaderHint = req.LeaderHint
	value, err := json.Marshal(storedLease(current))
	if err != nil {
		return store.Lease{}, false, err
	}
	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", modRevision)).
		Then(clientv3.OpPut(key, string(value), clientv3.WithLease(clientv3.LeaseID(req.Token)))).
		Commit()
	if err != nil {
		return store.Lease{}, false, err
	}
	if !txn.Succeeded {
		return store.Lease{}, false, store.ErrConflict
	}
	return current, true, nil
}

// ReleaseLease satisfies store.LeaseStore.
func (s *Store) ReleaseLease(ctx context.Context, req store.LeaseRequest) error {
	key := s.leaseKey(req.Name)
	current, modRevision, err := s.getLeaseWithRevision(ctx, key)
	if err != nil {
		return err
	}
	if current.Holder != req.Holder || current.Token != req.Token {
		return store.ErrConflict
	}
	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", modRevision)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return err
	}
	if !txn.Succeeded {
		return store.ErrConflict
	}
	_, _ = s.client.Revoke(ctx, clientv3.LeaseID(req.Token))
	return nil
}

func requestNow(req store.LeaseRequest) time.Time {
	if req.Now.IsZero() {
		return time.Now().UTC()
	}
	return req.Now.UTC()
}
