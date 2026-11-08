package memory

import (
	"context"
	"time"

	"github.com/fastabc/fastmesh/store"
)

// GetLease satisfies store.LeaseStore.
func (s *Store) GetLease(_ context.Context, name string) (store.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.leases[name]
	if !ok {
		return store.Lease{}, store.ErrNotFound
	}
	return lease, nil
}

// AcquireLease satisfies store.LeaseStore.
func (s *Store) AcquireLease(_ context.Context, req store.LeaseRequest) (store.Lease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := requestNow(req)
	current, ok := s.leases[req.Name]
	if ok && current.Holder != req.Holder && current.ExpiresAt.After(now) {
		return current, false, nil
	}
	s.version++
	next := store.Lease{
		Name:       req.Name,
		Holder:     req.Holder,
		LeaderHint: req.LeaderHint,
		Token:      s.version,
		ExpiresAt:  now.Add(req.TTL),
		UpdatedAt:  now,
	}
	s.leases[req.Name] = next
	return next, true, nil
}

// RenewLease satisfies store.LeaseStore.
func (s *Store) RenewLease(_ context.Context, req store.LeaseRequest) (store.Lease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.leases[req.Name]
	if !ok {
		return store.Lease{}, false, store.ErrNotFound
	}
	now := requestNow(req)
	if current.Holder != req.Holder || current.Token != req.Token || !current.ExpiresAt.After(now) {
		return current, false, nil
	}
	current.ExpiresAt = now.Add(req.TTL)
	current.UpdatedAt = now
	current.LeaderHint = req.LeaderHint
	s.leases[req.Name] = current
	return current, true, nil
}

// ReleaseLease satisfies store.LeaseStore.
func (s *Store) ReleaseLease(_ context.Context, req store.LeaseRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.leases[req.Name]
	if !ok {
		return store.ErrNotFound
	}
	if current.Holder != req.Holder || current.Token != req.Token {
		return store.ErrConflict
	}
	delete(s.leases, req.Name)
	return nil
}

func requestNow(req store.LeaseRequest) time.Time {
	if req.Now.IsZero() {
		return time.Now().UTC()
	}
	return req.Now.UTC()
}
