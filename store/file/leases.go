package file

import (
	"context"
	"time"

	"github.com/fastabc/fastmesh/store"
)

// GetLease satisfies store.LeaseStore.
func (s *Store) GetLease(_ context.Context, name string) (store.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.leaseIndex(name)
	if idx < 0 {
		return store.Lease{}, store.ErrNotFound
	}
	return s.state.Leases[idx], nil
}

// AcquireLease satisfies store.LeaseStore.
func (s *Store) AcquireLease(_ context.Context, req store.LeaseRequest) (store.Lease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate := s.state
	candidate.Leases = append([]store.Lease(nil), s.state.Leases...)

	now := requestNow(req)
	idx := s.leaseIndex(req.Name)
	if idx >= 0 {
		current := candidate.Leases[idx]
		if current.Holder != req.Holder && current.ExpiresAt.After(now) {
			return current, false, nil
		}
	}
	candidate.Version++
	next := store.Lease{
		Name:       req.Name,
		Holder:     req.Holder,
		LeaderHint: req.LeaderHint,
		Token:      candidate.Version,
		ExpiresAt:  now.Add(req.TTL),
		UpdatedAt:  now,
	}
	if idx >= 0 {
		candidate.Leases[idx] = next
	} else {
		candidate.Leases = append(candidate.Leases, next)
	}
	if err := s.commitStateLocked(candidate); err != nil {
		return store.Lease{}, false, err
	}
	return next, true, nil
}

// RenewLease satisfies store.LeaseStore.
func (s *Store) RenewLease(_ context.Context, req store.LeaseRequest) (store.Lease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate := s.state
	candidate.Leases = append([]store.Lease(nil), s.state.Leases...)

	idx := s.leaseIndex(req.Name)
	if idx < 0 {
		return store.Lease{}, false, store.ErrNotFound
	}
	current := candidate.Leases[idx]
	if current.Holder != req.Holder || current.Token != req.Token {
		return current, false, nil
	}
	now := requestNow(req)
	current.ExpiresAt = now.Add(req.TTL)
	current.UpdatedAt = now
	current.LeaderHint = req.LeaderHint
	candidate.Leases[idx] = current
	if err := s.commitStateLocked(candidate); err != nil {
		return store.Lease{}, false, err
	}
	return current, true, nil
}

// ReleaseLease satisfies store.LeaseStore.
func (s *Store) ReleaseLease(_ context.Context, req store.LeaseRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate := s.state
	candidate.Leases = append([]store.Lease(nil), s.state.Leases...)

	idx := s.leaseIndex(req.Name)
	if idx < 0 {
		return store.ErrNotFound
	}
	current := candidate.Leases[idx]
	if current.Holder != req.Holder || current.Token != req.Token {
		return store.ErrConflict
	}
	candidate.Leases = append(candidate.Leases[:idx], candidate.Leases[idx+1:]...)
	candidate.Version++
	return s.commitStateLocked(candidate)
}

func requestNow(req store.LeaseRequest) time.Time {
	if req.Now.IsZero() {
		return time.Now().UTC()
	}
	return req.Now.UTC()
}
