package memory

import (
	"context"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func TestWatchConformance(t *testing.T) {
	storetest.RunWatchConformance(t, func(t *testing.T) store.WatchableStore {
		t.Helper()
		return New()
	})
}

func TestObjectVersionConformance(t *testing.T) {
	storetest.RunObjectVersionConformance(t, func(t *testing.T) store.ObjectStore {
		t.Helper()
		return New()
	})
}

func TestLeaseAcquireRenewRelease(t *testing.T) {
	t.Parallel()

	s := New()
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	lease, ok, err := s.AcquireLease(context.Background(), store.LeaseRequest{
		Name:   "controller",
		Holder: "a",
		TTL:    time.Second,
		Now:    now,
	})
	if err != nil || !ok {
		t.Fatalf("AcquireLease ok=%v err=%v, want acquired", ok, err)
	}

	_, ok, err = s.AcquireLease(context.Background(), store.LeaseRequest{
		Name:   "controller",
		Holder: "b",
		TTL:    time.Second,
		Now:    now,
	})
	if err != nil {
		t.Fatalf("AcquireLease contender err=%v", err)
	}
	if ok {
		t.Fatal("contender acquired unexpired lease")
	}

	renewed, ok, err := s.RenewLease(context.Background(), store.LeaseRequest{
		Name:   "controller",
		Holder: "a",
		Token:  lease.Token,
		TTL:    time.Second,
		Now:    now.Add(500 * time.Millisecond),
	})
	if err != nil || !ok {
		t.Fatalf("RenewLease ok=%v err=%v, want renewed", ok, err)
	}
	if !renewed.ExpiresAt.After(lease.ExpiresAt) {
		t.Fatalf("renewed expiry %v did not extend %v", renewed.ExpiresAt, lease.ExpiresAt)
	}

	if err := s.ReleaseLease(context.Background(), store.LeaseRequest{Name: "controller", Holder: "a", Token: lease.Token}); err != nil {
		t.Fatalf("ReleaseLease returned error: %v", err)
	}
}

func TestExtensionConformance(t *testing.T) {
	storetest.RunExtensionConformance(t, func(t *testing.T) storetest.ExtensionBackend { return New() })
}
