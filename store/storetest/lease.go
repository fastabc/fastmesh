package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
)

// RunLeaseExpiryConformance tests the logical deadline independently of etcd's
// server-side TTL. Real server lease deletion needs an additional integration test.
func RunLeaseExpiryConformance(t *testing.T, open func(*testing.T) store.LeaseStore) {
	for _, delta := range []time.Duration{0, time.Second} {
		t.Run(delta.String(), func(t *testing.T) {
			s := open(t)
			ctx := context.Background()
			now := time.Now()
			req := store.LeaseRequest{Name: "expiry", Holder: "a", TTL: time.Second, Now: now}
			first, ok, err := s.AcquireLease(ctx, req)
			if err != nil || !ok {
				t.Fatalf("acquire: %v %v", ok, err)
			}
			req.Token = first.Token
			req.Now = first.ExpiresAt.Add(delta)
			if _, ok, err := s.RenewLease(ctx, req); err != nil || ok {
				t.Fatalf("expired renewal: %v %v", ok, err)
			}
			next, ok, err := s.AcquireLease(ctx, req)
			if err != nil || !ok || next.Token == first.Token {
				t.Fatalf("reacquire: %+v %v %v", next, ok, err)
			}
			if _, ok, err := s.RenewLease(ctx, req); err != nil || ok {
				t.Fatalf("stale token: %v %v", ok, err)
			}
		})
	}
}
