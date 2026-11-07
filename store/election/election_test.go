package election

import (
	"context"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store/memory"
)

func TestElectorActivePassiveTakeoverAfterExpiry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	backend := memory.New()

	active := New(backend, Config{Holder: "controller-a", Advertise: "controller-a:7443", TTL: time.Second, Clock: clock})
	standby := New(backend, Config{Holder: "controller-b", TTL: time.Second, Clock: clock})

	ok, err := active.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("active.TryAcquire() ok=%v err=%v, want active", ok, err)
	}
	if active.Lease().LeaderHint != "controller-a:7443" {
		t.Fatalf("leader hint=%q", active.Lease().LeaderHint)
	}
	ok, err = standby.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("standby.TryAcquire() err=%v", err)
	}
	if ok {
		t.Fatal("standby acquired lease before active expired")
	}

	now = now.Add(2 * time.Second)
	ok, err = standby.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("standby.TryAcquire() after expiry ok=%v err=%v, want active", ok, err)
	}
}

func TestElectorRenewRequiresToken(t *testing.T) {
	t.Parallel()

	backend := memory.New()
	a := New(backend, Config{Holder: "controller-a", TTL: time.Second})
	if ok, err := a.TryAcquire(context.Background()); err != nil || !ok {
		t.Fatalf("TryAcquire() ok=%v err=%v, want active", ok, err)
	}
	if ok, err := a.Renew(context.Background()); err != nil || !ok {
		t.Fatalf("Renew() ok=%v err=%v, want active", ok, err)
	}
}
