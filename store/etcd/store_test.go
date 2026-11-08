package etcd

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/test/fixture"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func TestWatchConformance(t *testing.T) {
	storetest.RunWatchConformance(t, func(t *testing.T) store.WatchableStore {
		t.Helper()
		return openEtcd(t)
	})
}

func TestObjectVersionConformance(t *testing.T) {
	storetest.RunObjectVersionConformance(t, func(t *testing.T) store.ObjectStore {
		t.Helper()
		return openEtcd(t)
	})
}

func TestObjectCASConflict(t *testing.T) {
	t.Parallel()

	st := openEtcd(t)
	key := store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Route", Name: "home"}
	first, err := st.PutObject(context.Background(), store.Object{Key: key, Data: []byte(`{"path":"/"}`)})
	if err != nil {
		t.Fatalf("PutObject first returned error: %v", err)
	}
	if _, err := st.PutObject(context.Background(), store.Object{
		Key:             key,
		ResourceVersion: first.ResourceVersion,
		Data:            []byte(`{"path":"/new"}`),
	}); err != nil {
		t.Fatalf("PutObject update returned error: %v", err)
	}
	if _, err := st.PutObject(context.Background(), store.Object{
		Key:             key,
		ResourceVersion: first.ResourceVersion,
		Data:            []byte(`{"path":"/stale"}`),
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale PutObject err=%v want ErrConflict", err)
	}
}

func TestLeaseAcquireRenewRelease(t *testing.T) {
	t.Parallel()

	st := openEtcd(t)
	now := time.Now().UTC()
	lease, ok, err := st.AcquireLease(context.Background(), store.LeaseRequest{
		Name:   "controller",
		Holder: "a",
		TTL:    5 * time.Second,
		Now:    now,
	})
	if err != nil || !ok {
		t.Fatalf("AcquireLease ok=%v err=%v, want acquired", ok, err)
	}
	if lease.Token == 0 {
		t.Fatal("lease token=0 want etcd lease id")
	}

	contender, ok, err := st.AcquireLease(context.Background(), store.LeaseRequest{
		Name:   "controller",
		Holder: "b",
		TTL:    5 * time.Second,
		Now:    now,
	})
	if err != nil {
		t.Fatalf("AcquireLease contender err=%v", err)
	}
	if ok {
		t.Fatal("contender acquired unexpired lease")
	}
	if contender.Holder != "a" {
		t.Fatalf("contender saw holder=%q want a", contender.Holder)
	}

	renewed, ok, err := st.RenewLease(context.Background(), store.LeaseRequest{
		Name:   "controller",
		Holder: "a",
		Token:  lease.Token,
		TTL:    5 * time.Second,
		Now:    now.Add(time.Second),
	})
	if err != nil || !ok {
		t.Fatalf("RenewLease ok=%v err=%v, want renewed", ok, err)
	}
	if !renewed.ExpiresAt.After(lease.ExpiresAt) {
		t.Fatalf("renewed expiry %v did not extend %v", renewed.ExpiresAt, lease.ExpiresAt)
	}

	if err := st.ReleaseLease(context.Background(), store.LeaseRequest{Name: "controller", Holder: "a", Token: lease.Token}); err != nil {
		t.Fatalf("ReleaseLease returned error: %v", err)
	}
	if _, err := st.GetLease(context.Background(), "controller"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetLease deleted err=%v want ErrNotFound", err)
	}
}

func openEtcd(t *testing.T) *Store {
	t.Helper()
	raw := fixture.EtcdEndpoints(t)
	if raw == "" {
		t.Skip("set FASTMESH_ETCD_ENDPOINTS to run etcd integration tests")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   splitEndpoints(raw),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	// Wall-clock nanoseconds can collide between parallel conformance cases.
	prefix := "/example-test/" + rand.Text()
	t.Cleanup(func() {
		_, _ = client.Delete(context.Background(), prefix, clientv3.WithPrefix())
		_ = client.Close()
	})
	return New(client, prefix)
}

func splitEndpoints(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func TestDefaultPrefixBelongsToMesh(t *testing.T) {
	if got := New(nil, "").Prefix(); got != "/fastmesh/v1" {
		t.Fatalf("default prefix = %q", got)
	}
	if got := New(nil, "/application/v1/").Prefix(); got != "/application/v1" {
		t.Fatalf("explicit prefix = %q", got)
	}
}

func TestLeaseExpiryConformance(t *testing.T) {
	storetest.RunLeaseExpiryConformance(t, func(t *testing.T) store.LeaseStore { return openEtcd(t) })
}

func TestExpiredServerLeaseCannotRenew(t *testing.T) {
	s := openEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, ok, err := s.AcquireLease(ctx, store.LeaseRequest{Name: "server-expiry", Holder: "old", TTL: time.Second})
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	for {
		_, err = s.GetLease(ctx, lease.Name)
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("server lease did not expire")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if _, ok, err := s.RenewLease(ctx, store.LeaseRequest{Name: lease.Name, Holder: lease.Holder, Token: lease.Token, TTL: time.Second}); ok || !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("renew deleted server lease: %v %v", ok, err)
	}
}
