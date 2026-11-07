package endpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/file"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

var ctx = context.Background()

func TestEndpointAuthorityGenerationAndRevocation(t *testing.T) {
	s := memory.New()
	session, req := seed(t, s)
	now := time.Now().UTC()
	m := endpoint.Manager{Objects: s, Viewer: s, TTL: time.Minute, Now: func() time.Time { return now }}
	imposter := session
	imposter.Workload = spiffeid.RequireFromString("spiffe://test/imposter")
	if _, err := m.Register(ctx, imposter, req); !errors.Is(err, endpoint.ErrDenied) {
		t.Fatalf("imposter: %v", err)
	}
	e, err := m.Register(ctx, session, req)
	if err != nil {
		t.Fatal(err)
	}
	if e.Owner != session.Workload || e.Tenant != session.Tenant || e.Generation != 1 {
		t.Fatalf("wrong binding: %+v", e)
	}
	before, _ := s.GetObject(ctx, store.ObjectKey{Tenant: "acme", Kind: model.ServiceKind, Name: "payments"})
	now = now.Add(10 * time.Second)
	renewed, err := m.Renew(ctx, session, req.Service, req.InstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.LeaseExpiresAt.After(e.LeaseExpiresAt) {
		t.Fatal("lease not renewed")
	}
	after, _ := s.GetObject(ctx, before.Key)
	if catalog.TopologyChanged(before, after) {
		t.Fatal("renewal rebuilt topology")
	}
	req.Generation = 2
	req.Address = netip.MustParseAddrPort("127.0.0.1:10002")
	if _, err = m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	if err = m.Deregister(ctx, session, req.Service, req.InstanceID, 1); !errors.Is(err, endpoint.ErrGeneration) {
		t.Fatalf("late deregister: %v", err)
	}
	if _, err = m.Renew(ctx, session, req.Service, req.InstanceID, 1); !errors.Is(err, endpoint.ErrGeneration) {
		t.Fatalf("late renewal: %v", err)
	}
	if err = m.Deregister(ctx, session, req.Service, req.InstanceID, 2); err != nil {
		t.Fatal(err)
	}
	if _, err = m.Register(ctx, session, req); !errors.Is(err, endpoint.ErrGeneration) {
		t.Fatalf("late register resurrected endpoint: %v", err)
	}
	req.Generation = 3
	if _, err = m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteObject(ctx, store.ObjectKey{Tenant: "acme", Kind: model.BindGrantKind, Name: "payments"}); err != nil {
		t.Fatal(err)
	}
	if _, err = m.Renew(ctx, session, req.Service, req.InstanceID, 3); !errors.Is(err, endpoint.ErrDenied) {
		t.Fatalf("revoked bind renewed: %v", err)
	}
	if err = expireCatalog(&m, ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	c, err := readCatalog(m.Viewer, ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Services["payments"].Status.Endpoints) != 0 {
		t.Fatal("revoked endpoint remains")
	}
	if err = s.DeleteObject(ctx, before.Key); err != nil {
		t.Fatal(err)
	}
	if _, err = m.Register(ctx, session, req); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted service registered: %v", err)
	}
}

func TestConcurrentEndpointUpdatesPreserveBothInstancesAndSpec(t *testing.T) {
	s := memory.New()
	session, req := seed(t, s)
	m := endpoint.Manager{Objects: s, Viewer: s}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := range 4 {
		wg.Go(func() {
			r := req
			r.InstanceID = fmt.Sprintf("instance-%d", i)
			_, err := m.Register(ctx, session, r)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	c, err := readCatalog(m.Viewer, ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	record := c.Services["payments"]
	if len(record.Status.Endpoints) != 4 || record.Spec.Ports[0].Number != 8080 {
		t.Fatalf("lost write: %+v", record)
	}
}

func TestExpirySurvivesRestartAndLeaderHandoff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s, err := file.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	session, req := seed(t, s)
	now := time.Now().UTC()
	a, ok, err := s.AcquireLease(ctx, store.LeaseRequest{Name: "catalog", Holder: "a", TTL: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("lease: %v", err)
	}
	m := endpoint.Manager{Objects: store.Fenced(s, s, "catalog", "a", a.Token), Viewer: s, TTL: time.Second, Now: func() time.Time { return now }}
	if _, err = m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseLease(ctx, store.LeaseRequest{Name: "catalog", Holder: "a", Token: a.Token, Now: now}); err != nil {
		t.Fatal(err)
	}
	b, ok, err := s.AcquireLease(ctx, store.LeaseRequest{Name: "catalog", Holder: "b", TTL: time.Minute, Now: now})
	if err != nil || !ok {
		t.Fatalf("lease: %v", err)
	}
	now = now.Add(2 * time.Second)
	if err = expireCatalog(&m, ctx, "acme"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale leader wrote: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := file.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	newLeader := endpoint.Manager{Objects: store.Fenced(reopened, reopened, "catalog", "b", b.Token), Viewer: reopened, Now: func() time.Time { return now }}
	if err = expireCatalog(&newLeader, ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	c, err := readCatalog(newLeader.Viewer, ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Services["payments"].Status.Endpoints) != 0 {
		t.Fatal("expired endpoint survived handoff")
	}
	if _, err = newLeader.Register(ctx, session, req); !errors.Is(err, endpoint.ErrGeneration) {
		t.Fatalf("expiry lost generation fence: %v", err)
	}
	req.Generation++
	if _, err = newLeader.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
}

func TestViewRequiresLoadedRangesAndExactRules(t *testing.T) {
	if _, err := catalog.Decode(store.View{Objects: map[store.Range][]store.Object{}}, "acme"); err == nil {
		t.Fatal("unloaded catalog accepted")
	}
	s := memory.New()
	seed(t, s)
	for _, rule := range []model.ServiceAccess{
		{Tenant: "acme", Service: "*", Source: model.Principal{ID: spiffeid.RequireFromString("spiffe://test/orders")}},
		{Tenant: "acme", Service: "payments", Port: "*", Source: model.Principal{ID: spiffeid.RequireFromString("spiffe://test/orders")}},
		{Tenant: "acme", Service: "payments", Source: model.Principal{ID: spiffeid.RequireFromString("spiffe://test/orders")}, L7: &model.HTTPMatch{Path: "/*"}},
	} {
		if rule.Validate() == nil {
			t.Fatalf("non-exact rule accepted: %+v", rule)
		}
	}
}

func TestStatusCASDoesNotOverwriteConcurrentDesiredEdit(t *testing.T) {
	s := memory.New()
	seed(t, s)
	key := store.ObjectKey{Tenant: "acme", Kind: model.ServiceKind, Name: "payments"}
	old, _ := s.GetObject(ctx, key)
	var record model.ServiceRecord
	if err := json.Unmarshal(old.Data, &record); err != nil {
		t.Fatal(err)
	}
	record.Spec.Ports[0].Number = 9090
	put(t, s, key, record)
	if _, err := store.PutStatusIfVersion(ctx, s, model.CatalogStatusOwner, key, old.ResourceVersion, record.Status); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale status write: %v", err)
	}
	current, _ := s.GetObject(ctx, key)
	if err := json.Unmarshal(current.Data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Spec.Ports[0].Number != 9090 {
		t.Fatal("desired edit lost")
	}
}
