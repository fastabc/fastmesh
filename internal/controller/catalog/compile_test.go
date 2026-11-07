package catalog_test

import (
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestCompileRecipientScopeAndCoherentRevocation(t *testing.T) {
	s := memory.New()
	publisher, req := seed(t, s)
	consumer := model.Session{Tenant: "acme", NodeID: "node-orders", Workload: spiffeid.RequireFromString("spiffe://test/orders")}
	key := store.ObjectKey{Tenant: "acme", Kind: model.ServiceAccessKind, Name: "orders-payments"}
	put(t, s, key, map[string]any{"spec": model.ServiceAccess{Tenant: "acme", Source: model.Principal{ID: consumer.Workload}, Service: "payments", Port: "api"}})
	m := endpoint.Manager{Objects: s, Viewer: s}
	if _, err := m.Register(ctx, publisher, req); err != nil {
		t.Fatal(err)
	}
	old, err := s.View(ctx, catalog.Ranges("acme"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := compilePlan(old, consumer, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Inbound) != 0 || len(plan.Outbound) != 1 || len(plan.Outbound[0].Endpoints) != 1 || plan.InputRevision != old.Revision {
		t.Fatalf("wrong recipient plan: %+v", plan)
	}
	bound, err := compilePlan(old, publisher, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.Inbound) != 1 || len(bound.Inbound[0].Allow) != 1 || len(bound.Outbound) != 0 {
		t.Fatalf("wrong publisher plan: %+v", bound)
	}
	done := make(chan error, 1)
	go func() {
		if err := s.DeleteObject(ctx, key); err != nil {
			done <- err
			return
		}
		req.Generation++
		req.Address = netip.MustParseAddrPort("127.0.0.1:10002")
		_, err := m.Register(ctx, publisher, req)
		done <- err
	}()
	for {
		view, err := s.View(ctx, catalog.Ranges("acme"))
		if err != nil {
			t.Fatal(err)
		}
		p, err := compilePlan(view, consumer, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range p.Outbound {
			for _, e := range o.Endpoints {
				if e.Address.Port() == 10002 {
					t.Fatal("mixed new instance with revoked permission")
				}
			}
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			goto finished
		default:
		}
	}
finished:
	// Compiling a captured view still sees the original instance, never a
	// fresh endpoint fetched from the store after its authorization was read.
	captured, err := compilePlan(old, consumer, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if captured.Outbound[0].Endpoints[0].Address.Port() != 10001 {
		t.Fatal("compiler read outside View")
	}
	fresh, _ := s.View(ctx, catalog.Ranges("acme"))
	denied, err := compilePlan(fresh, consumer, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(denied.Outbound) != 0 {
		t.Fatal("revocation not reflected")
	}
}

func TestCompileLeaseRenewalAndRevokedBind(t *testing.T) {
	s := memory.New()
	publisher, req := seed(t, s)
	consumer := model.Session{Tenant: "acme", NodeID: "node-orders", Workload: spiffeid.RequireFromString("spiffe://test/orders")}
	put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.ServiceAccessKind, Name: "allow"}, map[string]any{"spec": model.ServiceAccess{Tenant: "acme", Service: "payments", Source: model.Principal{ID: consumer.Workload}}})
	now := time.Now().UTC()
	m := endpoint.Manager{Objects: s, Viewer: s, TTL: time.Minute, Now: func() time.Time { return now }}
	if _, err := m.Register(ctx, publisher, req); err != nil {
		t.Fatal(err)
	}
	compile := func() model.Plan {
		t.Helper()
		v, err := s.View(ctx, catalog.Ranges("acme"))
		if err != nil {
			t.Fatal(err)
		}
		p, err := compilePlan(v, consumer, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	a := compile()
	now = now.Add(time.Second)
	if _, err := m.Renew(ctx, publisher, "payments", req.InstanceID, 1); err != nil {
		t.Fatal(err)
	}
	b := compile()
	if a.Hash != b.Hash || a.InputRevision >= b.InputRevision || !b.Outbound[0].Endpoints[0].LeaseExpiresAt.After(a.Outbound[0].Endpoints[0].LeaseExpiresAt) {
		t.Fatal("renewal lost deadline or changed topology")
	}
	if err := s.DeleteObject(ctx, store.ObjectKey{Tenant: "acme", Kind: model.BindGrantKind, Name: "payments"}); err != nil {
		t.Fatal(err)
	}
	if len(compile().Outbound[0].Endpoints) != 0 {
		t.Fatal("revoked publisher still eligible before expiry reconciliation")
	}
}
