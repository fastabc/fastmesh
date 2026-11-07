package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalogrpc"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestCompileOwnersSurviveTerminationDeletionAndExpiry(t *testing.T) {
	s := memory.New()
	publisher, req := seed(t, s)
	consumer := model.Session{Tenant: "acme", NodeID: "orders", Workload: spiffeid.RequireFromString("spiffe://test/orders")}
	put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.ServiceAccessKind, Name: "allow"}, map[string]any{"spec": model.ServiceAccess{Tenant: "acme", Service: "payments", Source: model.Principal{ID: consumer.Workload}}})
	now := time.Now().UTC()
	m := endpoint.Manager{Objects: s, Viewer: s, TTL: time.Minute, Now: func() time.Time { return now }}
	compile := func() model.OutboundService {
		t.Helper()
		v, err := s.View(ctx, catalog.Ranges("acme"))
		if err != nil {
			t.Fatal(err)
		}
		p, err := compilePlan(v, consumer, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Outbound) != 1 {
			t.Fatalf("outbound=%+v", p.Outbound)
		}
		return p.Outbound[0]
	}
	assertOwner := func(out model.OutboundService) {
		t.Helper()
		if len(out.Owners) != 1 || out.Owners[0] != publisher.Workload {
			t.Fatalf("owners=%v", out.Owners)
		}
	}
	assertOwner(compile()) // Bind authorization does not require an instance.
	e, err := m.Register(ctx, publisher, req)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Serving || !e.Ready || e.Terminating {
		t.Fatalf("initial conditions=%+v", e)
	}
	req.Generation++
	req.Ready, req.Serving, req.Terminating = false, true, true
	e, err = m.Register(ctx, publisher, req)
	if err != nil {
		t.Fatal(err)
	}
	if e.Ready || !e.Serving || !e.Terminating {
		t.Fatalf("terminating conditions=%+v", e)
	}
	out := compile()
	assertOwner(out)
	if len(out.Endpoints) != 1 || out.Endpoints[0].Ready || !out.Endpoints[0].Terminating {
		t.Fatalf("termination lost: %+v", out)
	}
	if err := m.Deregister(ctx, publisher, req.Service, req.InstanceID, req.Generation); err != nil {
		t.Fatal(err)
	}
	out = compile()
	assertOwner(out)
	if len(out.Endpoints) != 0 {
		t.Fatal("deleted instance still selected")
	}
	req.Generation++
	req.Terminating = false
	if _, err := m.Register(ctx, publisher, req); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	assertOwner(compile())
	if len(compile().Endpoints) != 0 {
		t.Fatal("expired instance still selected")
	}
	if err := s.DeleteObject(ctx, store.ObjectKey{Tenant: "acme", Kind: model.BindGrantKind, Name: "payments"}); err != nil {
		t.Fatal(err)
	}
	if len(compile().Owners) != 0 {
		t.Fatal("revoked owner survived")
	}
}

func TestRegisterRPCAdditiveConditions(t *testing.T) {
	s := memory.New()
	session, req := seed(t, s)
	m := &endpoint.Manager{Objects: s, Viewer: s}
	rpc := catalogrpc.Server{Manager: m, AuthorizeSession: func(_ context.Context, _ string) (model.Session, error) { return session, nil }}
	for _, terminating := range []bool{false, true} {
		_, err := rpc.RegisterEndpoint(ctx, &catalogv1.RegisterEndpointRequest{NodeId: session.NodeID, Service: req.Service, Port: req.Port, InstanceId: req.InstanceID, Generation: req.Generation, Address: req.Address.String(), Serving: true, Terminating: terminating})
		if err != nil {
			t.Fatal(err)
		}
		c, err := readCatalog(m.Viewer, ctx, session.Tenant)
		if err != nil {
			t.Fatal(err)
		}
		e := c.Services[req.Service].Status.Endpoints[0]
		if !e.Serving || e.Terminating != terminating || e.Ready == terminating {
			t.Fatalf("conditions=%+v", e)
		}
		req.Generation++
	}
}
