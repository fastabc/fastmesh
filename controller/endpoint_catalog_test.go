package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"testing"

	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func endpointCatalogController(t *testing.T) (*Controller, *memory.Store, model.Session) {
	t.Helper()
	ctx := context.Background()
	s := memory.New()

	// Exercise administration against the same object boundary without owning
	// the controller's concurrent election/index lifecycle.
	c := &Controller{objects: s}
	owner := spiffeid.RequireFromString("spiffe://test/payments")
	session := model.Session{Tenant: "acme", NodeID: "payments", Workload: owner}
	if err := c.PutService(ctx, model.Service{Tenant: "acme", Name: "payments", Ports: []model.ServicePort{{Name: "api", Number: 8080, Protocol: "tcp"}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutBindGrant(ctx, "payments", model.BindGrant{Tenant: "acme", Service: "payments", Workload: model.Principal{ID: owner}}); err != nil {
		t.Fatal(err)
	}
	return c, s, session
}

func TestPutServicePreservesDesiredPortsWithoutMutatingCaller(t *testing.T) {
	c, s, _ := endpointCatalogController(t)
	desired := model.Service{Tenant: "acme", Name: "payments", Ports: []model.ServicePort{{Name: "api", Number: 9090, Protocol: "http"}}, RequiresL7: true}
	if err := c.PutService(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	if desired.Ports[0].Number != 9090 || desired.Ports[0].Protocol != "http" {
		t.Fatalf("caller mutated: %+v", desired)
	}
	obj, err := s.GetObject(context.Background(), store.ObjectKey{Tenant: "acme", Kind: model.ServiceKind, Name: "payments"})
	if err != nil {
		t.Fatal(err)
	}
	var record model.ServiceRecord
	if err := json.Unmarshal(obj.Data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Spec.Ports[0].Number != 9090 || record.Spec.Ports[0].Protocol != "http" || !record.Spec.RequiresL7 {
		t.Fatalf("desired edit lost: %+v", record.Spec)
	}
}

func TestDeleteServiceCascadesLiveEndpointsAndTombstones(t *testing.T) {
	c, s, session := endpointCatalogController(t)
	ctx := context.Background()
	m := endpoint.Manager{Objects: s, Viewer: s}
	req := endpoint.Registration{Service: "payments", Port: "api", InstanceID: "live", Generation: 1, Address: netip.MustParseAddrPort("127.0.0.1:8080"), Ready: true}
	if _, err := m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	req.InstanceID = "removed"
	if _, err := m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	if err := m.Deregister(ctx, session, req.Service, req.InstanceID, req.Generation); err != nil {
		t.Fatal(err)
	}
	if err := c.PutService(ctx, model.Service{Tenant: "acme", Name: "other", Ports: []model.ServicePort{{Name: "api", Number: 8080, Protocol: "tcp"}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutBindGrant(ctx, "other", model.BindGrant{Tenant: "acme", Service: "other", Workload: model.Principal{ID: session.Workload}}); err != nil {
		t.Fatal(err)
	}
	req.Service = "other"
	if _, err := m.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteService(ctx, "acme", "payments"); err != nil {
		t.Fatal(err)
	}
	objects, err := s.ListObjects(ctx, "acme", model.EndpointKind)
	if err != nil || len(objects) != 1 {
		t.Fatalf("cascade left endpoints=%d err=%v", len(objects), err)
	}
	var remaining model.EndpointRecord
	if err := json.Unmarshal(objects[0].Data, &remaining); err != nil {
		t.Fatal(err)
	}
	if remaining.Endpoint.Service != "other" {
		t.Fatal("cascade removed unrelated service")
	}
	req.Service = "payments"
	if _, err := m.Register(ctx, session, req); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted service registered: %v", err)
	}
}
