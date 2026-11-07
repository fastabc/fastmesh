package catalog_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestGroupAndExactRulesCompileEquivalent(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	publisher, req := seed(t, s)
	manager := endpoint.Manager{Objects: s, Viewer: s}
	if _, err := manager.Register(ctx, publisher, req); err != nil {
		t.Fatal(err)
	}
	consumer := model.Session{Tenant: "acme", NodeID: "orders", Workload: spiffeid.RequireFromString("spiffe://test/orders")}
	access := model.ServiceAccess{Tenant: "acme", Service: "payments", Source: model.Principal{ID: consumer.Workload}, L7: &model.HTTPMatch{Methods: []string{"GET", "POST"}, PathPrefix: "/pay/"}}
	put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.ServiceAccessKind, Name: "orders"}, map[string]any{"spec": access})
	capture := func(recipient model.Session) model.Plan {
		v, err := s.View(ctx, catalog.Ranges("acme"))
		if err != nil {
			t.Fatal(err)
		}
		p, err := compilePlan(v, recipient, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	exact := capture(publisher)
	exactOut := capture(consumer)
	for name, id := range map[string]spiffeid.ID{"callers": consumer.Workload, "owners": publisher.Workload} {
		put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.WorkloadGroupKind, Name: name}, map[string]any{"spec": model.WorkloadGroup{Tenant: "acme", Name: name, Members: []spiffeid.ID{id}}})
	}
	access.Source = model.Principal{Group: "callers"}
	put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.ServiceAccessKind, Name: "orders"}, map[string]any{"spec": access})
	put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.BindGrantKind, Name: "payments"}, map[string]any{"spec": model.BindGrant{Tenant: "acme", Service: "payments", Workload: model.Principal{Group: "owners"}}})
	grouped := capture(publisher)
	groupedOut := capture(consumer)
	if len(groupedOut.Outbound) != 1 || len(groupedOut.Outbound[0].Endpoints) != 1 {
		t.Fatal("group rules did not project owners/endpoints")
	}
	if !reflect.DeepEqual(exact.Inbound, grouped.Inbound) || !reflect.DeepEqual(exactOut.Outbound, groupedOut.Outbound) {
		t.Fatalf("group projection differs: %+v %+v", exact, grouped)
	}
	// Group removal must remove both bind admission and inbound permission.
	if err := s.DeleteObject(ctx, store.ObjectKey{Tenant: "acme", Kind: model.WorkloadGroupKind, Name: "owners"}); err != nil {
		t.Fatal(err)
	}
	if len(capture(publisher).Inbound) != 0 {
		t.Fatal("missing group retained bind permission")
	}
	var initial model.ServiceAccess
	if err := json.Unmarshal([]byte(`{"tenant":"acme","service":"payments","source":"spiffe://test/orders"}`), &initial); err != nil || initial.Source.ID != consumer.Workload {
		t.Fatalf("initial principal: %+v %v", initial, err)
	}
}
