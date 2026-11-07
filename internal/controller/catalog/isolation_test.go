package catalog_test

import (
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestCompileIsolatesUnknownFieldsAndInvalidObjects(t *testing.T) {
	for _, tc := range []struct{ name, kind, data string }{
		{"unknown-service-field", model.ServiceKind, `{"spec":{"tenant":"acme","name":"payments","future":true,"ports":[{"name":"api","number":8080,"protocol":"tcp"}]}}`},
		{"unknown-bind-field", model.BindGrantKind, `{"spec":{"tenant":"acme","service":"payments","workload":"spiffe://test/payments","future":true}}`},
		{"unknown-access-field", model.ServiceAccessKind, `{"spec":{"tenant":"acme","service":"payments","source":"spiffe://test/orders","future":true}}`},
		{"invalid-service", model.ServiceKind, `{"spec":{"tenant":"acme","name":"bad","ports":[{"name":"api","number":8080,"protocol":"future"}]}}`},
		{"malformed-service", model.ServiceKind, `{"spec":`},
		{"invalid-bind", model.BindGrantKind, `{"spec":{"tenant":"acme","service":"*","workload":"spiffe://test/payments"}}`},
		{"invalid-access", model.ServiceAccessKind, `{"spec":{"tenant":"acme","service":"*","source":"spiffe://test/orders"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := memory.New()
			publisher, _ := seed(t, s)
			consumer := model.Session{Tenant: "acme", NodeID: "node-orders", Workload: spiffeid.RequireFromString("spiffe://test/orders")}
			put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.ServiceAccessKind, Name: "allow"}, map[string]any{"spec": model.ServiceAccess{Tenant: "acme", Service: "payments", Source: model.Principal{ID: consumer.Workload}}})
			name := "bad"
			if tc.name == "unknown-service-field" || tc.name == "unknown-bind-field" {
				name = "payments"
			}
			if tc.name == "unknown-access-field" {
				name = "allow"
			}
			if _, err := s.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "acme", Kind: tc.kind, Name: name}, Data: []byte(tc.data)}); err != nil {
				t.Fatal(err)
			}
			v, err := s.View(ctx, catalog.Ranges("acme"))
			if err != nil {
				t.Fatal(err)
			}
			for _, recipient := range []model.Session{consumer, publisher} {
				p, err := compilePlan(v, recipient, time.Now(), time.Minute)
				if err != nil {
					t.Fatalf("unrelated object stopped tenant compilation: %v", err)
				}
				if len(p.Inbound)+len(p.Outbound) != 1 {
					t.Fatalf("valid service missing or invalid service included: %+v", p)
				}
				if len(p.Inbound) == 1 && len(p.Inbound[0].Allow) != 1 {
					t.Fatalf("invalid policy affected valid service: %+v", p.Inbound[0])
				}
			}
		})
	}
}

func TestCompileIgnoresInlineEndpointStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(*model.ServiceRecord)
	}{
		{"generation", func(r *model.ServiceRecord) { r.Status.Endpoints[0].Generation = 0 }},
		{"address", func(r *model.ServiceRecord) { r.Status.Endpoints[0].Address = netip.MustParseAddrPort("0.0.0.0:8080") }},
		{"duplicate", func(r *model.ServiceRecord) { r.Status.Endpoints = append(r.Status.Endpoints, r.Status.Endpoints[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := memory.New()
			publisher, _ := seed(t, s)
			consumer := model.Session{Tenant: "acme", NodeID: "node-orders", Workload: spiffeid.RequireFromString("spiffe://test/orders")}
			for _, service := range []string{"payments", "bad"} {
				put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.ServiceAccessKind, Name: service}, map[string]any{"spec": model.ServiceAccess{Tenant: "acme", Service: service, Source: model.Principal{ID: consumer.Workload}}})
			}
			put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.BindGrantKind, Name: "bad"}, map[string]any{"spec": model.BindGrant{Tenant: "acme", Service: "bad", Workload: model.Principal{ID: publisher.Workload}}})
			record := model.ServiceRecord{
				Spec:   model.Service{Tenant: "acme", Name: "bad", Ports: []model.ServicePort{{Name: "api", Number: 8080, Protocol: "tcp"}}},
				Status: model.ServiceStatus{Endpoints: []model.Endpoint{{Tenant: "acme", Service: "bad", Port: "api", InstanceID: "instance", Owner: publisher.Workload, Address: netip.MustParseAddrPort("127.0.0.1:8080"), Ready: true, Serving: true, LeaseExpiresAt: time.Now().Add(time.Minute), Generation: 1}}},
			}
			tc.corrupt(&record)
			key := store.ObjectKey{Tenant: "acme", Kind: model.ServiceKind, Name: "bad"}
			written := put(t, s, key, record)
			v, err := s.View(ctx, catalog.Ranges("acme"))
			if err != nil {
				t.Fatal(err)
			}
			p, err := compilePlan(v, consumer, time.Now(), time.Minute)
			if err != nil {
				t.Fatalf("invalid endpoint stopped tenant compilation: %v", err)
			}
			if len(p.Outbound) != 2 || len(p.Outbound[0].Endpoints) != 0 || len(p.Outbound[1].Endpoints) != 0 {
				t.Fatalf("invalid service was not isolated: %+v", p.Outbound)
			}
			current, err := s.GetObject(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if current.ResourceVersion != written.ResourceVersion {
				t.Fatal("compilation mutated invalid stored status")
			}
		})
	}
}
