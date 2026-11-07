package catalog_test

import (
	"context"
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func put(t *testing.T, s store.ObjectStore, key store.ObjectKey, value any) store.Object {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := s.PutObject(ctx, store.Object{Key: key, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func seed(t *testing.T, s store.ObjectStore) (model.Session, endpoint.Registration) {
	t.Helper()

	session := model.Session{Tenant: "acme", NodeID: "node-payments", Workload: spiffeid.RequireFromString("spiffe://test/payments")}
	put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.ServiceKind, Name: "payments"}, model.ServiceRecord{Spec: model.Service{Tenant: "acme", Name: "payments", Ports: []model.ServicePort{{Name: "api", Number: 8080, Protocol: "tcp"}}}})
	put(t, s, store.ObjectKey{Tenant: "acme", Kind: model.BindGrantKind, Name: "payments"}, map[string]any{"spec": model.BindGrant{Tenant: "acme", Service: "payments", Workload: model.Principal{ID: session.Workload}}})
	return session, endpoint.Registration{Service: "payments", Port: "api", InstanceID: "instance-1", Generation: 1, Address: netip.MustParseAddrPort("127.0.0.1:10001"), Ready: true}
}

// compilePlan consumes only one previously captured coherent View. It never
// performs another store read, including for endpoint eligibility or grants.
func compilePlan(view store.View, recipient model.Session, now time.Time, validity time.Duration) (model.Plan, error) {
	c, err := catalog.Decode(view, recipient.Tenant)
	if err != nil {
		return model.Plan{}, err
	}
	return catalog.Compile(c, view.Revision, recipient, now, validity)
}

func readCatalog(viewer store.Viewer, ctx context.Context, tenant string) (catalog.Catalog, error) {
	v, err := viewer.View(ctx, catalog.Ranges(tenant))
	if err != nil {
		return catalog.Catalog{}, err
	}
	return catalog.Decode(v, tenant)
}

var ctx = context.Background()
