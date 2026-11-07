package plancache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/index"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

type cacheFixture struct {
	t       testing.TB
	objects *memory.Store
	index   *index.Index
	now     time.Time
	owner   spiffeid.ID
}

func newCacheFixture(t testing.TB, tenants, services int) (*cacheFixture, []*index.Snapshot) {
	t.Helper()
	f := &cacheFixture{t: t, objects: memory.New(), now: time.Now().UTC(), owner: spiffeid.RequireFromString("spiffe://test/owner")}
	for n := range tenants {

		tenant := fmt.Sprintf("tenant-%d", n)
		for i := range services {
			name := fmt.Sprintf("service-%d", i)
			f.put(tenant, model.ServiceKind, name, model.ServiceRecord{Spec: model.Service{Tenant: tenant, Name: name, Ports: []model.ServicePort{{Name: "api", Protocol: "tcp"}}}})
			f.put(tenant, model.BindGrantKind, name, map[string]any{"spec": model.BindGrant{Tenant: tenant, Service: name, Workload: model.Principal{ID: f.owner}}})
			f.put(tenant, model.ServiceAccessKind, name, map[string]any{"spec": model.ServiceAccess{Tenant: tenant, Service: name, Source: model.Principal{ID: f.owner}}})
			e := f.endpoint(tenant, name)
			f.put(tenant, model.EndpointKind, model.EndpointKey(tenant, name, f.owner, "one").Name, model.EndpointRecord{Endpoint: e})
		}
	}
	f.index = index.New(f.objects, f.objects)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.index.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	snapshots := make([]*index.Snapshot, tenants)
	for n := range snapshots {
		snapshots[n] = f.capture(fmt.Sprintf("tenant-%d", n), 0)
	}
	return f, snapshots
}

func (f *cacheFixture) endpoint(tenant, service string) model.Endpoint {
	return model.Endpoint{Tenant: tenant, Service: service, Port: "api", InstanceID: "one", Owner: f.owner,
		Address: netip.MustParseAddrPort("127.0.0.1:8443"), Generation: 1, Ready: true, Serving: true, LeaseExpiresAt: f.now.Add(time.Hour)}
}
func (f *cacheFixture) put(tenant, kind, name string, value any) int64 {
	f.t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		f.t.Fatal(err)
	}
	saved, err := f.objects.PutObject(context.Background(), store.Object{Key: store.ObjectKey{Tenant: tenant, Kind: kind, Name: name}, Data: raw})
	if err != nil {
		f.t.Fatal(err)
	}
	return saved.ResourceVersion
}
func (f *cacheFixture) capture(tenant string, revision int64) *index.Snapshot {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		s, err := f.index.Snapshot(ctx, tenant)
		if err != nil {
			f.t.Fatal(err)
		}
		if s.Revision >= revision {
			return s
		}
		select {
		case <-ctx.Done():
			f.t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
