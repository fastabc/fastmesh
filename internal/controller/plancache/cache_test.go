package plancache

import (
	"bytes"
	"context"
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/index"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestHeartbeatReusesTopologyAndRenewalNeverInventsLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := memory.New()
	owner := spiffeid.RequireFromString("spiffe://test/owner")
	now := time.Now().UTC()
	e := model.Endpoint{Tenant: "default", Service: "api", Port: "tcp", InstanceID: "one", Owner: owner, Address: netip.MustParseAddrPort("127.0.0.1:80"), Generation: 1, Ready: true, Serving: true, LeaseExpiresAt: now.Add(time.Minute)}
	put := func(kind, name string, data any) store.Object {
		t.Helper()
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		obj, err := s.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "default", Kind: kind, Name: name}, Data: raw})
		if err != nil {
			t.Fatal(err)
		}
		return obj
	}
	put(model.ServiceKind, "api", model.ServiceRecord{Spec: model.Service{Tenant: "default", Name: "api", Ports: []model.ServicePort{{Name: "tcp", Number: 80, Protocol: "tcp"}}}})
	put(model.BindGrantKind, "grant", map[string]any{"spec": model.BindGrant{Tenant: "default", Service: "api", Workload: model.Principal{ID: owner}}})
	put(model.ServiceAccessKind, "access", map[string]any{"spec": model.ServiceAccess{Tenant: "default", Service: "api", Source: model.Principal{ID: owner}}})
	endpoint := put(model.EndpointKind, model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID).Name, model.EndpointRecord{Endpoint: e})
	idx := index.New(s, s)
	done := make(chan error, 1)
	go func() { done <- idx.Run(ctx) }()
	defer func() { cancel(); <-done }()
	snap, err := idx.Snapshot(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	var cache Cache
	compiles := 0
	record := func(time.Duration, error) { compiles++ }
	recipient := model.Session{Tenant: "default", NodeID: "node", Workload: owner}
	initial, plan, err := cache.Resource(context.Background(), snap, recipient, model.PlanTypeURL, now, time.Minute, record)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Header   model.PlanHeader
		Topology json.RawMessage
	}
	if err := json.Unmarshal(initial.Resource.Value, &body); err != nil {
		t.Fatal(err)
	}
	for n := range 10 {
		resource, p, err := cache.Resource(context.Background(), snap, recipient, model.PlanTypeURL, now.Add(time.Duration(n+1)*time.Second), time.Minute, record)
		if err != nil {
			t.Fatal(err)
		}
		var renewed struct {
			Header   model.PlanHeader
			Topology json.RawMessage
		}
		if err := json.Unmarshal(resource.Resource.Value, &renewed); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body.Topology, renewed.Topology) || !renewed.Header.NotAfter.After(body.Header.NotAfter) || p.Hash != plan.Hash || !p.Outbound[0].Endpoints[0].LeaseExpiresAt.Equal(e.LeaseExpiresAt) {
			t.Fatal("heartbeat changed topology or extended unrenewed lease")
		}
	}
	if compiles != 1 {
		t.Fatalf("heartbeat compiled %d times", compiles)
	}
	// Actual lease renewal has a new revision and must refresh signed endpoint
	// deadlines, even though its authorization/topology hash is unchanged.
	e.LeaseExpiresAt = e.LeaseExpiresAt.Add(time.Minute)
	endpoint.Data, _ = json.Marshal(model.EndpointRecord{Endpoint: e})
	saved, err := s.PutObject(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	for {
		snap, err = idx.Snapshot(ctx, "default")
		if err != nil {
			t.Fatal(err)
		}
		if snap.Revision >= saved.ResourceVersion {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	_, updated, err := cache.Resource(context.Background(), snap, recipient, model.PlanTypeURL, now, time.Minute, record)
	if err != nil {
		t.Fatal(err)
	}
	if compiles != 2 || updated.Hash != plan.Hash || !updated.Outbound[0].Endpoints[0].LeaseExpiresAt.Equal(e.LeaseExpiresAt) || !plan.Outbound[0].Endpoints[0].LeaseExpiresAt.Equal(e.LeaseExpiresAt.Add(-time.Minute)) {
		t.Fatal("renewal stale or cached plan mutated")
	}
}
