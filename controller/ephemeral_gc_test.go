package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestEphemeralGCAndNodeEndpointOwnership(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	c, err := New(Config{ID: "gc", Objects: s, Leases: s, DevInsecure: true, EphemeralTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.elector.TryAcquire(ctx); err != nil {
		t.Fatal(err)
	}
	owner := spiffeid.RequireFromString("spiffe://test/shared")
	put := func(key store.ObjectKey, v any) {
		t.Helper()
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.PutObject(ctx, store.Object{Key: key, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []model.Node{
		{ID: "old", Tenant: "default", Ephemeral: true, Status: model.NodeStatus{LastContact: time.Now().Add(-2 * time.Second)}},
		{ID: "live", Tenant: "default", Ephemeral: true, Status: model.NodeStatus{LastContact: time.Now()}},
		{ID: "durable", Tenant: "default", Status: model.NodeStatus{LastContact: time.Now().Add(-2 * time.Second)}},
	} {
		put(model.NodeKey(n.ID), n)
		put(model.EndpointKey(n.Tenant, "api", owner, n.ID), model.EndpointRecord{NodeID: n.ID, Endpoint: model.Endpoint{Owner: owner, InstanceID: n.ID}})
	}
	objects, err := s.ListObjects(ctx, model.SystemTenant, model.NodeKind)
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range objects {
		if err := c.expireNode(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.GetObject(ctx, model.NodeKey("old")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired node=%v", err)
	}
	if _, err := s.GetObject(ctx, model.EndpointKey("default", "api", owner, "old")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired endpoint=%v", err)
	}
	if err := c.RevokeNode(ctx, "live"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject(ctx, model.EndpointKey("default", "api", owner, "live")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("revoked endpoint=%v", err)
	}
	if _, err := s.GetObject(ctx, model.EndpointKey("default", "api", owner, "durable")); err != nil {
		t.Fatalf("removed other node of same workload: %v", err)
	}
	if _, err := s.GetObject(ctx, model.NodeKey("durable")); err != nil {
		t.Fatalf("removed durable node: %v", err)
	}
}

func TestFollowerContactSupersedesScheduledEphemeralExpiry(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	cfg := Config{ID: "leader", Objects: s, Leases: s, DevInsecure: true, EphemeralTTL: time.Second}
	leader, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.elector.TryAcquire(ctx); err != nil {
		t.Fatal(err)
	}
	cfg.ID = "follower"
	follower, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n := model.Node{ID: "ephemeral", Tenant: "default", Intent: "active", Ephemeral: true, Status: model.NodeStatus{LastContact: time.Now().Add(-2 * time.Second)}}
	data, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.PutObject(ctx, store.Object{Key: model.NodeKey(n.ID), Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := follower.admission.AuthorizeReadSession(ctx, n.ID, n.Tenant); err != nil {
		t.Fatal(err)
	}
	if err := leader.expireNode(ctx, old); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("old expiry did not lose CAS to contact: %v", err)
	}
	current, err := s.GetObject(ctx, old.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(current.Data, &n); err != nil {
		t.Fatal(err)
	}
	if time.Since(n.Status.LastContact) > time.Second || n.Intent != "active" {
		t.Fatal("follower contact did not preserve live enrollment")
	}
}
