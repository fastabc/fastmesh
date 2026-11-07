package index

import (
	"context"
	"testing"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestGroupMembershipChangesInvalidateBindIndexWithoutMutatingOldSnapshot(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	owner := spiffeid.RequireFromString("spiffe://test/owner")
	group := object(t, model.WorkloadGroupKind, "owners", model.WorkloadGroup{Tenant: "default", Name: "owners", Members: []spiffeid.ID{owner}})
	grant := object(t, model.BindGrantKind, "grant", model.BindGrant{Tenant: "default", Service: "api", Workload: model.Principal{Group: "owners"}})
	for _, o := range []store.Object{group, grant} {
		if _, err := s.PutObject(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	v, err := s.View(ctx, catalog.Ranges(""))
	if err != nil {
		t.Fatal(err)
	}
	i := New(s, s)
	i.reset(v)
	old, err := i.Snapshot(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if !old.CanBind("api", owner.String()) {
		t.Fatal("group member cannot bind")
	}
	group = object(t, model.WorkloadGroupKind, "owners", model.WorkloadGroup{Tenant: "default", Name: "owners"})
	i.apply(store.RevisionBatch{Revision: v.Revision + 1, Events: []store.Event{{Object: group}}})
	next, err := i.Snapshot(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if next.CanBind("api", owner.String()) {
		t.Fatal("removed member can still bind")
	}
	if !old.CanBind("api", owner.String()) {
		t.Fatal("old immutable snapshot was mutated")
	}
}
