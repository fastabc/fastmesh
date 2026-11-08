package plancache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
)

func TestCatalogPlanBodyBudget(t *testing.T) {
	for _, count := range []int{1000, 10000} {
		f, snaps := newCacheFixture(t, 1, count)
		var c Cache
		resource, _, err := c.Resource(context.Background(), snaps[0], model.Session{Tenant: snaps[0].Tenant, NodeID: "node", Workload: f.owner}, model.PlanTypeURL, f.now, time.Minute, nil)
		if count == 1000 {
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%d services: %d body bytes", count, len(resource.Resource.Value))
		} else {
			if !errors.Is(err, model.ErrPlanTooLarge) {
				t.Fatalf("large directory: %v", err)
			}
			if c.Stats().Entries != 0 {
				t.Fatal("retained oversized plan")
			}
		}
	}
}
