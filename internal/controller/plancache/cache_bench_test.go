package plancache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/index"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

// Each miss operation is a batch of eight requests against a fresh cache.
// Catalog decoding is warmed separately; signatures, I/O and store updates are excluded.
func BenchmarkCache(b *testing.B) {
	f, snapshots := newCacheFixture(b, 8, 100)
	for _, s := range snapshots {
		if _, err := s.Catalog(); err != nil {
			b.Fatal(err)
		}
	}
	for _, mode := range []string{"Hit", "MissSameRecipient", "MissSameTenant", "MissManyTenants"} {
		b.Run(mode, func(b *testing.B) {
			var cache Cache
			var compiles atomic.Int64
			record := func(time.Duration, error) { compiles.Add(1) }
			call := func(c *Cache, worker int) {
				s := snapshots[0]
				if mode == "MissManyTenants" {
					s = snapshots[worker]
				}
				id := "node"
				if mode == "MissSameTenant" {
					id = fmt.Sprintf("node-%d", worker)
				}
				if _, _, err := c.Resource(context.Background(), s, model.Session{Tenant: s.Tenant, NodeID: id, Workload: f.owner}, model.PlanTypeURL, f.now, time.Minute, record); err != nil {
					b.Error(err)
				}
			}
			call(&cache, 0)
			compiles.Store(0)
			b.ReportAllocs()
			b.ResetTimer()
			if mode == "Hit" {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						call(&cache, 0)
					}
				})
			} else {
				for range b.N {
					var batch Cache
					var wg sync.WaitGroup
					for worker := range 8 {
						wg.Go(func() { call(&batch, worker) })
					}
					wg.Wait()
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(compiles.Load())/float64(b.N), "compiles/op")
		})
	}
	// Capture a real lease-only revision, then a real grant revocation.
	e := f.endpoint(snapshots[0].Tenant, "service-0")
	e.LeaseExpiresAt = e.LeaseExpiresAt.Add(time.Minute)
	revision := f.put(e.Tenant, model.EndpointKind, model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID).Name, model.EndpointRecord{Endpoint: e})
	lease := f.capture(e.Tenant, revision)
	if err := f.objects.DeleteObject(context.Background(), store.ObjectKey{Tenant: e.Tenant, Kind: model.BindGrantKind, Name: e.Service}); err != nil {
		b.Fatal(err)
	}
	topology := f.capture(e.Tenant, revision+1)
	for name, next := range map[string]*index.Snapshot{"LeaseRevision": lease, "RevocationRevision": topology} {
		b.Run(name, func(b *testing.B) {
			if _, err := next.Catalog(); err != nil {
				b.Fatal(err)
			}
			recipient := model.Session{Tenant: next.Tenant, NodeID: "node", Workload: f.owner}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var cache Cache
				for _, s := range []*index.Snapshot{snapshots[0], next} {
					if _, _, err := cache.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
