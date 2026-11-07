package plancache

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
)

func BenchmarkCacheCapacity(b *testing.B) {
	for _, services := range []int{100, 1000} {
		b.Run(fmt.Sprint(services), func(b *testing.B) {
			f, snaps := newCacheFixture(b, 1, services)
			s := snaps[0]
			if _, err := s.Catalog(); err != nil {
				b.Fatal(err)
			}
			var peak uint64
			for range b.N {
				c := &Cache{MaxEntries: 16, MaxPending: 64, Parallelism: 8}
				start := time.Now()
				var wg sync.WaitGroup
				for i := range 32 {
					wg.Go(func() {
						recipient := model.Session{Tenant: s.Tenant, NodeID: fmt.Sprint(i), Workload: f.owner}
						for range 2 {
							if _, _, err := c.Resource(context.Background(), s, recipient, model.PlanTypeURL, f.now, time.Minute, nil); err != nil {
								b.Error(err)
							}
						}
					})
				}
				wg.Wait()
				stats := c.Stats()
				if stats.Entries > 16 || stats.Pending != 0 || stats.Running != 0 || time.Since(start) > 10*time.Second {
					b.Fatalf("capacity budget: %+v", stats)
				}
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				peak = max(peak, mem.HeapAlloc)
				b.ReportMetric(float64(stats.QueueWait.Nanoseconds()), "queue-ns/batch")
				b.ReportMetric(float64(stats.Rejected), "rejected/batch")
				b.ReportMetric(float64(stats.Hits), "hits/batch")
			}
			b.ReportMetric(float64(peak), "sampled-heap-bytes")
		})
	}
}
