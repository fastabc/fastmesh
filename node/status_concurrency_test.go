package node

import (
	"sync"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestStatusConcurrentWithPlanPublication(t *testing.T) {
	id := spiffeid.RequireFromString("spiffe://test/client")
	_, p := runtimePlans(id, spiffeid.RequireFromString("spiffe://test/server"))
	n := &Node{runtime: dataplane.New(nil, time.Second)}
	installPlan(t, n.runtime, p)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			_ = n.Status()
		}
	})
	for range 100 {
		p.InputRevision++
		installPlan(t, n.runtime, p)
	}
	wg.Wait()
	if got := n.Status(); got.InputRevision != p.InputRevision {
		t.Fatalf("revision=%d want=%d", got.InputRevision, p.InputRevision)
	}
}
