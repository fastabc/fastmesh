package apply

import (
	"context"

	"github.com/fastabc/fastmesh/policy/snapshot"
)

// Projection materializes a verified runtime snapshot into live data-plane
// resources. Prepare must stage fallible work without changing live traffic;
// Commit atomically publishes the new state and starts draining old resources;
// Rollback must release anything staged by Prepare.
type Projection interface {
	Prepare(ctx context.Context, snap *snapshot.Snapshot) (PreparedChange, error)
	Addrs() []string
	Shutdown(ctx context.Context) error
}
