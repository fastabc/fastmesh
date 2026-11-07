package verify

import (
	"context"
	"testing"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

type testIdentitySource struct{}

func (testIdentitySource) GetSVID(context.Context) (spiffe.SVID, error) { return spiffe.SVID{}, nil }

func TestSourceRequiresTrustAndRecipient(t *testing.T) {
	snap := signed(&snapshot.Snapshot{Tenant: "default", NodeID: "node-1"})
	for _, nodeID := range []string{"node-1", "other"} {
		if err := FromSource(testIdentitySource{}, []string{"spiffe://example.org/controller"}, nodeID, "default")(context.Background(), snap, snapshot.Signature{}); err == nil {
			t.Fatal("accepted unsigned input without trust")
		}
	}
}
