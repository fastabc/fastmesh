package election

import "testing"

// Each controller region is a cell with its own leader lease.
// Two cells contending for one lease would elect one leader across regions,
// rather than one independent leader per region.
func TestLeaseNameIsScopedByRegion(t *testing.T) {
	if got := LeaseNameFor("cn"); got == LeaseNameFor("us") {
		t.Fatalf("cn and us share the lease %q", got)
	}
	if got := LeaseNameFor("cn"); got != DefaultLeaseName+"/cn" {
		t.Fatalf("lease=%q", got)
	}
}

// A deployment that never heard of regions keeps the lease it already holds.
// A new name here would make an upgrade look like a leaderless control plane
// to the offline tooling that reads this lease, and to the operator watching
// two processes both believe they are active during the changeover.
func TestUnregionedLeaseKeepsItsName(t *testing.T) {
	if got := LeaseNameFor(""); got != DefaultLeaseName {
		t.Fatalf("lease=%q, want the name a single-cell deployment already uses", got)
	}
	if got := LeaseNameFor("   "); got != DefaultLeaseName {
		t.Fatalf("lease=%q, want blank treated as unregioned", got)
	}
}
