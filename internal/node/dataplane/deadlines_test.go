package dataplane

import (
	"context"
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestExecutionDeadlinesPreserveSignedInputAndUnrenewedLeases(t *testing.T) {
	client := spiffeid.RequireFromString("spiffe://test/client")
	server := spiffeid.RequireFromString("spiffe://test/server")
	p, inbound := runtimePlans(client, server)
	p.NotAfter = p.NotAfter.UTC() // Wire times never carry a monotonic component.
	p.Outbound = []model.OutboundService{{Service: "payments", Port: inbound.Inbound[0].Port, Endpoints: []model.Endpoint{{Tenant: "default", Service: "payments", Port: "api", InstanceID: "one", Owner: server, Address: netip.MustParseAddrPort("127.0.0.1:8443"), Ready: true, Serving: true, Generation: 1, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()}}}}
	p.Hash = p.ContentHash()
	before, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	r := New(nil, time.Second)
	applyPlan := func(p *model.Plan) {
		t.Helper()
		change, err := r.Prepare(context.Background(), &snapshot.Snapshot{MeshPlan: p})
		if err != nil {
			t.Fatal(err)
		}
		if err := change.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	applyPlan(&p)
	first := r.CurrentPlan()
	lease := first.Outbound[0].Endpoints[0].LeaseExpiresAt
	if first.NotAfter == first.NotAfter.Round(0) || lease == lease.Round(0) {
		t.Fatal("wire deadlines were not anchored to the monotonic clock")
	}
	after, _ := json.Marshal(p)
	if string(before) != string(after) || !first.NotAfter.Equal(p.NotAfter) || first.ContentHash() != p.Hash {
		t.Fatal("anchoring changed the signed input or content hash")
	}
	applyPlan(&p)
	if r.CurrentPlan() != first {
		t.Fatal("same input acquired new local deadlines")
	}
	renewed := p
	renewed.InputRevision++
	renewed.NotAfter = renewed.NotAfter.Add(time.Minute)
	applyPlan(&renewed)
	if got := r.CurrentPlan(); !got.NotAfter.After(first.NotAfter) || got.Outbound[0].Endpoints[0].LeaseExpiresAt != lease {
		t.Fatal("plan renewal extended an unrenewed endpoint lease")
	}
	// An unchanged signed expiry retains its original local anchor even if a
	// later input arrives after a backwards clock adjustment.
	if got := localDeadline(p.NotAfter, first.NotAfter, time.Now().Add(-time.Hour)); got != first.NotAfter {
		t.Fatal("unchanged expiry was reanchored after clock rollback")
	}
	expired := p
	expired.NotAfter = time.Now().Add(-time.Second).UTC()
	applyPlan(&expired)
	if Fresh(r.CurrentPlan()) {
		t.Fatal("an expired signed plan gained validity during anchoring")
	}
}
