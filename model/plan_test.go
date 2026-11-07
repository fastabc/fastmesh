package model

import (
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestPlanLoadedEmptyAndDeadlineHash(t *testing.T) {
	p := Plan{Tenant: "default", NodeID: "orders", Workload: spiffeid.RequireFromString("spiffe://test/orders"), InputRevision: 1, NotAfter: time.Now().Add(time.Minute), AccessSet: true, EndpointsSet: true}
	if !p.Valid() {
		t.Fatal("loaded empty plan invalid")
	}
	hash := p.ContentHash()
	p.NotAfter = p.NotAfter.Add(time.Minute)
	p.InputRevision++
	if p.ContentHash() != hash {
		t.Fatal("deadline-only change rebuilt content")
	}
	p.AccessSet = false
	if p.Valid() {
		t.Fatal("unloaded access accepted")
	}
	p.AccessSet = true
	p.EndpointsSet = false
	if p.Valid() {
		t.Fatal("unloaded endpoints accepted")
	}
}
