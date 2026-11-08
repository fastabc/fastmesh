package dataplane

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestExecutionIndexMatchesLinearAuthorization(t *testing.T) {
	peer := spiffeid.RequireFromString("spiffe://test/allowed")
	other := spiffeid.RequireFromString("spiffe://test/other")
	for _, count := range []int{1, 15, 16, 100} {
		p := &model.Plan{}
		r := New(nil, time.Second)
		for i := range count {
			p.Inbound = append(p.Inbound, model.InboundService{Service: fmt.Sprint(i), Port: model.ServicePort{Name: "api"}, RequiresL7: i%3 == 0, Allow: []model.InboundAllow{{Source: peer, L7: &model.HTTPMatch{Method: "GET", Path: "/ok"}}}})
		}
		r.plan.Store(p)
		for _, id := range []spiffeid.ID{peer, other} {
			for _, mode := range []bool{false, true} {
				for _, path := range []string{"/ok", "/no"} {
					for i := range count + 1 {
						ref := ServiceRef{Service: fmt.Sprint(i), Port: "api"}
						req := &http.Request{Method: "GET", URL: &url.URL{Path: path}}
						if got, want := r.AllowedInbound(p, ref, id, mode, req), AllowedInbound(p, ref, id, mode, req); got != want {
							t.Fatalf("count=%d service=%d: %v != %v", count, i, got, want)
						}
					}
				}
			}
		}
		old := p
		denied := *p
		denied.Inbound = nil
		r.plan.Store(&denied)
		// An old captured plan, or a composed host plan, must use its own rules.
		if got, want := r.AllowedInbound(old, ServiceRef{Service: "0", Port: "api"}, peer, true, nil), AllowedInbound(old, ServiceRef{Service: "0", Port: "api"}, peer, true, nil); got != want {
			t.Fatal("mixed publications")
		}
		if r.AllowedInbound(r.CurrentPlan(), ServiceRef{Service: "0", Port: "api"}, peer, true, nil) {
			t.Fatal("stale authorization after revoke")
		}
	}
}

func TestExecutionIndexLookupAndDuplicateSemantics(t *testing.T) {
	p := &model.Plan{}
	for i := range 100 {
		p.Inbound = append(p.Inbound, model.InboundService{Service: fmt.Sprint(i), Port: model.ServicePort{Name: "api"}})
		p.Outbound = append(p.Outbound, model.OutboundService{Service: fmt.Sprint(i), Port: model.ServicePort{Name: "api"}})
	}
	// Normal publication filters duplicates before indexing. Keep the lookup
	// primitive equivalent to the linear oracle even for unfiltered fixtures.
	p.Inbound = append(p.Inbound, p.Inbound[0])
	p.Outbound = append(p.Outbound, p.Outbound[0])
	r := New(nil, time.Second)
	r.plan.Store(p)
	for i := range 100 {
		ref := ServiceRef{Service: fmt.Sprint(i), Port: "api"}
		if r.Inbound(p, ref) != &p.Inbound[i] || r.outbound(p, ref) != &p.Outbound[i] {
			t.Fatalf("wrong entry for %v", ref)
		}
	}
	if r.outbound(p, ServiceRef{Service: "absent", Port: "api"}) != nil {
		t.Fatal("unknown service matched")
	}
	r.plan.Store(nil)
	if r.outbound(p, ServiceRef{Service: "0", Port: "api"}) != &p.Outbound[0] || r.CurrentPlan() != nil {
		t.Fatal("fallback or clear failed")
	}
}

func BenchmarkExecutionIndexBuild(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			p := &model.Plan{}
			for i := range count {
				p.Inbound = append(p.Inbound, model.InboundService{Service: fmt.Sprint(i), Port: model.ServicePort{Name: "api"}})
				p.Outbound = append(p.Outbound, model.OutboundService{Service: fmt.Sprint(i), Port: model.ServicePort{Name: "api"}})
			}
			var h planHolder
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				h.Store(p)
			}
		})
	}
}
