package dataplane

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestLocalDeadline(t *testing.T) {
	now := time.Now()
	for _, c := range []struct{ signed, previous, want time.Time }{
		{now.Add(time.Minute), time.Time{}, now.Add(time.Minute)},
		{now.Add(time.Minute), now.Add(time.Minute), now.Add(time.Minute)},
		{now.Add(30 * time.Second), now.Add(time.Minute), now.Add(30 * time.Second)},
		{now.Add(2 * time.Minute), now.Add(time.Minute), now.Add(2 * time.Minute)},
	} {
		if got := localDeadline(c.signed, c.previous, now); !got.Equal(c.want) {
			t.Errorf("deadline=%v want=%v", got, c.want)
		}
	}
}

func TestExecutablePlanDropsDuplicateServiceEntries(t *testing.T) {
	id := spiffeid.RequireFromString("spiffe://test/client")
	_, p := runtimePlans(id, spiffeid.RequireFromString("spiffe://test/server"))
	p.Inbound = append(p.Inbound, p.Inbound[0])
	if got := executablePlan(&p); len(got.Inbound) != 0 {
		t.Fatal("duplicate service retained")
	}
	if len(p.Inbound) != 2 {
		t.Fatal("signed input changed")
	}
}

func TestAllowedInboundRequiresHTTPForL7(t *testing.T) {
	id := spiffeid.RequireFromString("spiffe://test/client")
	_, p := runtimePlans(id, spiffeid.RequireFromString("spiffe://test/server"))
	in := &p.Inbound[0]
	in.RequiresL7 = true
	in.Allow[0].L7 = &model.HTTPMatch{Method: "GET", Path: "/allowed"}
	ref := ServiceRef{Service: "payments", Port: "api"}
	if AllowedInbound(&p, ref, id, false, nil) {
		t.Fatal("L7 rule admitted raw connection")
	}
	for _, path := range []string{"/allowed", "/denied"} {
		got := AllowedInbound(&p, ref, id, true, &http.Request{Method: "GET", URL: &url.URL{Path: path}})
		if got != (path == "/allowed") {
			t.Errorf("%s allowed=%v", path, got)
		}
	}
	in.Allow = nil
	if AllowedInbound(&p, ref, id, true, nil) {
		t.Fatal("empty allow granted access")
	}
	in.Allow = []model.InboundAllow{{Source: id}}
	if !AllowedInbound(&p, ref, id, true, nil) {
		t.Fatal("authorized L4 rule lost in HTTP mode")
	}
}
