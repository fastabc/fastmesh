package planwire

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/protobuf/types/known/anypb"
)

func fuzzPlan() model.Plan {
	id := spiffeid.RequireFromString("spiffe://test/orders")
	p := model.Plan{TypeURL: model.PlanTypeURL, Tenant: "default", NodeID: "orders", Workload: id, InputRevision: 1,
		NotAfter: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), AccessSet: true, EndpointsSet: true,
		Inbound: []model.InboundService{{Service: "orders", Port: model.ServicePort{Name: "http", Protocol: "http"},
			RequiresL7: true, Allow: []model.InboundAllow{{Source: id, L7: &model.HTTPMatch{Method: "GET", Path: "/ok"}}}}}}
	p.Hash = p.ContentHash()
	return p
}

func FuzzDecodePlan(f *testing.F) {
	seed, err := PackPlan(fuzzPlan())
	if err != nil {
		f.Fatal(err)
	}
	valid := string(seed.Resource.Value)
	for _, body := range [][]byte{
		seed.Resource.Value, nil, []byte("null"), []byte("{"),
		bytes.ReplaceAll([]byte(valid), []byte(`"GET"`), []byte(`"BAD METHOD"`)),
		bytes.ReplaceAll([]byte(valid), []byte(`"http"`), []byte(`"future-protocol"`)),
		bytes.ReplaceAll([]byte(valid), []byte(`"default"`), []byte(`"bad tenant"`)),
	} {
		f.Add(body)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		// Exercise the post-authentication decoder independently. Production
		// callers must verify the raw body before reaching this entry point.
		before := bytes.Clone(body)
		p, err := DecodePlanBody(body)
		if err == nil {
			if !p.InputValid() || !p.IsCurrent() {
				t.Fatal("accepted invalid header")
			}
			resource := &discoveryv1.Resource{Name: p.NodeID + "-wrong", Version: p.Hash, Resource: &anypb.Any{TypeUrl: model.PlanTypeURL, Value: body}}
			if _, err := DecodePlan(resource); err == nil {
				t.Fatal("envelope name did not bind recipient")
			}
			resource.Name, resource.Version = p.NodeID, p.Hash+"-wrong"
			if _, err := DecodePlan(resource); err == nil {
				t.Fatal("envelope version did not bind topology")
			}
		}
		if !bytes.Equal(body, before) {
			t.Fatal("decoder mutated authenticated input")
		}
	})
}

func FuzzPlanUnknownPermissions(f *testing.F) {
	f.Add("condition", "required", false)
	f.Add("claim", "admin", true)
	f.Fuzz(func(t *testing.T, field, value string, inL7 bool) {
		p := fuzzPlan()
		good := p.Inbound[0]
		good.Service = "unaffected"
		p.Inbound = append(p.Inbound, good)
		body, err := json.Marshal(p.Wire())
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]any
		if err := json.Unmarshal(body, &wire); err != nil {
			t.Fatal(err)
		}
		in := wire["topology"].(map[string]any)["inbound"].([]any)[0].(map[string]any)
		rule := in["allow"].([]any)[0].(map[string]any)
		if inL7 {
			rule = rule["l7"].(map[string]any)
		}
		rule["future_"+field] = value // Always an unknown execution requirement.
		body, err = json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodePlanBody(body)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded.Inbound) != 2 || len(decoded.Inbound[0].Allow) != 0 || len(decoded.Inbound[1].Allow) != 1 {
			t.Fatal("unknown permission broadened authority or removed valid sibling")
		}
	})
}
