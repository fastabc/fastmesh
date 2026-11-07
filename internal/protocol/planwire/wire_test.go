package planwire

import (
	"encoding/json"
	"testing"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/protobuf/proto"
)

func TestPlanRoundTripAndEnvelopeBinding(t *testing.T) {
	p := fuzzPlan()
	resource, err := PackPlan(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePlan(resource)
	if err != nil || got.Hash != p.Hash || got.NodeID != p.NodeID || !got.IsCurrent() || len(got.Inbound) != 1 {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for _, field := range []string{"name", "version", "type"} {
		t.Run(field, func(t *testing.T) {
			changed := proto.Clone(resource).(*discoveryv1.Resource)
			switch field {
			case "name":
				changed.Name += "wrong"
			case "version":
				changed.Version += "wrong"
			case "type":
				changed.Resource.TypeUrl = "fastmesh.plan.v2"
			}
			if _, err := DecodePlan(changed); err == nil {
				t.Fatal("accepted mismatched envelope")
			}
		})
	}
}

func TestPlanRejectsUnversionedBody(t *testing.T) {
	p := fuzzPlan()
	// A flat execution model is never a supported wire body.
	flat, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{flat, []byte("null"), []byte(`{}`), []byte(`{"header":null,"topology":{}}`)} {
		if _, err := DecodePlanBody(raw); err == nil {
			t.Fatalf("accepted body: %s", raw)
		}
	}
	if model.PlanTypeURL != "fastmesh.plan.v1" || model.PlanCapability != "mesh.plan.v1" {
		t.Fatal("unexpected protocol identity")
	}
}
