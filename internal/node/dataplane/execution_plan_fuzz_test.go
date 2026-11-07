package dataplane

import (
	"testing"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/protocol/planwire"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/protobuf/types/known/anypb"
)

func FuzzExecutablePlanFiltersDecodedPlans(f *testing.F) {
	_, seed := runtimePlans(spiffeid.RequireFromString("spiffe://test/client"), spiffeid.RequireFromString("spiffe://test/server"))
	seed.Hash = seed.ContentHash()
	v1, err := planwire.PackPlan(seed)
	if err != nil {
		f.Fatal(err)
	}
	for _, body := range [][]byte{v1.Resource.Value, nil, []byte("{")} {
		f.Add(body)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if p, err := planwire.DecodePlanBody(body); err == nil && !executablePlan(&p).Valid() {
			t.Fatal("invalid entry survived execution filtering")
		}
		r := &discoveryv1.Resource{Name: v1.Name, Version: v1.Version, Resource: &anypb.Any{TypeUrl: model.PlanTypeURL, Value: body}}
		if p, err := planwire.DecodePlan(r); err == nil && !executablePlan(&p).Valid() {
			t.Fatal("invalid V1 entry survived execution filtering")
		}
	})
}
