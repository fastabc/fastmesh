package plancache

import (
	"encoding/json"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/protobuf/types/known/anypb"
)

// resource refreshes validity without rebuilding the cached topology.
func resource(p projection, typeURL string, now time.Time, validity time.Duration) (*discoveryv1.Resource, model.Plan, error) {
	plan := p.plan
	plan.NotAfter = now.Add(validity).UTC()
	if typeURL != model.PlanTypeURL {
		return nil, plan, model.ErrInvalid
	}
	header, marshalErr := json.Marshal(plan.Wire().Header)
	if marshalErr != nil {
		return nil, plan, marshalErr
	}
	body := make([]byte, 0, len(header)+len(p.topology)+24)
	body = append(body, `{"header":`...)
	body = append(body, header...)
	body = append(body, `,"topology":`...)
	body = append(body, p.topology...)
	body = append(body, '}')
	return &discoveryv1.Resource{Name: plan.NodeID, Version: plan.Hash, Resource: &anypb.Any{TypeUrl: typeURL, Value: body}}, plan, nil
}
