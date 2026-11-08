package planwire

import (
	"encoding/json"
	"errors"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/protobuf/types/known/anypb"
)

func PackPlan(p model.Plan) (*discoveryv1.Resource, error) {
	p.TypeURL = model.PlanTypeURL
	if !p.Valid() || p.Hash == "" {
		return nil, model.ErrInvalid
	}
	body, err := json.Marshal(p.Wire())
	if err != nil {
		return nil, err
	}
	if len(body) > model.MaxPlanBodyBytes {
		return nil, model.ErrPlanTooLarge
	}
	return &discoveryv1.Resource{Name: p.NodeID, Version: p.Hash, Resource: &anypb.Any{TypeUrl: model.PlanTypeURL, Value: body}}, nil
}

// DecodePlanBody must only receive bytes already authenticated by the caller.
// The topology hash is opaque; unknown JSON fields do not invalidate it.
func DecodePlanBody(body []byte) (model.Plan, error) {
	if len(body) > model.MaxPlanBodyBytes {
		return model.Plan{}, model.ErrPlanTooLarge
	}
	var wire struct {
		Header   model.PlanHeader `json:"header"`
		Topology struct {
			Inbound  []json.RawMessage `json:"inbound"`
			Outbound []json.RawMessage `json:"outbound"`
		} `json:"topology"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return model.Plan{}, err
	}
	p := (model.WirePlan{Header: wire.Header}).Plan()
	if !p.InputValid() {
		return p, model.ErrInvalid
	}
	for _, raw := range wire.Topology.Inbound {
		var in model.InboundService
		if json.Unmarshal(raw, &in) != nil {
			continue
		}
		// Unknown protocols or invalid permission attributes cannot authorize.
		single := p
		single.Inbound = []model.InboundService{in}
		if !single.Valid() {
			in.Allow = nil
		}
		// A future L7 attribute cannot silently turn into an unrestricted allow.
		var fields struct {
			Allow []map[string]json.RawMessage `json:"allow"`
		}
		if json.Unmarshal(raw, &fields) != nil {
			continue
		}
		for _, allow := range fields.Allow {
			for key := range allow {
				if key != "source" && key != "l7" {
					in.Allow = nil
				}
			}
			var l7 map[string]json.RawMessage
			if json.Unmarshal(allow["l7"], &l7) != nil && len(allow["l7"]) != 0 {
				in.Allow = nil
			}
			for key := range l7 {
				if key != "method" && key != "path" && key != "methods" && key != "path_prefix" {
					in.Allow = nil
				}
			}
		}
		p.Inbound = append(p.Inbound, in)
	}
	for _, raw := range wire.Topology.Outbound {
		var out model.OutboundService
		if json.Unmarshal(raw, &out) != nil {
			continue
		}
		single := p
		single.Inbound = nil
		single.Outbound = []model.OutboundService{out}
		if single.Valid() {
			p.Outbound = append(p.Outbound, out)
		}
	}
	return p, nil
}

func DecodePlan(r *discoveryv1.Resource) (model.Plan, error) {
	if r.GetResource().GetTypeUrl() != model.PlanTypeURL {
		return model.Plan{}, errors.New("mesh: unsupported plan envelope type")
	}
	p, err := DecodePlanBody(r.GetResource().GetValue())
	if err != nil {
		return p, err
	}
	if r.GetName() != p.NodeID || r.GetVersion() != p.Hash {
		return p, model.ErrInvalid
	}
	return p, nil
}
