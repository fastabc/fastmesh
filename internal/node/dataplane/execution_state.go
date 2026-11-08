package dataplane

import (
	"sync/atomic"

	"github.com/fastabc/fastmesh/model"
)

const serviceIndexThreshold = 16

// planHolder publishes a plan and its derived service indexes together. Small
// plans retain slice lookup. Indexes never enter the signed/persisted model.
type planHolder struct {
	current atomic.Pointer[executionState]
}
type executionState struct {
	plan     *model.Plan
	inbound  map[ServiceRef]*model.InboundService
	outbound map[ServiceRef]*model.OutboundService
}

func (h *planHolder) Store(p *model.Plan) {
	if p == nil {
		h.current.Store(nil)
		return
	}
	next := &executionState{plan: p}
	if len(p.Inbound) >= serviceIndexThreshold {
		next.inbound = make(map[ServiceRef]*model.InboundService, len(p.Inbound))
		for i := range p.Inbound {
			in := &p.Inbound[i]
			ref := ServiceRef{Service: in.Service, Port: in.Port.Name}
			if next.inbound[ref] == nil {
				next.inbound[ref] = in
			}
		}
	}
	if len(p.Outbound) >= serviceIndexThreshold {
		next.outbound = make(map[ServiceRef]*model.OutboundService, len(p.Outbound))
		for i := range p.Outbound {
			out := &p.Outbound[i]
			ref := ServiceRef{Service: out.Service, Port: out.Port.Name}
			if next.outbound[ref] == nil {
				next.outbound[ref] = out
			}
		}
	}
	h.current.Store(next)
}
func (h *planHolder) Load() *model.Plan {
	if next := h.current.Load(); next != nil {
		return next.plan
	}
	return nil
}
func (r *Runtime) Inbound(p *model.Plan, ref ServiceRef) *model.InboundService {
	if p != nil && len(p.Inbound) >= serviceIndexThreshold {
		if state := r.plan.current.Load(); state != nil && state.plan == p {
			return state.inbound[ref]
		}
	}
	// A host projection or concurrent publication may supply another immutable
	// plan. Never combine that plan with indexes from a different publication.
	return Inbound(p, ref)
}
func (r *Runtime) outbound(p *model.Plan, ref ServiceRef) *model.OutboundService {
	if p == nil {
		return nil
	}
	if len(p.Outbound) >= serviceIndexThreshold {
		if state := r.plan.current.Load(); state != nil && state.plan == p {
			return state.outbound[ref]
		}
	}
	for i := range p.Outbound {
		out := &p.Outbound[i]
		if out.Service == ref.Service && out.Port.Name == ref.Port {
			return out
		}
	}
	return nil
}
