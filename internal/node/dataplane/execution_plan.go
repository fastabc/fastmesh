package dataplane

import (
	"time"

	"github.com/fastabc/fastmesh/model"
)

// ExecutionPlan gives signed wall-clock deadlines a process-local monotonic
// component before publication. The journal and signature keep the original
// plan; only the serving copy carries local time. On restart, persisted wall
// deadlines are anchored again, without granting a new TTL.
func (r *Runtime) ExecutionPlan(p *model.Plan) *model.Plan {
	if p == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.CurrentPlan()
	if p == r.inputPlan {
		return old // Credential reapplication must preserve pointer identity.
	}
	now := time.Now()
	copy := executablePlan(p)
	var previous time.Time
	endpoints := map[endpointKey]time.Time{}
	if old != nil {
		previous = old.NotAfter
		for _, out := range old.Outbound {
			for _, e := range out.Endpoints {
				endpoints[keyOf(e)] = e.LeaseExpiresAt
			}
		}
	}
	deadline := p.NotAfter
	if !p.RestoreNotAfter.IsZero() && p.RestoreNotAfter.Before(deadline) {
		deadline = p.RestoreNotAfter
	}
	copy.NotAfter = localDeadline(deadline, previous, now)
	copy.Outbound = append([]model.OutboundService(nil), copy.Outbound...)
	for i := range copy.Outbound {
		copy.Outbound[i].Endpoints = append([]model.Endpoint(nil), copy.Outbound[i].Endpoints...)
		for j := range copy.Outbound[i].Endpoints {
			e := &copy.Outbound[i].Endpoints[j]
			e.LeaseExpiresAt = localDeadline(e.LeaseExpiresAt, endpoints[keyOf(*e)], now)
		}
	}
	return &copy
}

func localDeadline(signed, previous, now time.Time) time.Time {
	if signed.Equal(previous) {
		return previous // Unchanged signed expiry cannot gain time after a clock step.
	}
	deadline := now.Add(signed.Sub(now))
	if !previous.IsZero() && signed.Before(previous) && previous.Before(deadline) {
		return previous // A shorter signed deadline cannot extend local validity.
	}
	return deadline
}

// Keep the signed input untouched. The serving copy denies an entire invalid
// service entry, including duplicate service/port entries, after verification.
func executablePlan(p *model.Plan) model.Plan {
	copy := *p
	copy.Inbound, copy.Outbound = nil, nil
	inboundCounts, outboundCounts := map[string]int{}, map[string]int{}
	for _, in := range p.Inbound {
		inboundCounts[in.Service+"/"+in.Port.Name]++
	}
	for _, out := range p.Outbound {
		outboundCounts[out.Service+"/"+out.Port.Name]++
	}
	entry := copy
	for _, in := range p.Inbound {
		entry.Inbound = []model.InboundService{in}
		if inboundCounts[in.Service+"/"+in.Port.Name] == 1 && entry.Valid() {
			copy.Inbound = append(copy.Inbound, in)
		}
	}
	entry.Inbound = nil
	for _, out := range p.Outbound {
		entry.Outbound = []model.OutboundService{out}
		if outboundCounts[out.Service+"/"+out.Port.Name] == 1 && entry.Valid() {
			copy.Outbound = append(copy.Outbound, out)
		}
	}
	return copy
}
