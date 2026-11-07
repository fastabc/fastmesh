package dataplane

import (
	"maps"
	"math/rand/v2"
	"net/netip"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/breaker"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

type dialCandidate struct {
	Endpoint model.Endpoint
	Circuit  *breaker.Breaker
}

// Select from two random candidates using admitted outbound connection counts.
func (r *Runtime) selectCandidate(candidates []dialCandidate) int {
	if len(candidates) < 2 {
		return 0
	}
	second := 1 + rand.IntN(len(candidates)-1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.connections[keyOf(candidates[second].Endpoint)] < r.connections[keyOf(candidates[0].Endpoint)] {
		return second
	}
	return 0
}

func allTerminating(endpoints []model.Endpoint) bool {
	for _, endpoint := range endpoints {
		if endpoint.Serving && !endpoint.Terminating && time.Now().Before(endpoint.LeaseExpiresAt) {
			return false
		}
	}
	return true
}

type endpointKey struct {
	Service, Port, Instance string
	Owner                   spiffeid.ID
	Address                 netip.AddrPort
}

func keyOf(e model.Endpoint) endpointKey {
	return endpointKey{e.Service, e.Port, e.InstanceID, e.Owner, e.Address}
}

func (r *Runtime) Circuit(e model.Endpoint) *breaker.Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := keyOf(e)
	circuit := r.circuits[key]
	if circuit == nil {
		circuit = breaker.New(breaker.Config{})
		r.circuits[key] = circuit
	}
	return circuit
}

// DialableCounts snapshots map ownership under the lock, then checks each
// breaker's own synchronized state without blocking admission/reconciliation.
func (r *Runtime) DialableCounts(p *model.Plan, now time.Time) map[ServiceRef]int {
	out := map[ServiceRef]int{}
	if p == nil || !now.Before(p.NotAfter) || (!p.RestoreNotAfter.IsZero() && !now.Before(p.RestoreNotAfter)) {
		return out
	}
	r.mu.Lock()
	circuits := maps.Clone(r.circuits)
	r.mu.Unlock()
	for _, service := range p.Outbound {
		ref := ServiceRef{Service: service.Service, Port: service.Port.Name}
		allTerminating := allTerminating(service.Endpoints)
		for _, e := range service.Endpoints {
			if !now.Before(e.LeaseExpiresAt) || (!e.Serving || e.Terminating && !allTerminating) {
				continue
			}
			if b := circuits[keyOf(e)]; b != nil && !b.CanAttempt() {
				continue
			}
			out[ref]++
		}
	}
	return out
}
