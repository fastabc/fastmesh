package dataplane

import (
	"context"
	"net"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/breaker"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

func (r *Runtime) Prepare(ctx context.Context, snap *snapshot.Snapshot) (apply.PreparedChange, error) {
	if snap == nil {
		return nil, model.ErrInvalid
	}
	input := snap.MeshPlan
	if r.verifyStoredPlan != nil {
		var err error
		input, err = r.verifyStoredPlan(ctx, snap)
		if err != nil {
			return nil, err
		}
	}
	if input == nil || !input.InputValid() {
		return nil, model.ErrInvalid
	}
	snap.MeshPlan = input
	recipient := r.Recipient()
	if recipient.NodeID != "" && (input.NodeID != recipient.NodeID || input.Tenant != recipient.Tenant || input.Workload != recipient.Workload) {
		return nil, model.ErrDenied
	}
	p := r.ExecutionPlan(input)
	return apply.PreparedChangeFunc{CommitFunc: func(context.Context) error {
		r.mu.Lock()
		r.plan.Store(p)
		r.inputPlan = input
		r.mu.Unlock()
		if err := r.Reconcile(); err != nil {
			return err
		}
		r.Applied(p)
		return nil
	}}, nil
}

func (r *Runtime) CurrentPlan() *model.Plan {
	if r.planSource != nil {
		return r.planSource()
	}
	return r.plan.Load()
}

// Reconcile connections against the already-published tuple. This does not
// publish policy: composed runtimes own that single atomic publication.
func (r *Runtime) Reconcile() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return net.ErrClosed
	}
	p := r.CurrentPlan()
	circuits := map[endpointKey]*breaker.Breaker{}
	if p != nil {
		for _, out := range p.Outbound {
			for _, endpoint := range out.Endpoints {
				key := keyOf(endpoint)
				circuit := r.circuits[key]
				if circuit == nil {
					circuit = breaker.New(breaker.Config{})
				}
				circuits[key] = circuit
			}
		}
	}
	r.circuits = circuits
	r.signalAvailability()
	var revoked []*Conn
	for c := range r.active {
		if c.ready && !c.allowed(p) {
			revoked = append(revoked, c)
		}
	}
	r.mu.Unlock()
	for _, c := range revoked {
		_ = c.Close()
	}
	observer := r.Observer()
	for _, c := range revoked {
		if observer != nil {
			dir := model.DirectionInbound
			if c.outbound {
				dir = model.DirectionOutbound
			}
			observer.ConnRevoked(dir, c.ref, c.peer)
		}
	}
	return nil
}
