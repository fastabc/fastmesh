package dataplane

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"net"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/breaker"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
)

func (r *Runtime) Dial(ctx context.Context, ref ServiceRef) (net.Conn, error) {
	return r.DialPlan(ctx, ref, r.CurrentPlan())
}

func (r *Runtime) DialPlan(ctx context.Context, ref ServiceRef, p *model.Plan) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !Fresh(p) {
		return nil, model.ErrPlanUnavailable
	}
	var endpoints []model.Endpoint
	found := false
	for _, out := range p.Outbound {
		if out.Service == ref.Service && out.Port.Name == ref.Port {
			found = true
			endpoints = out.Endpoints
			break
		}
	}
	if !found {
		return nil, model.ErrServiceUnavailable
	}
	var last error = model.ErrNoEndpoints
	defer r.notifyAvailability()
	candidates := make([]dialCandidate, 0, len(endpoints))
	fallback := make([]dialCandidate, 0)
	for _, endpoint := range endpoints {
		if !time.Now().Before(endpoint.LeaseExpiresAt) || !endpoint.Serving {
			continue
		}
		r.mu.Lock()
		key := keyOf(endpoint)
		circuit := r.circuits[key]
		if circuit == nil {
			circuit = breaker.New(breaker.Config{})
			r.circuits[key] = circuit
		}
		r.mu.Unlock()
		if !circuit.CanAttempt() {
			last = breaker.ErrOpen
			continue
		}
		if endpoint.Terminating {
			fallback = append(fallback, dialCandidate{endpoint, circuit})
		} else {
			candidates = append(candidates, dialCandidate{endpoint, circuit})
		}
	}
	if len(candidates) == 0 && allTerminating(endpoints) {
		candidates = fallback
	}
	if len(candidates) > 0 {
		start := rand.IntN(len(candidates))
		candidates = append(candidates[start:], candidates[:start]...)
	}
	for i := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		choice := r.selectCandidate(candidates[i:])
		candidates[i], candidates[i+choice] = candidates[i+choice], candidates[i]
		candidate := candidates[i]
		endpoint, circuit := candidate.Endpoint, candidate.Circuit
		if !time.Now().Before(endpoint.LeaseExpiresAt) || !endpoint.Serving {
			continue
		}
		probe, err := circuit.AllowWithProbe()
		if err != nil {
			last = err
			continue
		}
		native := spiffe.X509Source{Context: ctx, Source: r.identity}
		cfg := tlsconfig.MTLSClientConfig(native, native, tlsconfig.AuthorizeID(endpoint.Owner))
		attemptTimeout := r.timeout
		if deadline, ok := ctx.Deadline(); ok {
			attemptTimeout = min(attemptTimeout, time.Until(deadline)/time.Duration(len(candidates)-i))
		}
		dialCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		conn, err := (&tls.Dialer{NetDialer: &net.Dialer{}, Config: cfg}).DialContext(dialCtx, "tcp", endpoint.Address.String())
		cancel()
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				if probe {
					circuit.CancelProbe()
				}
				return nil, canceled
			}
			if observer := r.Observer(); observer != nil {
				observer.DialFailed(ref, endpoint.Address, err)
			}
			circuit.OnFailure()
			last = err
			continue
		}
		circuit.OnSuccess()
		tracked := &Conn{conn: conn, runtime: r, ref: ref, peer: endpoint.Owner, outbound: true, endpoint: endpoint, ready: true}
		if err := r.admit(tracked); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tracked, nil
	}
	return nil, last
}
