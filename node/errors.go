package node

import (
	"context"
	"errors"
	"net"

	"github.com/fastabc/fastmesh/model"

	"github.com/fastabc/fastmesh/node/breaker"
	"github.com/fastabc/fastmesh/policy/apply"
)

var (
	ErrPlanUnavailable    = model.ErrPlanUnavailable
	ErrServiceUnavailable = model.ErrServiceUnavailable
	ErrNoEndpoints        = model.ErrNoEndpoints
	ErrCircuitOpen        = breaker.ErrOpen
	ErrProtocolMismatch   = errors.New("mesh: service protocol mismatch")
)

// Reason returns a bounded diagnostic vocabulary, suitable for metric labels.
func Reason(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, net.ErrClosed):
		return "closed"
	case errors.Is(err, apply.ErrRecoveryRequired):
		return "recovery_required"
	case errors.Is(err, ErrPlanUnavailable):
		return "plan_unavailable"
	case errors.Is(err, ErrServiceUnavailable):
		return "service_unavailable"
	case errors.Is(err, ErrNoEndpoints):
		return "no_endpoints"
	case errors.Is(err, ErrCircuitOpen):
		return "circuit_open"
	case errors.Is(err, ErrProtocolMismatch):
		return "protocol_mismatch"
	case errors.Is(err, ErrDenied):
		return "denied"
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return "network"
	}
	return "error"
}

var ErrDenied = model.ErrDenied
