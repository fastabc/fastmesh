package node

import (
	"net/netip"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

type Direction = model.Direction

const (
	Inbound  = model.DirectionInbound
	Outbound = model.DirectionOutbound
)

// Observer callbacks run without runtime locks and must be concurrency safe.
// Implementations may inspect Status and must return promptly.
type Observer interface {
	PlanApplied(int64, time.Time, bool)
	ConnDenied(Direction, ServiceRef, spiffeid.ID, string)
	ConnRevoked(Direction, ServiceRef, spiffeid.ID)
	DialFailed(ServiceRef, netip.AddrPort, error)
	ControlContact(string, error)
}

func WithObserver(o Observer) Option {
	return func(n *Node) error { n.runtime.SetObserver(o); return nil }
}
