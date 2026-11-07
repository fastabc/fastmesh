package dataplane

import (
	"net/netip"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// Observer callbacks run synchronously after releasing internal locks. They
// may inspect Status and must return promptly; implementations must be safe
// for concurrent control, dial and listener callbacks.
type Observer interface {
	PlanApplied(rev int64, notAfter time.Time, topologyChanged bool)
	ConnDenied(dir model.Direction, svc ServiceRef, peer spiffeid.ID, reason string)
	ConnRevoked(dir model.Direction, svc ServiceRef, peer spiffeid.ID)
	DialFailed(svc ServiceRef, endpoint netip.AddrPort, err error)
	ControlContact(addr string, err error)
}

func (r *Runtime) Applied(p *model.Plan) {
	observer := r.Observer()
	if p == nil || observer == nil {
		return
	}
	r.mu.Lock()
	changed := r.observedHash != p.Hash
	r.observedHash = p.Hash
	r.mu.Unlock()
	observer.PlanApplied(p.InputRevision, p.NotAfter, changed)
}
func (r *Runtime) denied(c *Conn) {
	if observer := r.Observer(); observer != nil {
		dir := model.DirectionInbound
		if c.outbound {
			dir = model.DirectionOutbound
		}
		observer.ConnDenied(dir, c.ref, c.peer, model.ErrDenied.Error())
	}
}
