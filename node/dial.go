package node

import (
	"context"
	"net"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func (n *Node) Dial(ctx context.Context, service ServiceRef) (net.Conn, error) {
	return n.dialPlan(ctx, service, n.runtime.CurrentPlan())
}

func (n *Node) dialTimeout() time.Duration {
	if n.cfg.DialTimeout > 0 {
		return n.cfg.DialTimeout
	}
	return n.cfg.ConnectTimeout
}

// DialFromPlan selects from the verified plan captured with a composed host's
// request view. The live plan is checked again at connection admission, so a
// retired request view cannot bypass a later revocation. Hosts must supply only
// plans previously accepted by their composed projection.
func DialFromPlan(ctx context.Context, n *Node, service ServiceRef, plan *model.Plan) (net.Conn, error) {
	return n.dialPlan(ctx, service, plan)
}

func (n *Node) dialPlan(ctx context.Context, ref ServiceRef, plan *model.Plan) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, n.dialTimeout())
	defer cancel()
	conn, err := n.runtime.DialPlan(ctx, ref, plan)
	if Reason(err) == "denied" && n.runtime.Observer() != nil {
		n.runtime.Observer().ConnDenied(Outbound, ref, spiffeid.ID{}, err.Error())
	}
	return conn, err
}
