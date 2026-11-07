package node

import (
	"context"
	"net"
)

// WaitService waits for a locally selectable endpoint, not remote business health.
// Start must have been called. Cancel ctx to stop waiting for a missing service.
func (n *Node) WaitService(ctx context.Context, ref ServiceRef) error {
	n.mu.Lock()
	started, closed, lifetime := n.started, n.closed, n.ctx
	n.mu.Unlock()
	if !started || closed {
		return net.ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(lifetime, cancel)
	defer stop()
	return n.runtime.WaitService(ctx, ref)
}
