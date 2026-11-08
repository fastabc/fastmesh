package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Listener is a bound service endpoint. Close unpublishes it; SetServing
// changes application readiness without changing the instance generation.
type Listener struct {
	leaseMu    sync.Mutex
	serving    bool
	closing    bool
	listener   *dataplane.Listener
	node       *Node
	instance   string
	generation uint64
	cancel     context.CancelFunc
	done       chan struct{}
	once       sync.Once
	err        error
	httpServer *http.Server
	status     BindingStatus // protected by node.mu
}

func (n *Node) listen(ctx context.Context, ref ServiceRef, httpMode bool) (*Listener, error) {
	n.mu.Lock()
	if !n.started || n.closed {
		n.mu.Unlock()
		return nil, net.ErrClosed
	}
	if _, ok := n.bindings[ref]; ok {
		n.mu.Unlock()
		return nil, errors.New("mesh node: service already bound")
	}
	// Reserve the service while registration is in progress.
	n.bindings[ref] = nil
	n.mu.Unlock()
	var bound *Listener
	defer func() {
		if bound == nil {
			n.mu.Lock()
			delete(n.bindings, ref)
			n.mu.Unlock()
		}
	}()
	l, err := n.runtime.Listen(ctx, ref, n.cfg.ListenAddress, httpMode)
	if err != nil {
		return nil, err
	}
	id := sha256.Sum256([]byte(n.nodeID + "\x00" + ref.Service + "\x00" + ref.Port))
	b := &Listener{listener: l, node: n, instance: fmt.Sprintf("mesh-%x", id[:16]), serving: true, done: make(chan struct{})}
	if _, err := b.register(ctx, false); err != nil {
		_ = l.Close()
		return nil, err
	}
	lease, err := b.register(ctx, true)
	if err != nil {
		_ = l.Close()
		return nil, err
	}
	bctx, cancel := context.WithCancel(n.ctx)
	b.cancel = cancel
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		cancel()
		_ = l.Close()
		return nil, net.ErrClosed
	}
	n.bindings[ref] = b
	n.mu.Unlock()
	bound = b
	go b.renew(bctx, lease)
	return b, nil
}

func (b *Listener) register(ctx context.Context, serving bool) (*catalogv1.EndpointLease, error) {
	return b.registerConditions(ctx, serving, false)
}

func (b *Listener) registerConditions(ctx context.Context, serving, terminating bool) (*catalogv1.EndpointLease, error) {
	if serving {
		p := b.node.runtime.CurrentPlan()
		if !dataplane.Fresh(p) || b.node.runtime.Inbound(p, b.listener.Service()) == nil {
			return nil, ErrDenied
		}
	}
	generation, err := b.node.state.NextMeshGeneration()
	if err != nil {
		return nil, err
	}
	b.generation = generation
	lease, err := b.node.lease(ctx, func(ctx context.Context, client catalogv1.CatalogClient) (*catalogv1.EndpointLease, error) {
		return client.RegisterEndpoint(ctx, &catalogv1.RegisterEndpointRequest{NodeId: b.node.nodeID, Service: b.listener.Service().Service, Port: b.listener.Service().Port, InstanceId: b.instance, Generation: generation, Address: b.advertiseAddress(), Ready: serving && !terminating, Serving: serving, Terminating: terminating})
	})
	if err == nil {
		b.recordLease(lease, serving)
	}
	return lease, err
}

func (b *Listener) renew(ctx context.Context, lease *catalogv1.EndpointLease) {
	defer close(b.done)
	retry := time.Duration(0)
	for {
		delay := 100 * time.Millisecond
		if lease != nil {
			delay = max(time.Until(lease.ExpiresAt.AsTime())/3, time.Millisecond)
		}
		if retry > 0 {
			delay = retry
			if lease != nil {
				delay = min(delay, max(time.Until(lease.ExpiresAt.AsTime()), time.Millisecond))
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if lease != nil && !time.Now().Before(lease.ExpiresAt.AsTime()) {
			lease = nil
		}
		b.leaseMu.Lock()
		var next *catalogv1.EndpointLease
		var err error
		if lease == nil {
			next, err = b.register(ctx, b.serving)
		} else {
			next, err = b.node.lease(ctx, func(ctx context.Context, client catalogv1.CatalogClient) (*catalogv1.EndpointLease, error) {
				return client.RenewEndpoint(ctx, &catalogv1.EndpointRequest{NodeId: b.node.nodeID, Service: b.listener.Service().Service, InstanceId: b.instance, Generation: b.generation})
			})
		}
		if err == nil {
			b.recordLease(next, b.serving)
		}
		b.leaseMu.Unlock()
		if err == nil {
			lease, retry = next, 0
			continue
		}
		if status.Code(err) == codes.FailedPrecondition {
			lease = nil
		}
		retry = min(max(2*retry, 100*time.Millisecond), time.Second)
	}
}

func (b *Listener) advertiseAddress() string {
	if b.node.cfg.AdvertiseAddress == "" {
		return b.Addr().String()
	}
	a := netip.MustParseAddrPort(b.node.cfg.AdvertiseAddress)
	if a.Port() == 0 {
		a = netip.AddrPortFrom(a.Addr(), uint16(b.Addr().(*net.TCPAddr).Port))
	}
	return a.String()
}

func (b *Listener) SetServing(ok bool) error {
	b.leaseMu.Lock()
	defer b.leaseMu.Unlock()
	if b.closing {
		return net.ErrClosed
	}
	ctx, cancel := context.WithTimeout(b.node.ctx, b.node.cfg.ConnectTimeout)
	defer cancel()
	lease, err := b.node.lease(ctx, func(ctx context.Context, client catalogv1.CatalogClient) (*catalogv1.EndpointLease, error) {
		return client.SetEndpointServing(ctx, &catalogv1.EndpointRequest{NodeId: b.node.nodeID, Service: b.listener.Service().Service, InstanceId: b.instance, Generation: b.generation, Serving: ok})
	})
	if err == nil {
		b.serving = ok
		b.recordLease(lease, ok)
	}
	return err
}

func (b *Listener) Accept() (net.Conn, error) { return b.listener.Accept() }

func (b *Listener) Addr() net.Addr { return b.listener.Addr() }

func (b *Listener) recordLease(lease *catalogv1.EndpointLease, serving bool) {
	b.node.mu.Lock()
	defer b.node.mu.Unlock()
	b.status = BindingStatus{Serving: serving, LeaseExpiresAt: lease.GetExpiresAt().AsTime()}
}

func (b *Listener) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), b.node.cfg.ConnectTimeout)
	defer cancel()
	return b.close(ctx)
}

func (b *Listener) close(ctx context.Context) error {
	b.once.Do(func() {
		b.leaseMu.Lock()
		b.closing = true
		b.leaseMu.Unlock()
		b.cancel()
		<-b.done
		previous := b.generation
		_, _ = b.registerConditions(ctx, true, true)
		b.err = b.listener.Close()
		b.node.mu.Lock()
		httpServer := b.httpServer
		b.node.mu.Unlock()
		if httpServer != nil {
			if err := httpServer.Shutdown(ctx); err != nil {
				b.err = err
			}
		}
		// A failed publish may have committed remotely. Try the newly reserved
		// generation and the previous one; catalog CAS prevents deleting a newer
		// replacement while cleaning up either possible outcome.
		for _, generation := range []uint64{b.generation, previous} {
			for _, conn := range b.node.conns {
				_, err := catalogv1.NewCatalogClient(conn).DeregisterEndpoint(ctx, &catalogv1.EndpointRequest{NodeId: b.node.nodeID, Service: b.listener.Service().Service, InstanceId: b.instance, Generation: generation})
				if err == nil {
					break
				}
			}
		}
		b.node.mu.Lock()
		delete(b.node.bindings, b.listener.Service())
		b.node.mu.Unlock()
	})
	return b.err
}

func (n *Node) Listen(ctx context.Context, service ServiceRef) (*Listener, error) {
	return n.listen(ctx, service, false)
}
