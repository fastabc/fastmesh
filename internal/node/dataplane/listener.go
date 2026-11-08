package dataplane

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync"

	"github.com/fastabc/fastmesh/model"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

func (r *Runtime) Listen(ctx context.Context, ref ServiceRef, address string, httpMode bool) (*Listener, error) {
	p := r.CurrentPlan()
	in := r.Inbound(p, ref)
	if !Fresh(p) || in == nil || (in.RequiresL7 && !httpMode) || (httpMode && in.Port.Protocol != "http" && in.Port.Protocol != "grpc") {
		return nil, model.ErrDenied
	}
	native := spiffe.X509Source{Context: context.Background(), Source: r.identity}
	cfg := tlsconfig.MTLSServerConfig(native, native, func(peer spiffeid.ID, _ [][]*x509.Certificate) error {
		p := r.CurrentPlan()
		if !Fresh(p) || !r.AllowedInbound(p, ref, peer, httpMode, nil) {
			return model.ErrDenied
		}
		return nil
	})
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	handshakeCtx, cancel := context.WithCancel(context.Background())
	l := &Listener{listener: lis, runtime: r, ref: ref, httpMode: httpMode, config: cfg, ctx: handshakeCtx, cancel: cancel, accepted: make(chan net.Conn, MaxPendingHandshakes), done: make(chan struct{})}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		cancel()
		_ = lis.Close()
		return nil, net.ErrClosed
	}
	r.listeners[l] = struct{}{}
	return l, nil
}

const MaxPendingHandshakes = 128

type Listener struct {
	listener         net.Listener
	runtime          *Runtime
	ref              ServiceRef
	httpMode         bool
	config           *tls.Config
	ctx              context.Context
	cancel           context.CancelFunc
	accepted         chan net.Conn
	done             chan struct{}
	start, closeOnce sync.Once
	wg               sync.WaitGroup
	mu               sync.Mutex
	pending          []*Conn
	acceptErr        error
}

func (l *Listener) Accept() (net.Conn, error) {
	l.start.Do(func() { l.wg.Add(1); go l.acceptLoop() })
	select {
	case c := <-l.accepted:
		return c, nil
	case <-l.done:
		l.mu.Lock()
		err := l.acceptErr
		l.mu.Unlock()
		if err == nil {
			err = net.ErrClosed
		}
		return nil, err
	}
}

func (l *Listener) acceptLoop() {
	defer l.wg.Done()
	for {
		raw, err := l.listener.Accept()
		if err != nil {
			l.mu.Lock()
			l.acceptErr = err
			l.mu.Unlock()
			l.stop()
			return
		}
		tlsConn := tls.Server(raw, l.config)
		c := &Conn{conn: tlsConn, runtime: l.runtime, ref: l.ref, httpMode: l.httpMode}
		if err := l.runtime.admit(c); err != nil {
			_ = raw.Close()
			continue
		}
		oldest := l.addPending(c)
		if oldest != nil {
			_ = oldest.Close()
		}
		l.wg.Add(1)
		go l.handshake(c, tlsConn)
	}
}

func (l *Listener) handshake(c *Conn, tlsConn *tls.Conn) {
	defer l.wg.Done()
	defer l.removePending(c)
	ctx, cancel := context.WithTimeout(l.ctx, l.runtime.timeout)
	defer cancel()
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		l.runtime.denied(c)
		_ = c.Close()
		return
	}
	peer, err := x509svid.IDFromCert(tlsConn.ConnectionState().PeerCertificates[0])
	if err != nil {
		_ = c.Close()
		return
	}
	if !l.finishHandshake(c, peer) {
		l.runtime.denied(c)
		_ = c.Close()
		return
	}

	select {
	case l.accepted <- c:
	case <-l.done:
		_ = c.Close()
	default:
		_ = c.Close() // Bound completed handshakes when the application stops accepting.
	}
}

// The pending lock orders successful handshakes against eviction. Removing a
// victim before closing it prevents that victim from becoming ready meanwhile.
func (l *Listener) addPending(c *Conn) *Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	var oldest *Conn
	if len(l.pending) >= MaxPendingHandshakes {
		oldest = l.pending[0]
		l.pending = l.pending[1:]
	}
	l.pending = append(l.pending, c)
	return oldest
}

func (l *Listener) removePending(c *Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, pending := range l.pending {
		if pending == c {
			l.pending = append(l.pending[:i], l.pending[i+1:]...)
			return true
		}
	}
	return false
}

func (l *Listener) finishHandshake(c *Conn, peer spiffeid.ID) bool {
	if !l.removePending(c) {
		return false
	} // Already selected for eviction.
	l.runtime.mu.Lock()
	defer l.runtime.mu.Unlock()
	c.peer = peer
	p := l.runtime.CurrentPlan()
	if l.runtime.closed || !Fresh(p) || !c.allowed(p) {
		return false
	}
	c.ready = true
	return true
}

func (l *Listener) stop() {
	l.closeOnce.Do(func() { l.cancel(); close(l.done); _ = l.listener.Close() })
}

func (l *Listener) Close() error {
	l.stop()
	l.start.Do(func() {})
	l.wg.Wait()
	for {
		select {
		case c := <-l.accepted:
			_ = c.Close()
		default:
			goto drained
		}
	}
drained:
	l.runtime.mu.Lock()
	delete(l.runtime.listeners, l)
	l.runtime.mu.Unlock()
	return nil
}

func (l *Listener) Service() ServiceRef { return l.ref }
func (l *Listener) Addr() net.Addr      { return l.listener.Addr() }
func (l *Listener) PendingCount() int   { l.mu.Lock(); defer l.mu.Unlock(); return len(l.pending) }
