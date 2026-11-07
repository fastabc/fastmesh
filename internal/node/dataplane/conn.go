package dataplane

import (
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// Admission maintains a closed-when-zero channel for bounded shutdown.
// Verified peer/service metadata permits selective synchronous revocation.
func (r *Runtime) admit(c *Conn) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.CurrentPlan()
	if r.closed {
		return net.ErrClosed
	}
	if c.ready && (!Fresh(p) || !c.allowed(p)) {
		return model.ErrDenied
	}
	if c.outbound {
		live := false
		for _, out := range p.Outbound {
			if out.Service != c.ref.Service || out.Port.Name != c.ref.Port {
				continue
			}
			for _, endpoint := range out.Endpoints {
				if endpoint.Owner == c.peer && endpoint.InstanceID == c.endpoint.InstanceID && endpoint.Address == c.endpoint.Address && time.Now().Before(endpoint.LeaseExpiresAt) && (endpoint.Serving && (!endpoint.Terminating || allTerminating(out.Endpoints))) {
					live = true
				}
			}
		}
		if !live {
			return model.ErrDenied
		}
	}
	if len(r.active) == 0 {
		r.zero = make(chan struct{})
	}
	r.active[c] = struct{}{}
	if c.outbound {
		r.connections[keyOf(c.endpoint)]++
	}
	return nil
}

type Conn struct {
	conn                      net.Conn
	runtime                   *Runtime
	ref                       ServiceRef
	peer                      spiffeid.ID
	outbound, httpMode, ready bool
	endpoint                  model.Endpoint
	once                      sync.Once
}

func (c *Conn) Close() error {
	err := c.conn.Close()
	c.once.Do(func() {
		c.runtime.mu.Lock()
		defer c.runtime.mu.Unlock()
		delete(c.runtime.active, c)
		if c.outbound {
			key := keyOf(c.endpoint)
			c.runtime.connections[key]--
			if c.runtime.connections[key] == 0 {
				delete(c.runtime.connections, key)
			}
		}
		if len(c.runtime.active) == 0 {
			close(c.runtime.zero)
		}
	})
	return err
}

// PeerID returns only the identity verified by this node's TLS handshake.
func PeerID(conn net.Conn) (spiffeid.ID, bool) {
	c, ok := conn.(*Conn)
	if !ok || !c.ready {
		return spiffeid.ID{}, false
	}
	return c.peer, true
}

func (c *Conn) Service() ServiceRef                { return c.ref }
func (c *Conn) Outbound() bool                     { return c.outbound }
func (c *Conn) Read(b []byte) (int, error)         { return c.conn.Read(b) }
func (c *Conn) Write(b []byte) (int, error)        { return c.conn.Write(b) }
func (c *Conn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr               { return c.conn.RemoteAddr() }
func (c *Conn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

// TLSState returns a TLS snapshot without exposing the mutable socket.
func (c *Conn) TLSState() tls.ConnectionState {
	if conn, ok := c.conn.(*tls.Conn); ok {
		return conn.ConnectionState()
	}
	return tls.ConnectionState{}
}
