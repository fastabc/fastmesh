package node

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
)

// HTTPHandler binds an HTTP-only listener and serves next after per-request
// authorization. Close the returned listener to unpublish the endpoint.
func (n *Node) HTTPHandler(ctx context.Context, service ServiceRef, next http.Handler) (*Listener, error) {
	if next == nil {
		return nil, errors.New("mesh node: HTTP handler required")
	}
	l, err := n.listen(ctx, service, true)
	if err != nil {
		return nil, err
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{Protocols: protocols, Handler: n.runtime.HTTPHandler(service, next), ConnContext: dataplane.PeerContext, ReadHeaderTimeout: n.cfg.ConnectTimeout}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, net.ErrClosed
	}
	l.httpServer = server
	n.mu.Unlock()
	go func() { _ = server.Serve(l.listener) }()
	return l, nil
}

// HTTPClient is scoped to service; both HTTP and HTTPS URLs use authenticated
// mesh connections. The caller owns the standard transport's idle lifecycle.
func (n *Node) HTTPClient(service ServiceRef) *http.Client {
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) { return n.Dial(ctx, service) }
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	p := n.runtime.CurrentPlan()
	if p != nil {
		for _, out := range p.Outbound {
			if out.Service == service.Service && out.Port.Name == service.Port && out.Port.Protocol == "grpc" {
				protocols.SetHTTP1(false)
				protocols.SetUnencryptedHTTP2(true)
			}
		}
	}
	return &http.Client{Transport: &http.Transport{Protocols: protocols, DialContext: dial, DialTLSContext: dial}}
}

// HTTPClientWithProtocol constructs a pooled client independently of plan timing.
// protocol is "http" for HTTP/1 or "grpc" for h2c over mesh mTLS.
func (n *Node) HTTPClientWithProtocol(ref ServiceRef, protocol string) (*http.Client, error) {
	if protocol != "http" && protocol != "grpc" {
		return nil, ErrProtocolMismatch
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(protocol == "http")
	protocols.SetUnencryptedHTTP2(protocol == "grpc")
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) { return n.Dial(ctx, ref) }
	transport := &http.Transport{Protocols: protocols, DialContext: dial, DialTLSContext: dial}
	return &http.Client{Transport: &protocolTransport{Transport: transport, node: n, ref: ref, protocol: protocol}}, nil
}

type protocolTransport struct {
	*http.Transport
	node     *Node
	ref      ServiceRef
	protocol string
}

func (t *protocolTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	p := t.node.runtime.CurrentPlan()
	if !dataplane.Fresh(p) {
		return nil, ErrPlanUnavailable
	}
	for _, out := range p.Outbound {
		if out.Service == t.ref.Service && out.Port.Name == t.ref.Port {
			if out.Port.Protocol != t.protocol {
				return nil, ErrProtocolMismatch
			}
			return t.Transport.RoundTrip(req)
		}
	}
	return nil, ErrServiceUnavailable
}
