package node

import (
	"context"
	"net"
	"net/http"
	"net/url"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// GRPCListen binds a gRPC service. Serve with GRPCServerCredentials and both
// GRPCAuthorizer interceptors to enforce per-method L7 permissions.
func (n *Node) GRPCListen(ctx context.Context, service ServiceRef) (*Listener, error) {
	in := dataplane.Inbound(n.runtime.CurrentPlan(), service)
	if in == nil || in.Port.Protocol != "grpc" {
		return nil, ErrDenied
	}
	return n.listen(ctx, service, true)
}

// NewGRPCServer binds a service and installs authenticated mesh credentials and
// both method authorizers. Register services before calling Serve(listener).
// The caller owns server shutdown and must close the returned listener. Hosts
// needing additional gRPC options may use the lower-level construction seams.
func (n *Node) NewGRPCServer(ctx context.Context, service ServiceRef) (*grpc.Server, *Listener, error) {
	listener, err := n.GRPCListen(ctx, service)
	if err != nil {
		return nil, nil, err
	}
	unary, stream := n.GRPCAuthorizer(service)
	server := grpc.NewServer(
		grpc.Creds(n.GRPCServerCredentials()),
		grpc.ChainUnaryInterceptor(unary),
		grpc.ChainStreamInterceptor(stream),
		grpc.MaxConcurrentStreams(128),
	)
	return server, listener, nil
}

// GRPCDialOptions uses the mesh's authenticated connection as gRPC transport.
func (n *Node) GRPCDialOptions(service ServiceRef) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return n.Dial(ctx, service) }),
		grpc.WithTransportCredentials(meshCredentials{service: service}),
		grpc.WithAuthority(service.Service + ".mesh"),
	}
}
func (n *Node) GRPCServerCredentials() credentials.TransportCredentials {
	return meshCredentials{server: true}
}

type meshCredentials struct {
	service ServiceRef
	server  bool
}

func (c meshCredentials) authenticate(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	m, ok := conn.(*dataplane.Conn)
	id, verified := dataplane.PeerID(conn)
	if !ok || !verified || m.Outbound() == c.server || (!c.server && m.Service() != c.service) {
		return nil, nil, ErrDenied
	}
	return conn, dataplane.AuthInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}, ID: id, Service: m.Service()}, nil
}
func (c meshCredentials) ClientHandshake(_ context.Context, _ string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.authenticate(conn)
}
func (c meshCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.authenticate(conn)
}
func (meshCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "fastmesh", SecurityVersion: "1"}
}
func (c meshCredentials) Clone() credentials.TransportCredentials { return c }
func (meshCredentials) OverrideServerName(string) error           { return nil }

// GRPCAuthorizer treats each method as POST /package.Service/Method. Both
// unary and streaming RPCs obtain identity from the verified mesh connection.
func (n *Node) GRPCAuthorizer(service ServiceRef) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	authorize := func(ctx context.Context, method string) error {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing mesh peer")
		}
		info, ok := p.AuthInfo.(dataplane.AuthInfo)
		if !ok || info.Service != service {
			return status.Error(codes.Unauthenticated, "wrong mesh service")
		}
		plan := n.runtime.CurrentPlan()
		req := &http.Request{Method: "POST", URL: &url.URL{Path: method}}
		if !dataplane.Fresh(plan) || !dataplane.AllowedInbound(plan, service, info.ID, true, req) {
			return status.Error(codes.PermissionDenied, "service access denied")
		}
		return nil
	}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorize(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		check := func() error { return authorize(ss.Context(), info.FullMethod) }
		if err := check(); err != nil {
			return err
		}
		return handler(srv, &authorizedStream{ServerStream: ss, check: check})
	}
	return unary, stream
}

type authorizedStream struct {
	grpc.ServerStream
	check func() error
}

func (s *authorizedStream) RecvMsg(m any) error {
	if err := s.check(); err != nil {
		return err
	}
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	// Policy can change while RecvMsg blocks waiting for a peer message.
	return s.check()
}
func (s *authorizedStream) SendMsg(m any) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.ServerStream.SendMsg(m)
}
