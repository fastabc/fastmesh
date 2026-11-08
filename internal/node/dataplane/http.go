package dataplane

import (
	"context"
	"net"
	"net/http"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type peerContextKey struct{}

func PeerContext(ctx context.Context, c net.Conn) context.Context {
	id, ok := PeerID(c)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, peerContextKey{}, id)
}

func (r *Runtime) HTTPHandler(ref ServiceRef, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		id, ok := PeerFromContext(req.Context())
		p := r.CurrentPlan()
		observer := r.Observer()
		if in := r.Inbound(p, ref); in != nil {
			l7 := in.RequiresL7
			for _, rule := range in.Allow {
				l7 = l7 || rule.L7 != nil
			}
			if l7 && !model.CanonicalRequestPath(req.URL.Path, req.URL.RawPath) {
				if observer != nil {
					observer.ConnDenied(model.DirectionInbound, ref, id, "mesh: non-canonical request path")
				}
				http.Error(w, "service access denied", http.StatusForbidden)
				return
			}
		}
		if !ok || !Fresh(p) || !r.AllowedInbound(p, ref, id, true, req) {
			if observer != nil {
				observer.ConnDenied(model.DirectionInbound, ref, id, model.ErrDenied.Error())
			}
			http.Error(w, "service access denied", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, req)
	})
}

// PeerFromContext returns the caller identity verified by the mesh handshake.
// PeerContext attaches it to HTTP connections; gRPC supplies verified AuthInfo.
func PeerFromContext(ctx context.Context) (spiffeid.ID, bool) {
	id, ok := ctx.Value(peerContextKey{}).(spiffeid.ID)
	if ok && !id.IsZero() {
		return id, true
	}
	if p, ok := peer.FromContext(ctx); ok {
		if info, ok := p.AuthInfo.(AuthInfo); ok {
			return info.ID, !info.ID.IsZero()
		}
	}
	return spiffeid.ID{}, false
}

type AuthInfo struct {
	credentials.CommonAuthInfo
	ID      spiffeid.ID
	Service ServiceRef
}

func (AuthInfo) AuthType() string { return "fastmesh" }
