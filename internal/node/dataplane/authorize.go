package dataplane

import (
	"net/http"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func Fresh(p *model.Plan) bool {
	return p != nil && time.Now().Before(p.NotAfter) && (p.RestoreNotAfter.IsZero() || time.Now().Before(p.RestoreNotAfter))
}

func Inbound(p *model.Plan, ref ServiceRef) *model.InboundService {
	if p != nil {
		for i := range p.Inbound {
			in := &p.Inbound[i]
			if in.Service == ref.Service && in.Port.Name == ref.Port {
				return in
			}
		}
	}
	return nil
}

func AllowedInbound(p *model.Plan, ref ServiceRef, peer spiffeid.ID, httpMode bool, request *http.Request) bool {
	return allowedInbound(Inbound(p, ref), peer, httpMode, request)
}

func (r *Runtime) AllowedInbound(p *model.Plan, ref ServiceRef, peer spiffeid.ID, httpMode bool, request *http.Request) bool {
	return allowedInbound(r.Inbound(p, ref), peer, httpMode, request)
}

func allowedInbound(in *model.InboundService, peer spiffeid.ID, httpMode bool, request *http.Request) bool {
	if in == nil || (in.RequiresL7 && !httpMode) {
		return false
	}
	for _, rule := range in.Allow {
		if rule.Source != peer {
			continue
		}
		if rule.L7 == nil {
			return true
		}
		if httpMode && (request == nil || rule.L7.Matches(request.Method, request.URL.Path)) {
			return true
		}
	}
	return false
}

func (c *Conn) allowed(p *model.Plan) bool {
	if !c.outbound {
		return c.runtime.AllowedInbound(p, c.ref, c.peer, c.httpMode, nil)
	}
	if out := c.runtime.outbound(p, c.ref); out != nil {
		for _, owner := range out.Owners {
			if owner == c.peer {
				return true
			}
		}
	}
	return false
}
