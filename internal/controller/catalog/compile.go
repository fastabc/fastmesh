package catalog

import (
	"sort"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// Compile projects an already decoded immutable tenant catalog.
func Compile(c Catalog, revision int64, recipient model.Session, now time.Time, validity time.Duration) (model.Plan, error) {
	p := model.Plan{TypeURL: model.PlanTypeURL, Tenant: recipient.Tenant, NodeID: recipient.NodeID, Workload: recipient.Workload, InputRevision: revision, NotAfter: now.Add(validity).UTC(), AccessSet: true, EndpointsSet: true, Inbound: []model.InboundService{}, Outbound: []model.OutboundService{}}
	if recipient.Workload.IsZero() || recipient.NodeID == "" || validity <= 0 {
		return p, model.ErrInvalid
	}
	names := make([]string, 0, len(c.Services))
	for name := range c.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		record := c.Services[name]
		for _, port := range record.Spec.Ports {
			in := model.InboundService{Service: name, Port: port, RequiresL7: record.Spec.RequiresL7, Allow: []model.InboundAllow{}}
			allowed := false
			for _, rule := range c.Access {
				if rule.Service != name || (rule.Port != "" && rule.Port != port.Name) {
					continue
				}
				for _, source := range c.members(rule.Source) {
					in.Allow = append(in.Allow, model.InboundAllow{Source: source, L7: rule.L7})
					if source == recipient.Workload {
						allowed = true
					}
				}
			}
			if c.CanBind(recipient.Tenant, name, recipient.Workload.String()) {
				p.Inbound = append(p.Inbound, in)
			}
			if !allowed {
				continue
			}
			out := model.OutboundService{Service: name, Port: port, RequiresL7: record.Spec.RequiresL7, Endpoints: []model.Endpoint{}}
			owners := map[spiffeid.ID]bool{}
			for _, grant := range c.Grants {
				if grant.Tenant == recipient.Tenant && grant.Service == name {
					for _, owner := range c.members(grant.Workload) {
						if !owners[owner] {
							owners[owner] = true
							out.Owners = append(out.Owners, owner)
						}
					}
				}
			}

			sort.Slice(out.Owners, func(i, j int) bool { return out.Owners[i].String() < out.Owners[j].String() })
			for _, e := range record.Status.Endpoints {
				if e.Tenant == recipient.Tenant && e.Service == name && e.Port == port.Name && e.Serving && now.Before(e.LeaseExpiresAt) && owners[e.Owner] {
					out.Endpoints = append(out.Endpoints, e)
				}
			}
			p.Outbound = append(p.Outbound, out)
		}
	}
	if !p.Valid() {
		return p, model.ErrInvalid
	}
	p.Hash = p.ContentHash()
	return p, nil
}
