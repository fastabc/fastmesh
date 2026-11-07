package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

const PlanTypeURL = "fastmesh.plan.v1"
const PlanCapability = "mesh.plan.v1"

type InboundAllow struct {
	Source spiffeid.ID `json:"source"`
	L7     *HTTPMatch  `json:"l7,omitempty"`
}

// An inbound entry also records permission to bind, including an empty allow
// set. An empty set means deny every caller, not missing policy.
type InboundService struct {
	Service    string         `json:"service"`
	Port       ServicePort    `json:"port"`
	RequiresL7 bool           `json:"requires_l7,omitempty"`
	Allow      []InboundAllow `json:"allow"`
}

type OutboundService struct {
	Service    string        `json:"service"`
	Port       ServicePort   `json:"port"`
	RequiresL7 bool          `json:"requires_l7,omitempty"`
	Owners     []spiffeid.ID `json:"owners,omitempty"`
	Endpoints  []Endpoint    `json:"endpoints"`
}

type Plan struct {
	// TypeURL records the authenticated wire contract for local persistence.
	// Only the current wire contract is accepted.
	TypeURL string `json:"wire_type,omitempty"`
	// RestoreNotAfter bounds execution recovered from a historical signer.
	// The authenticated body and its original deadline remain unchanged.
	RestoreNotAfter time.Time         `json:"-"`
	Tenant          string            `json:"tenant"`
	Workload        spiffeid.ID       `json:"workload"`
	NodeID          string            `json:"node_id"`
	InputRevision   int64             `json:"input_revision"`
	NotAfter        time.Time         `json:"not_after"`
	Inbound         []InboundService  `json:"inbound"`
	Outbound        []OutboundService `json:"outbound"`
	AccessSet       bool              `json:"access_set"`
	EndpointsSet    bool              `json:"endpoints_set"`
	Hash            string            `json:"hash"`
}

func (p Plan) IsCurrent() bool { return p.TypeURL == PlanTypeURL }

// InputValid validates local signed input. Topology hashes are opaque declarations
// authenticated with the original body and must never be reconstructed.
func (p Plan) InputValid() bool {
	return p.EnvelopeValid() && p.IsCurrent() && p.Hash != ""
}

type PlanHeader struct {
	Tenant        string      `json:"tenant"`
	NodeID        string      `json:"node_id"`
	Workload      spiffeid.ID `json:"workload"`
	InputRevision int64       `json:"input_revision"`
	NotAfter      time.Time   `json:"not_after"`
	TopologyHash  string      `json:"topology_hash"`
}

type PlanTopology struct {
	Inbound  []InboundService  `json:"inbound"`
	Outbound []OutboundService `json:"outbound"`
}

// WirePlan is the raw-body wire contract; Plan remains the execution model.
type WirePlan struct {
	Header   PlanHeader   `json:"header"`
	Topology PlanTopology `json:"topology"`
}

func (p Plan) Wire() WirePlan {
	return WirePlan{Header: PlanHeader{Tenant: p.Tenant, NodeID: p.NodeID, Workload: p.Workload, InputRevision: p.InputRevision, NotAfter: p.NotAfter, TopologyHash: p.Hash}, Topology: PlanTopology{Inbound: p.Inbound, Outbound: p.Outbound}}
}

func (p WirePlan) Plan() Plan {
	return Plan{TypeURL: PlanTypeURL, Tenant: p.Header.Tenant, NodeID: p.Header.NodeID, Workload: p.Header.Workload, InputRevision: p.Header.InputRevision, NotAfter: p.Header.NotAfter, Hash: p.Header.TopologyHash, Inbound: p.Topology.Inbound, Outbound: p.Topology.Outbound, AccessSet: true, EndpointsSet: true}
}

// EnvelopeValid checks the authenticated plan header. Execution entries are
// checked independently after verification so future protocols fail closed.
func (p Plan) EnvelopeValid() bool {
	return p.AccessSet && p.EndpointsSet && ExactName(p.Tenant) && p.NodeID != "" && !p.Workload.IsZero() && p.InputRevision >= 0 && !p.NotAfter.IsZero()
}

func (p Plan) Valid() bool {
	if !p.EnvelopeValid() {
		return false
	}
	seen := map[string]bool{}
	for _, in := range p.Inbound {
		key := in.Service + "/" + in.Port.Name
		if seen[key] || (Service{Tenant: p.Tenant, Name: in.Service, Ports: []ServicePort{in.Port}, RequiresL7: in.RequiresL7}).Validate() != nil {
			return false
		}
		seen[key] = true
		for _, a := range in.Allow {
			if (ServiceAccess{Tenant: p.Tenant, Service: in.Service, Port: in.Port.Name, Source: Principal{ID: a.Source}, L7: a.L7}).Validate() != nil {
				return false
			}
		}
	}
	seen = map[string]bool{}
	for _, out := range p.Outbound {
		key := out.Service + "/" + out.Port.Name
		if seen[key] || (Service{Tenant: p.Tenant, Name: out.Service, Ports: []ServicePort{out.Port}, RequiresL7: out.RequiresL7}).Validate() != nil {
			return false
		}
		seen[key] = true
		for _, owner := range out.Owners {
			if owner.IsZero() {
				return false
			}
		}
		instances := map[string]bool{}
		for _, e := range out.Endpoints {
			instance := e.Owner.String() + "/" + e.InstanceID
			if instances[instance] || e.Tenant != p.Tenant || e.Service != out.Service || e.Port != out.Port.Name || e.Owner.IsZero() || !ExactName(e.InstanceID) || e.Generation == 0 || !(e.Ready || e.Serving) || !e.Address.IsValid() || e.Address.Port() == 0 || e.Address.Addr().IsUnspecified() || e.Address.Addr().IsMulticast() || e.LeaseExpiresAt.IsZero() {
				return false
			}
			instances[instance] = true
		}
	}
	return true
}

// ContentHash excludes deadlines and the input watermark. These still belong
// to the signed payload. Renewing validity does not change runtime topology.
func (p Plan) ContentHash() string {
	p.Hash = ""
	p.NotAfter = time.Time{}
	p.InputRevision = 0
	p.Inbound = append([]InboundService{}, p.Inbound...)
	p.Outbound = append([]OutboundService{}, p.Outbound...)
	for i := range p.Inbound {
		p.Inbound[i].Allow = append([]InboundAllow{}, p.Inbound[i].Allow...)
		sort.Slice(p.Inbound[i].Allow, func(a, b int) bool { return allowKey(p.Inbound[i].Allow[a]) < allowKey(p.Inbound[i].Allow[b]) })
	}
	for i := range p.Outbound {
		p.Outbound[i].Owners = append([]spiffeid.ID(nil), p.Outbound[i].Owners...)
		sort.Slice(p.Outbound[i].Owners, func(a, b int) bool { return p.Outbound[i].Owners[a].String() < p.Outbound[i].Owners[b].String() })
		p.Outbound[i].Endpoints = append([]Endpoint{}, p.Outbound[i].Endpoints...)
		for j := range p.Outbound[i].Endpoints {
			p.Outbound[i].Endpoints[j].LeaseExpiresAt = time.Time{}
		}
		sort.Slice(p.Outbound[i].Endpoints, func(a, b int) bool {
			x, y := p.Outbound[i].Endpoints[a], p.Outbound[i].Endpoints[b]
			return x.Owner.String()+"/"+x.InstanceID < y.Owner.String()+"/"+y.InstanceID
		})
	}
	sort.Slice(p.Inbound, func(i, j int) bool {
		return p.Inbound[i].Service+"/"+p.Inbound[i].Port.Name < p.Inbound[j].Service+"/"+p.Inbound[j].Port.Name
	})
	sort.Slice(p.Outbound, func(i, j int) bool {
		return p.Outbound[i].Service+"/"+p.Outbound[i].Port.Name < p.Outbound[j].Service+"/"+p.Outbound[j].Port.Name
	})
	data, _ := json.Marshal(p)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func allowKey(a InboundAllow) string {
	if a.L7 == nil {
		return a.Source.String()
	}
	key := a.Source.String() + "\n" + a.L7.Method + "\n" + a.L7.Path
	if len(a.L7.Methods) > 0 || a.L7.PathPrefix != "" {
		b, _ := json.Marshal(a.L7)
		key += "\n" + string(b)
	}
	return key
}
