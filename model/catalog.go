package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"time"

	"github.com/fastabc/fastmesh/store/key"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

const (
	WorkloadGroupKind  = "mesh.WorkloadGroup"
	ServiceKind        = "mesh.Service"
	EndpointKind       = "mesh.Endpoint"
	BindGrantKind      = "mesh.BindGrant"
	ServiceAccessKind  = "mesh.ServiceAccess"
	CatalogStatusOwner = "mesh-catalog"
)

var ErrInvalid = errors.New("mesh: invalid catalog object")

type ServicePort struct {
	Name     string `json:"name"`
	Number   uint16 `json:"number"`
	Protocol string `json:"protocol"`
}

type Service struct {
	Tenant     string        `json:"tenant"`
	Name       string        `json:"name"`
	Ports      []ServicePort `json:"ports"`
	RequiresL7 bool          `json:"requires_l7,omitempty"`
}

type HTTPMatch struct {
	Method     string   `json:"method,omitempty"`
	Methods    []string `json:"methods,omitempty"`
	Path       string   `json:"path,omitempty"`
	PathPrefix string   `json:"path_prefix,omitempty"`
}

// ServiceAccess is one exact allow. Empty Port covers all service ports;
// empty HTTP match fields cover every method or path at an HTTP enforcement point.
type ServiceAccess struct {
	Tenant  string     `json:"tenant"`
	Source  Principal  `json:"source"`
	Service string     `json:"service"`
	Port    string     `json:"port,omitempty"`
	L7      *HTTPMatch `json:"l7,omitempty"`
}

type BindGrant struct {
	Tenant   string    `json:"tenant"`
	Service  string    `json:"service"`
	Workload Principal `json:"workload"`
}

type Endpoint struct {
	Tenant         string         `json:"tenant"`
	Service        string         `json:"service"`
	Port           string         `json:"port"`
	InstanceID     string         `json:"instance_id"`
	Owner          spiffeid.ID    `json:"owner"`
	Address        netip.AddrPort `json:"address"`
	Ready          bool           `json:"ready"`
	Serving        bool           `json:"serving,omitempty"`
	Terminating    bool           `json:"terminating,omitempty"`
	LeaseExpiresAt time.Time      `json:"lease_expires_at"`
	Generation     uint64         `json:"generation"`
}

// EndpointRecord owns one instance's lease and generation fence. A tombstone
// retains a generation fence until its retention expires.
type EndpointRecord struct {
	NodeID      string    `json:"node_id,omitempty"`
	Endpoint    Endpoint  `json:"endpoint"`
	RemovedAt   time.Time `json:"removed_at,omitempty"`
	RetainUntil time.Time `json:"retain_until,omitempty"`
}

func EndpointKey(tenant, service string, owner spiffeid.ID, instance string) key.Key {
	hash := sha256.Sum256([]byte(owner.String() + "\n" + instance))
	return key.Key{Tenant: tenant, Kind: EndpointKind, Name: service + "-" + hex.EncodeToString(hash[:])}
}

// Session is established by controller admission from an enrolled node and
// its authenticated mTLS peer. It must never be decoded from an RPC body.
type Session struct {
	Tenant   string
	NodeID   string
	Workload spiffeid.ID
}

type ServiceStatus struct {
	Endpoints []Endpoint `json:"endpoints"`
}

type ServiceRecord struct {
	Spec   Service       `json:"spec"`
	Status ServiceStatus `json:"status"`
}

func ExactName(s string) bool {
	return s != "" && !strings.ContainsAny(s, "*? /\\\t\r\n")
}

func (s Service) Validate() error {
	if !ExactName(s.Tenant) || !ExactName(s.Name) || len(s.Ports) == 0 {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, p := range s.Ports {
		if !ExactName(p.Name) || seen[p.Name] || (p.Protocol != "tcp" && p.Protocol != "http" && p.Protocol != "grpc") || (s.RequiresL7 && p.Protocol != "http" && p.Protocol != "grpc") {
			return ErrInvalid
		}
		seen[p.Name] = true
	}
	return nil
}

func (s Service) Port(name string) (ServicePort, bool) {
	for _, p := range s.Ports {
		if p.Name == name {
			return p, true
		}
	}
	return ServicePort{}, false
}

func (g BindGrant) Validate() error {
	if !ExactName(g.Tenant) || !ExactName(g.Service) || !g.Workload.Valid() {
		return ErrInvalid
	}
	return nil
}

func (a ServiceAccess) Validate() error {
	if !ExactName(a.Tenant) || !ExactName(a.Service) || !a.Source.Valid() || (a.Port != "" && !ExactName(a.Port)) {
		return ErrInvalid
	}
	if a.L7 != nil && !a.L7.Valid() {
		return ErrInvalid
	}

	return nil
}

// Principal is exactly one authenticated ID or an administrator-owned group.
// Exact IDs retain their historical JSON string encoding.
type Principal struct {
	ID    spiffeid.ID `json:"id,omitempty"`
	Group string      `json:"group,omitempty"`
}

func (p Principal) Valid() bool {
	return (!p.ID.IsZero() && p.Group == "" && !strings.ContainsAny(p.ID.String(), "*?")) || (p.ID.IsZero() && ExactName(p.Group))
}
func (p Principal) MarshalJSON() ([]byte, error) {
	if p.Group == "" {
		return json.Marshal(p.ID)
	}
	type plain Principal
	return json.Marshal(plain(p))
}
func (p *Principal) UnmarshalJSON(b []byte) error {
	*p = Principal{}
	b = bytes.TrimSpace(b)
	if len(b) > 0 && b[0] == '"' {
		return json.Unmarshal(b, &p.ID)
	}
	type plain Principal
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	return decoder.Decode((*plain)(p))
}
func (p Principal) String() string { return p.ID.String() }

type WorkloadGroup struct {
	Tenant  string        `json:"tenant"`
	Name    string        `json:"name"`
	Members []spiffeid.ID `json:"members"`
}

func (g WorkloadGroup) Validate() error {
	if !ExactName(g.Tenant) || !ExactName(g.Name) {
		return ErrInvalid
	}
	seen := map[spiffeid.ID]bool{}
	for _, id := range g.Members {
		if id.IsZero() || strings.ContainsAny(id.String(), "*?") || seen[id] {
			return ErrInvalid
		}
		seen[id] = true
	}
	return nil
}
func (m HTTPMatch) Valid() bool {
	if m.Method != "" && len(m.Methods) > 0 || m.Path != "" && m.PathPrefix != "" {
		return false
	}
	for _, method := range append(append([]string{}, m.Methods...), m.Method) {
		if strings.ContainsAny(method, "*? \t\r\n") {
			return false
		}
	}
	for _, path := range []string{m.Path, m.PathPrefix} {
		if path != "" && (!CanonicalRequestPath(path, "") || strings.ContainsAny(path, "*?\r\n")) {
			return false
		}
	}
	for _, method := range m.Methods {
		if method == "" {
			return false
		}
	}
	return true
}
func (m HTTPMatch) Matches(method, path string) bool {
	if !m.Valid() {
		return false
	}
	methods := m.Methods
	if m.Method != "" {
		methods = []string{m.Method}
	}
	allowed := len(methods) == 0
	for _, v := range methods {
		if v == method {
			allowed = true
		}
	}
	if !allowed || (m.Path != "" && m.Path != path) {
		return false
	}
	if prefix := m.PathPrefix; prefix != "" {
		if strings.HasSuffix(prefix, "/") {
			return strings.HasPrefix(path, prefix)
		}
		return path == prefix || strings.HasPrefix(path, prefix+"/")
	}
	return true
}
