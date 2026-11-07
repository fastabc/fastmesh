package model

import (
	"time"

	"github.com/fastabc/fastmesh/store/key"
)

const NodeKind = "Node"

const (
	IntentActive   = "active"
	IntentDisabled = "disabled"
)

// DefaultTenant is the default namespace; SystemTenant owns control-plane records.
const (
	DefaultTenant = "default"
	SystemTenant  = "system"
)
const NodeStatusOwner = "ads-controller"

// CellLabel is the managed label selecting a controller cell. It is separate
// from operator-defined region labels used for workload placement.
const CellLabel = "fastmesh.io/cell"

// Node is a registered agent with desired intent and reported status.
type Node struct {
	Ephemeral     bool              `json:"ephemeral,omitempty"`
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Tenant        string            `json:"tenant"`
	SpiffeID      string            `json:"spiffe_id,omitempty"`
	ManagedLabels map[string]string `json:"managed_labels,omitempty"`
	Intent        string            `json:"intent,omitempty"`
	Generation    uint64            `json:"generation,omitempty"`
	Status        NodeStatus        `json:"status,omitempty"`
}

type NodeStatus struct {
	LastContact          time.Time         `json:"last_contact,omitempty"`
	ReportedLabels       map[string]string `json:"reported_labels,omitempty"`
	ReportedCapabilities []string          `json:"reported_capabilities,omitempty"`
	AgentVersion         string            `json:"agent_version,omitempty"`
	Runtime              string            `json:"runtime,omitempty"`
	JoinedAt             time.Time         `json:"joined_at,omitempty"`
	ObservedIntent       string            `json:"observed_intent,omitempty"`
	ObservedGeneration   uint64            `json:"observed_generation,omitempty"`
	ObservedAt           time.Time         `json:"observed_at,omitempty"`
}

// ApplyNodeIntent applies the operator-owned part of a Node patch without
// disturbing registration or observed status fields.
func ApplyNodeIntent(node Node, intent string, managedLabels map[string]string) Node {
	if intent != "" {
		node.Intent = intent
	}
	if managedLabels != nil {
		node.ManagedLabels = make(map[string]string, len(managedLabels))
		for key, value := range managedLabels {
			node.ManagedLabels[key] = value
		}
	}
	node.Generation++
	return node
}

// NodeKey identifies an enrolled-node record.
func NodeKey(nodeID string) key.Key {
	return key.Key{Tenant: SystemTenant, Kind: NodeKind, Name: nodeID}
}
