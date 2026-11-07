package snapshot

import (
	"time"

	"github.com/fastabc/fastmesh/model"
)

const SchemaVersion = "1.0"

// Revision identifies a desired runtime state produced by the controller.
type Revision string

// ResourceType is the stable wire name for an object carried in a snapshot.
type ResourceType string

// ResourceRef is the identity of one resource inside a tenant snapshot.
type ResourceRef struct {
	Tenant string       `json:"tenant"`
	Type   ResourceType `json:"type"`
	Name   string       `json:"name"`
}

// Resource is an opaque desired-state object. Data stays raw so the snapshot
// contract does not import public typed resource implementations.
type Resource struct {
	Ref       ResourceRef       `json:"ref"`
	Version   string            `json:"version,omitempty"`
	Hash      string            `json:"hash,omitempty"`
	Data      []byte            `json:"data,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	DependsOn []ResourceRef     `json:"depends_on,omitempty"`
}

// Snapshot is the full desired state for one node and tenant.
type Snapshot struct {
	// MeshPlan is local composition state for the single apply/journal path.
	// It travels over ADS only in its independent fastmesh.plan.v1 envelope,
	// never as a host snapshot resource. CanonicalHash retains its v1 rules.
	MeshPlan *model.Plan `json:"mesh_plan,omitempty"`
	// MeshPlanEnvelope preserves exactly the authenticated bytes, including
	// fields unknown to this binary, through journal and last-good recovery.
	MeshPlanEnvelope   *SignedMeshPlan `json:"mesh_plan_envelope,omitempty"`
	Tenant             string          `json:"tenant"`
	NodeID             string          `json:"node_id"`
	AssignmentID       string          `json:"assignment_id"`
	Revision           Revision        `json:"revision"`
	GeneratedAt        time.Time       `json:"generated_at"`
	InstallFingerprint string          `json:"install_fingerprint,omitempty"`
	RoutingFingerprint string          `json:"routing_fingerprint,omitempty"`
	Resources          []Resource      `json:"resources,omitempty"`
}

type SignedMeshPlan struct {
	TypeURL   string    `json:"type_url"`
	Body      []byte    `json:"body"`
	Signature Signature `json:"signature"`
}
