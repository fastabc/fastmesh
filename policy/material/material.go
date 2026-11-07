// Package material defines the encrypted journal material representation.
package material

import "time"

// SecretRef is an opaque pointer to material stored outside the communication kernel.
type SecretRef struct {
	Provider string            `json:"provider"`
	Tenant   string            `json:"tenant,omitempty"`
	Name     string            `json:"name"`
	Version  string            `json:"version,omitempty"`
	Path     string            `json:"path,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Plaintext is resolved secret material. Callers must keep it in memory only.
type Plaintext struct {
	Value       []byte
	ContentType string
	ExpiresAt   time.Time
}

// ResolvedMaterial binds one immutable SecretRef to the plaintext that was
// used while preparing a snapshot. It exists only in agent memory except when
// the state store encrypts it as part of the atomic last-good bundle.
type ResolvedMaterial struct {
	Ref       SecretRef
	Plaintext Plaintext
}

// ClearResolvedMaterials overwrites plaintext byte slices once a temporary
// material copy is no longer needed.
func ClearResolvedMaterials(materials []ResolvedMaterial) {
	for i := range materials {
		for j := range materials[i].Plaintext.Value {
			materials[i].Plaintext.Value[j] = 0
		}
	}
}
