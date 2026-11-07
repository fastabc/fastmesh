// Package key defines the dependency-free identity of a stored object.
package key

// Key identifies an object within a tenant and resource kind.
type Key struct {
	Tenant string
	Kind   string
	Name   string
}
