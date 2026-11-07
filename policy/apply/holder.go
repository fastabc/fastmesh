package apply

import (
	"sync/atomic"

	"github.com/fastabc/fastmesh/policy/snapshot"
)

// Holder stores the current applied snapshot behind an atomic pointer.
type Holder struct {
	current atomic.Pointer[snapshot.Snapshot]
}

// NewHolder returns an empty holder.
func NewHolder() *Holder { return &Holder{} }

// Store atomically installs s as the live snapshot.
func (h *Holder) Store(s *snapshot.Snapshot) { h.current.Store(s) }

// Load returns the live snapshot, or nil if nothing has been applied yet.
func (h *Holder) Load() *snapshot.Snapshot { return h.current.Load() }

// Revision returns the revision of the live snapshot, or empty if none.
func (h *Holder) Revision() snapshot.Revision {
	if s := h.current.Load(); s != nil {
		return s.Revision
	}
	return ""
}
