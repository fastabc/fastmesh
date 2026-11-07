package node

import (
	"sort"
	"time"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// BindingStatus describes the last confirmed directory lease, not remote health.
type BindingStatus struct {
	Serving        bool
	LeaseExpiresAt time.Time
	LeaseValid     bool
}

type Status struct {
	NodeID                string
	Workload              spiffeid.ID
	InputRevision         int64
	TopologyHash          string
	NotAfter              time.Time
	Bound                 []ServiceRef
	Dialable              map[ServiceRef]int
	LastControllerContact time.Time
	ControlState          string
	LastApplyFailure      string
	Bindings              map[ServiceRef]BindingStatus
}

func (n *Node) Status() Status {
	n.mu.Lock()
	s := Status{NodeID: n.nodeID, LastControllerContact: n.lastContact, Dialable: map[ServiceRef]int{}, ControlState: n.controlState, LastApplyFailure: n.lastApplyFailure, Bindings: map[ServiceRef]BindingStatus{}}
	if s.ControlState == "" {
		s.ControlState = "not_started"
	}
	if n.closed {
		s.ControlState = "closed"
	}
	for ref, b := range n.bindings {
		if b != nil {
			s.Bound = append(s.Bound, ref)
			bs := b.status
			bs.LeaseValid = time.Now().Before(bs.LeaseExpiresAt)
			s.Bindings[ref] = bs
		}
	}
	n.mu.Unlock()
	sort.Slice(s.Bound, func(i, j int) bool {
		if s.Bound[i].Service == s.Bound[j].Service {
			return s.Bound[i].Port < s.Bound[j].Port
		}
		return s.Bound[i].Service < s.Bound[j].Service
	})
	p := n.runtime.CurrentPlan()
	if p == nil {
		return s
	}
	s.Workload, s.InputRevision, s.TopologyHash, s.NotAfter = p.Workload, p.InputRevision, p.Hash, p.NotAfter
	if !dataplane.Fresh(p) {
		return s
	}
	s.Dialable = n.runtime.DialableCounts(p, time.Now())
	return s
}
