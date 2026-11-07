package expiry

import (
	"container/heap"
	"encoding/json"
	"sync"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

type entry struct {
	object   store.Object
	deadline time.Time
}
type queue []entry

func (q queue) Len() int           { return len(q) }
func (q queue) Less(i, j int) bool { return q[i].deadline.Before(q[j].deadline) }
func (q queue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(v any)        { *q = append(*q, v.(entry)) }
func (q *queue) Pop() any          { old := *q; v := old[len(old)-1]; *q = old[:len(old)-1]; return v }

type Schedule struct {
	NodeTTL  time.Duration
	mu       sync.Mutex
	pending  queue
	versions map[store.ObjectKey]int64
}

func (s *Schedule) Reset(objects []store.Object) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = nil
	s.versions = map[store.ObjectKey]int64{}
	for _, obj := range objects {
		s.put(obj)
	}
}
func (s *Schedule) put(obj store.Object) {
	at, ok := catalog.EndpointDeadline(obj)
	if obj.Key.Kind == model.NodeKind && s.NodeTTL > 0 {
		var n model.Node
		if json.Unmarshal(obj.Data, &n) == nil && n.Ephemeral {
			last := n.Status.LastContact
			if last.IsZero() {
				last = n.Status.JoinedAt
			}
			if !last.IsZero() {
				at, ok = last.Add(s.NodeTTL), true
			}
		}
	}
	if ok {
		s.versions[obj.Key] = obj.ResourceVersion
		heap.Push(&s.pending, entry{obj, at})
	} else {
		delete(s.versions, obj.Key)
	}
}
func (s *Schedule) Apply(events []store.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.versions == nil {
		s.versions = map[store.ObjectKey]int64{}
	}
	for _, e := range events {
		if e.Type == store.EventDelete {
			delete(s.versions, e.Object.Key)
		} else {
			s.put(e.Object)
		}
	}
}
func (s *Schedule) Due(now time.Time) []store.Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.Object
	for len(s.pending) > 0 && !now.Before(s.pending[0].deadline) {
		e := heap.Pop(&s.pending).(entry)
		if s.versions[e.object.Key] == e.object.ResourceVersion {
			delete(s.versions, e.object.Key)
			out = append(out, e.object)
		}
	}
	return out
}

// Retry retains a failed task until reconciliation can try it again. A newer
// watch update supersedes it, so an old task cannot expire a renewed instance.
func (s *Schedule) Retry(obj store.Object) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version := s.versions[obj.Key]; version > obj.ResourceVersion {
		return
	}
	s.versions[obj.Key] = obj.ResourceVersion
	heap.Push(&s.pending, entry{obj, time.Now()})
}
