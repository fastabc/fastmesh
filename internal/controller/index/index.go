// Package index materializes complete store revisions for the mesh compiler.
package index

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/expiry"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

type Snapshot struct {
	Revision int64
	Tenant   string
	objects  map[store.ObjectKey]store.Object
	grants   map[store.ObjectKey]model.BindGrant
	groups   map[string]model.WorkloadGroup
	once     sync.Once
	decoded  catalog.Catalog
	err      error
}

// CanBind uses grants decoded at publication, independently of lease revisions.
// Renewal must not decode every service and endpoint in the tenant.
func (s *Snapshot) CanBind(service, workload string) bool {
	for _, g := range s.grants {
		if g.Service != service {
			continue
		}
		if g.Workload.ID.String() == workload {
			return true
		}
		for _, id := range s.groups[g.Workload.Group].Members {
			if id.String() == workload {
				return true
			}
		}
	}
	return false
}

func (s *Snapshot) Catalog() (catalog.Catalog, error) {
	s.once.Do(func() { s.decoded, s.err = catalog.Decode(s.View(), s.Tenant) })
	return s.decoded, s.err
}
func (s *Snapshot) View() store.View {
	v := store.View{Revision: s.Revision, Objects: map[store.Range][]store.Object{}}
	for _, r := range catalog.Ranges(s.Tenant) {
		v.Objects[r] = []store.Object{}
	}
	for _, obj := range s.objects {
		r := store.Range{Tenant: s.Tenant, Kind: obj.Key.Kind}
		if _, ok := v.Objects[r]; ok {
			v.Objects[r] = append(v.Objects[r], cloneObject(obj))
		}
	}
	for r := range v.Objects {
		sort.Slice(v.Objects[r], func(i, j int) bool { return v.Objects[r][i].Key.Name < v.Objects[r][j].Key.Name })
	}
	return v
}

type Index struct {
	objects     store.RevisionWatcher
	viewer      store.Viewer
	mu          sync.Mutex
	snapshots   map[string]*Snapshot
	nodes       map[store.ObjectKey]store.Object
	revision    int64
	available   bool
	changed     chan struct{}
	subscribers map[string]map[chan struct{}]struct{}
	Expiry      expiry.Schedule
}

func New(objects store.RevisionWatcher, viewer store.Viewer) *Index {
	return &Index{objects: objects, viewer: viewer, snapshots: map[string]*Snapshot{}, changed: make(chan struct{}), subscribers: map[string]map[chan struct{}]struct{}{}}
}
func (i *Index) Snapshot(ctx context.Context, tenant string) (*Snapshot, error) {
	for {
		i.mu.Lock()
		if i.available {
			s := i.snapshots[tenant]
			if s == nil {
				s = &Snapshot{Tenant: tenant, Revision: i.revision, objects: map[store.ObjectKey]store.Object{}}
				s.loadGroups()
				i.snapshots[tenant] = s
			}
			i.mu.Unlock()
			return s, nil
		}
		changed := i.changed
		i.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}
func (i *Index) Subscribe(ctx context.Context, tenant string) (<-chan struct{}, error) {
	if _, err := i.Snapshot(ctx, tenant); err != nil {
		return nil, err
	}
	out := make(chan struct{}, 1)
	i.mu.Lock()
	if i.subscribers[tenant] == nil {
		i.subscribers[tenant] = map[chan struct{}]struct{}{}
	}
	i.subscribers[tenant][out] = struct{}{}
	i.mu.Unlock()
	go func() { <-ctx.Done(); i.mu.Lock(); delete(i.subscribers[tenant], out); close(out); i.mu.Unlock() }()
	return out, nil
}
func (i *Index) notify(tenant string) {
	for ch := range i.subscribers[tenant] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
func (i *Index) signal() { close(i.changed); i.changed = make(chan struct{}) }
func cloneObject(obj store.Object) store.Object {
	obj.Data = append([]byte(nil), obj.Data...)
	if obj.Labels != nil {
		labels := map[string]string{}
		for k, v := range obj.Labels {
			labels[k] = v
		}
		obj.Labels = labels
	}
	return obj
}
func (i *Index) reset(v store.View) {
	i.mu.Lock()
	defer i.mu.Unlock()
	next := map[string]*Snapshot{}
	var all []store.Object
	i.nodes = map[store.ObjectKey]store.Object{}
	for r, objects := range v.Objects {
		if r.Kind == model.NodeKind {
			for _, obj := range objects {
				i.nodes[obj.Key] = cloneObject(obj)
			}
			all = append(all, objects...)
			continue
		}
		for _, obj := range objects {
			s := next[obj.Key.Tenant]
			if s == nil {
				s = &Snapshot{Tenant: obj.Key.Tenant, Revision: v.Revision, objects: map[store.ObjectKey]store.Object{}, grants: map[store.ObjectKey]model.BindGrant{}}
				next[obj.Key.Tenant] = s
			}
			obj = cloneObject(obj)
			s.objects[obj.Key] = obj
			if obj.Key.Kind == model.BindGrantKind {
				if grant, err := catalog.DecodeBindGrant(obj); err == nil {
					s.grants[obj.Key] = grant
				}
			}
			all = append(all, obj)
		}
	}
	for _, s := range next {
		s.loadGroups()
	}
	i.snapshots = next
	i.revision = v.Revision
	i.available = true
	i.Expiry.Reset(all)
	i.signal()
	for tenant := range i.subscribers {
		i.notify(tenant)
	}
}
func (i *Index) apply(batch store.RevisionBatch) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if batch.Revision <= i.revision {
		return
	}
	next := map[string]*Snapshot{}
	notify := map[string]bool{}
	grantsCopied := map[string]bool{}
	for _, e := range batch.Events {
		if e.Object.Key.Kind == model.NodeKind {
			previous := i.nodes[e.Object.Key]
			object := e.Object
			if e.Type == store.EventDelete {
				object = previous
				delete(i.nodes, e.Object.Key)
			} else {
				if i.nodes == nil {
					i.nodes = map[store.ObjectKey]store.Object{}
				}
				i.nodes[e.Object.Key] = cloneObject(object)
			}
			tenant, changed := nodeNotify(previous, object)
			if changed || e.Type == store.EventDelete {
				if tenant == "" {
					for name := range i.subscribers {
						notify[name] = true
					}
				} else {
					notify[tenant] = true
					if oldTenant, _ := nodeNotify(store.Object{}, previous); oldTenant != "" {
						notify[oldTenant] = true
					}
				}
			}
			continue
		}
		tenant := e.Object.Key.Tenant
		s := next[tenant]
		if s == nil {
			s = &Snapshot{Tenant: tenant, Revision: batch.Revision, objects: map[store.ObjectKey]store.Object{}}
			if old := i.snapshots[tenant]; old != nil {
				s.grants = old.grants
				s.groups = old.groups
				for key, obj := range old.objects {
					s.objects[key] = obj
				}
			}
			next[tenant] = s
		}
		old := s.objects[e.Object.Key]
		if e.Object.Key.Kind == model.BindGrantKind {
			if !grantsCopied[tenant] {
				grants := make(map[store.ObjectKey]model.BindGrant, len(s.grants))
				for key, grant := range s.grants {
					grants[key] = grant
				}
				s.grants = grants
				grantsCopied[tenant] = true
			}
			delete(s.grants, e.Object.Key)
			if e.Type != store.EventDelete {
				if grant, err := catalog.DecodeBindGrant(e.Object); err == nil {
					s.grants[e.Object.Key] = grant
				}
			}
		}
		if e.Type == store.EventDelete {
			delete(s.objects, e.Object.Key)
		} else {
			s.objects[e.Object.Key] = cloneObject(e.Object)
		}
		if e.Type == store.EventDelete || catalog.TopologyChanged(old, e.Object) {
			notify[tenant] = true
		}
	}
	for tenant, s := range next {
		if s.groups == nil || groupChanged(batch.Events, tenant) {
			s.loadGroups()
		}
		i.snapshots[tenant] = s
	}
	i.revision = batch.Revision
	i.Expiry.Apply(batch.Events)
	for tenant := range notify {
		i.notify(tenant)
	}
}
func (i *Index) invalidate() { i.mu.Lock(); i.available = false; i.signal(); i.mu.Unlock() }
func (i *Index) Run(ctx context.Context) error {
	kinds := []string{}
	for _, r := range catalog.Ranges("") {
		kinds = append(kinds, r.Kind)
	}
	kinds = append(kinds, model.NodeKind)
	backoff := 10 * time.Millisecond
	for ctx.Err() == nil {
		watchCtx, cancel := context.WithCancel(ctx)
		var batches <-chan store.RevisionBatch
		var err error
		batches, err = i.objects.WatchRevisions(watchCtx, "", kinds...)
		if err == nil {
			var v store.View
			ranges := catalog.Ranges("")
			ranges = append(ranges, store.Range{Kind: model.NodeKind})
			v, err = i.viewer.View(watchCtx, ranges)
			if err == nil {
				i.reset(v)
				backoff = 10 * time.Millisecond
				err = i.consume(watchCtx, batches)
			}
		}
		cancel()
		i.invalidate()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, time.Second)
	}
	return ctx.Err()
}
func (i *Index) consume(ctx context.Context, batches <-chan store.RevisionBatch) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-batches:
			if !ok {
				return errors.New("index: revision watch closed")
			}
			i.apply(batch)
		}
	}
}

func (s *Snapshot) loadGroups() {
	s.groups = map[string]model.WorkloadGroup{}
	for _, obj := range s.objects {
		if obj.Key.Kind == model.WorkloadGroupKind {
			if g, err := catalog.DecodeWorkloadGroup(obj); err == nil {
				s.groups[g.Name] = g
			}
		}
	}
}
func groupChanged(events []store.Event, tenant string) bool {
	for _, e := range events {
		if e.Object.Key.Tenant == tenant && e.Object.Key.Kind == model.WorkloadGroupKind {
			return true
		}
	}
	return false
}
