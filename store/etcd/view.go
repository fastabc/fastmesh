package etcd

import (
	"context"
	"sort"

	"github.com/fastabc/fastmesh/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ store.Viewer = (*Store)(nil)

// View pins every range after the first to that first read's revision. If it
// is compacted during this operation, fail the entire view rather than mix
// revisions. Callers can retry a fresh View.
func (s *Store) View(ctx context.Context, ranges []store.Range) (store.View, error) {
	view := store.View{Objects: make(map[store.Range][]store.Object, len(ranges))}
	if len(ranges) == 0 {
		resp, err := s.client.Get(ctx, s.objectListPrefix("", ""), clientv3.WithPrefix(), clientv3.WithLimit(1))
		if err != nil {
			return store.View{}, err
		}
		view.Revision = resp.Header.Revision
		return view, nil
	}
	for _, r := range ranges {
		opts := []clientv3.OpOption{clientv3.WithPrefix()}
		if view.Revision != 0 {
			opts = append(opts, clientv3.WithRev(view.Revision))
		}
		resp, err := s.client.Get(ctx, s.objectListPrefix(r.Tenant, r.Kind), opts...)
		if err != nil {
			return store.View{}, err
		}
		if view.Revision == 0 {
			view.Revision = resp.Header.Revision
		}
		objects := []store.Object{}
		for _, kv := range resp.Kvs {
			obj, err := decodeObjectKV(kv)
			if err != nil {
				return store.View{}, err
			}
			if objectMatches(obj, r.Tenant, store.NewKindFilter([]string{r.Kind})) {
				objects = append(objects, obj)
			}
		}
		sort.Slice(objects, func(i, j int) bool {
			a, b := objects[i].Key, objects[j].Key
			if a.Tenant != b.Tenant {
				return a.Tenant < b.Tenant
			}
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			return a.Name < b.Name
		})
		view.Objects[r] = objects
	}
	return view, nil
}
