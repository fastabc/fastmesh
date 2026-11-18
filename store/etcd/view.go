package etcd

import (
	"context"
	"sort"

	"github.com/fastabc/fastmesh/internal/storeutil"
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
	// Several logical kinds share one physical prefix in a cross-tenant view.
	groups := make(map[string][]store.Range)
	var prefixes []string
	for _, r := range ranges {
		if _, ok := view.Objects[r]; ok {
			continue
		}
		view.Objects[r] = []store.Object{}
		prefix := s.objectListPrefix(r.Tenant, r.Kind)
		if len(groups[prefix]) == 0 {
			prefixes = append(prefixes, prefix)
		}
		groups[prefix] = append(groups[prefix], r)
	}
	for _, prefix := range prefixes {
		opts := []clientv3.OpOption{clientv3.WithPrefix()}
		if view.Revision != 0 {
			opts = append(opts, clientv3.WithRev(view.Revision))
		}
		resp, err := s.client.Get(ctx, prefix, opts...)
		if err != nil {
			return store.View{}, err
		}
		if view.Revision == 0 {
			view.Revision = resp.Header.Revision
		}
		for _, kv := range resp.Kvs {
			obj, err := decodeObjectKV(kv)
			if err != nil {
				return store.View{}, err
			}
			assigned := false
			for _, r := range groups[prefix] {
				if (r.Tenant != "" && r.Tenant != obj.Key.Tenant) || (r.Kind != "" && r.Kind != obj.Key.Kind) {
					continue
				}
				owned := obj
				if assigned {
					owned = storeutil.CloneObject(obj)
				}
				view.Objects[r] = append(view.Objects[r], owned)
				assigned = true
			}
		}
	}
	for _, objects := range view.Objects {
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
	}
	return view, nil
}
