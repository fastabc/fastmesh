package file

import (
	"context"
	"sort"

	"github.com/fastabc/fastmesh/internal/storeutil"
	"github.com/fastabc/fastmesh/store"
)

var _ store.Viewer = (*Store)(nil)

func (s *Store) View(ctx context.Context, ranges []store.Range) (store.View, error) {
	if err := ctx.Err(); err != nil {
		return store.View{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	view := store.View{Revision: s.state.Version, Objects: make(map[store.Range][]store.Object, len(ranges))}
	for _, r := range ranges {
		objects := []store.Object{}
		for _, obj := range s.state.Objects {
			if (r.Tenant == "" || r.Tenant == obj.Key.Tenant) && (r.Kind == "" || r.Kind == obj.Key.Kind) {
				objects = append(objects, storeutil.CloneObject(obj))
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
