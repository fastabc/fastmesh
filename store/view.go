package store

import (
	"context"
	"errors"
)

var ErrViewUnsupported = errors.New("store: coherent view unsupported")

// Range selects a tenant and kind. An empty field selects all values.
type Range struct{ Tenant, Kind string }

// View owns copies of objects read at one backend revision. Revision orders
// views across controller restarts sharing the same persistent backend.
type View struct {
	Revision int64
	Objects  map[Range][]Object
}

type Viewer interface {
	View(context.Context, []Range) (View, error)
}

func (s *MeteredStore) View(ctx context.Context, ranges []Range) (View, error) {
	viewer, ok := s.next.(Viewer)
	if !ok {
		return View{}, ErrViewUnsupported
	}
	return viewer.View(ctx, ranges)
}

func (s fencedStore) View(ctx context.Context, ranges []Range) (View, error) {
	viewer, ok := s.inner.(Viewer)
	if !ok {
		return View{}, ErrViewUnsupported
	}
	return viewer.View(ctx, ranges)
}
