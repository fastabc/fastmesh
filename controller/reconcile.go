package controller

import (
	"context"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

func (c *Controller) reconcile(ctx context.Context) error {
	budget := 5 * time.Second
	if c.cfg.LeaseTTL > 0 {
		budget = min(budget, c.cfg.LeaseTTL/3)
	}
	// A rebuilding index or a slow backend must yield to election renewal.
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	due := c.index.Expiry.Due(time.Now())
	for n, obj := range due {
		err := ctx.Err()
		if err == nil {
			if obj.Key.Kind == model.NodeKind {
				err = c.expireNode(ctx, obj)
			} else {
				err = c.endpoints.ExpireObject(ctx, obj)
			}
		}
		if err != nil && !errors.Is(err, store.ErrConflict) && !errors.Is(err, store.ErrNotFound) {
			for _, pending := range due[n:] {
				c.index.Expiry.Retry(pending)
			}
			return err
		}
	}
	return nil
}
