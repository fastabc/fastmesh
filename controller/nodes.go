package controller

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

func (c *Controller) DisableNode(ctx context.Context, id string) error {
	unchanged := errors.New("node already disabled")
	_, err := store.UpdateJSON(ctx, c.objects, model.NodeKey(id), 32, func(n *model.Node, exists bool) error {
		if !exists {
			return store.ErrNotFound
		}
		if n.Intent == model.IntentDisabled {
			return unchanged
		}
		*n = model.ApplyNodeIntent(*n, model.IntentDisabled, nil)
		return nil
	})
	if errors.Is(err, unchanged) {
		return nil
	}
	return err
}

// RevokeNode first disables admission, then removes its endpoints and node.
// Retrying after a partial cleanup is safe. Other nodes of the same workload
// keep their independently registered endpoints.
func (c *Controller) RevokeNode(ctx context.Context, id string) error {
	if err := c.DisableNode(ctx, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	obj, err := c.objects.GetObject(ctx, model.NodeKey(id))
	if err != nil {
		return err
	}
	var n model.Node
	if err = json.Unmarshal(obj.Data, &n); err != nil {
		return err
	}
	if err = c.removeNodeEndpoints(ctx, n); err != nil {
		return err
	}
	return store.DeleteObjectIfVersion(ctx, c.objects, obj.Key, obj.ResourceVersion)
}
func (c *Controller) removeNodeEndpoints(ctx context.Context, n model.Node) error {
	objects, err := c.objects.ListObjects(ctx, n.Tenant, model.EndpointKind)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		var r model.EndpointRecord
		if json.Unmarshal(obj.Data, &r) != nil || r.NodeID != n.ID {
			continue
		}
		if err := store.DeleteObjectIfVersion(ctx, c.objects, obj.Key, obj.ResourceVersion); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}
func (c *Controller) expireNode(ctx context.Context, obj store.Object) error {
	var n model.Node
	if err := json.Unmarshal(obj.Data, &n); err != nil {
		return err
	}
	if !n.Ephemeral {
		return nil
	}
	last := n.Status.LastContact
	if last.IsZero() {
		last = n.Status.JoinedAt
	}
	if last.IsZero() || time.Since(last) < c.cfg.EphemeralTTL {
		return nil
	}
	// Atomically disable only the expired version. A newer authenticated contact
	// or operator update supersedes this scheduled task.
	n = model.ApplyNodeIntent(n, model.IntentDisabled, nil)
	var err error
	obj.Data, err = json.Marshal(n)
	if err != nil {
		return err
	}
	if _, err = c.objects.PutObject(ctx, obj); err != nil {
		return err
	}
	return c.RevokeNode(ctx, n.ID)
}
