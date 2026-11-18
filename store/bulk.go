package store

import (
	"context"
	"errors"
)

// BulkObjectDeleter removes the requested keys and counts completed deletions.
// Missing keys are skipped. A backend may commit in batches and return a
// partial count with an error; repeating the request is safe.
type BulkObjectDeleter interface {
	DeleteObjects(ctx context.Context, keys []ObjectKey) (int, error)
}

// DeleteObjects removes every key and reports how many objects it actually
// took away. Keys that were already absent are skipped, not counted, and not
// an error.
//
// Backends that do not implement BulkObjectDeleter get a per-key loop, which is
// correct everywhere and cheap on backends whose writes are already O(1).
func DeleteObjects(ctx context.Context, objects ObjectStore, keys []ObjectKey) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if bulk, ok := objects.(BulkObjectDeleter); ok {
		return bulk.DeleteObjects(ctx, keys)
	}
	removed := 0
	for _, key := range keys {
		switch err := objects.DeleteObject(ctx, key); {
		case err == nil:
			removed++
		case errors.Is(err, ErrNotFound):
		default:
			return removed, err
		}
	}
	return removed, nil
}
