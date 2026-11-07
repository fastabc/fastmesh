package store

import (
	"context"
	"errors"
)

// BulkObjectDeleter removes many objects as one durable transition.
//
// It exists for the one caller that deletes objects by the thousand: the
// offline prune of runtime state earlier releases wrote into the configuration
// store. The file backend re-serializes and fsyncs its entire contents per
// write, so deleting ten thousand keys one at a time costs ten thousand full
// rewrites of a store that is large precisely because those keys are in it.
//
// Implementations MUST treat a key that does not exist as already deleted, so
// an interrupted run can simply be repeated.
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
