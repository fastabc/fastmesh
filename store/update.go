package store

import (
	"context"
	"encoding/json"
	"errors"
)

// UpdateJSON retries a JSON read-modify-write on object conflicts. Creation is
// atomic; a fenced NotLeaderError returns immediately to the former leader.
// fn receives a fresh decoded value and may abort without writing.
func UpdateJSON[T any](ctx context.Context, objects ObjectStore, key ObjectKey, attempts int, fn func(*T, bool) error) (Object, error) {
	for range attempts {
		if err := ctx.Err(); err != nil {
			return Object{}, err
		}
		obj, err := objects.GetObject(ctx, key)
		exists := err == nil
		if err != nil && !errors.Is(err, ErrNotFound) {
			return Object{}, err
		}
		var value T
		if exists {
			if err := json.Unmarshal(obj.Data, &value); err != nil {
				return Object{}, err
			}
		}
		if err := fn(&value, exists); err != nil {
			return Object{}, err
		}
		obj.Key = key
		obj.Data, err = json.Marshal(value)
		if err != nil {
			return Object{}, err
		}
		var saved Object
		if exists {
			saved, err = objects.PutObject(ctx, obj)
		} else {
			saved, err = CreateObject(ctx, objects, obj)
		}
		var fenced *NotLeaderError
		if errors.Is(err, ErrConflict) && !errors.As(err, &fenced) {
			continue
		}
		return saved, err
	}
	return Object{}, ErrConflict
}
