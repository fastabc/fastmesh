package store

import (
	"context"
	"encoding/json"
	"errors"
)

var ErrStatusOwner = errors.New("store: status owner mismatch")

type statusOwnerKey struct{}

// StatusOwner returns the in-process status writer carried by ctx.
func StatusOwner(ctx context.Context) string {
	owner, _ := ctx.Value(statusOwnerKey{}).(string)
	return owner
}

// StatusOwners registers controller-owned status fields. Embedding hosts may
// add their own kinds before starting concurrent store users.
var StatusOwners = map[string]string{
	"Node":         "ads-controller",
	"mesh.Service": "mesh-catalog",
}

// PutStatus updates only status through the already-fenced ObjectStore.
func PutStatus(ctx context.Context, objects ObjectStore, owner string, key ObjectKey, status any) (Object, error) {
	return putStatus(ctx, objects, owner, key, 0, status)
}

// PutStatusIfVersion preserves the caller's read/modify/write CAS boundary.
// Concurrent status or desired writes require rereading before retrying.
func PutStatusIfVersion(ctx context.Context, objects ObjectStore, owner string, key ObjectKey, version int64, status any) (Object, error) {
	if version <= 0 {
		return Object{}, ErrConflict
	}
	return putStatus(ctx, objects, owner, key, version, status)
}

func putStatus(ctx context.Context, objects ObjectStore, owner string, key ObjectKey, version int64, status any) (Object, error) {
	if registered, ok := StatusOwners[key.Kind]; !ok || owner == "" || registered != owner {
		return Object{}, ErrStatusOwner
	}
	current, err := objects.GetObject(ctx, key)
	if err != nil {
		return Object{}, err
	}
	if version != 0 && current.ResourceVersion != version {
		return Object{}, ErrConflict
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(current.Data, &envelope); err != nil {
		return Object{}, err
	}
	if envelope == nil {
		return Object{}, errors.New("store: status requires an object envelope")
	}
	raw, err := json.Marshal(status)
	if err != nil {
		return Object{}, err
	}
	envelope["status"] = raw
	current.Data, err = json.Marshal(envelope)
	if err != nil {
		return Object{}, err
	}
	return objects.PutObject(context.WithValue(ctx, statusOwnerKey{}, owner), current)
}
