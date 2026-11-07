package endpoint

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/fastabc/fastmesh/internal/controller/catalog"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

// ExpireObject compares the scheduled version before changing an instance.
// A renewal or generation replacement makes old expiry work harmless.
func (m *Manager) ExpireObject(ctx context.Context, obj store.Object) error {
	current, err := m.Objects.GetObject(ctx, obj.Key)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.ResourceVersion != obj.ResourceVersion {
		return nil
	}
	if obj.Key.Kind != model.EndpointKind {
		return nil
	}
	record, err := catalog.DecodeEndpoint(current)
	if err != nil {
		return err
	}
	now := m.now()
	if !record.RemovedAt.IsZero() {
		if now.Before(record.RetainUntil) {
			return nil
		}
		return store.DeleteObjectIfVersion(ctx, m.Objects, current.Key, current.ResourceVersion)
	}
	allowed, err := m.allowed(ctx, record.Endpoint.Tenant, record.Endpoint.Service, record.Endpoint.Owner.String())
	if err != nil {
		return err
	}
	_, _, serviceErr := m.service(ctx, record.Endpoint.Tenant, record.Endpoint.Service)
	if serviceErr != nil && !errors.Is(serviceErr, store.ErrNotFound) {
		return serviceErr
	}
	if now.Before(record.Endpoint.LeaseExpiresAt) && allowed && serviceErr == nil {
		return nil
	}
	record = m.tombstone(record.Endpoint, now)
	current.Data, err = json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = m.Objects.PutObject(ctx, current)
	return err
}
