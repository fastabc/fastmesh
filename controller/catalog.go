package controller

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

// These are local administration APIs. Applications exposing remote management
// must authorize callers before invoking them, e.g. via product grpcRegistrars.
func (c *Controller) CreateJoinToken(ctx context.Context, req enrollment.CreateRequest) (enrollment.CreateResponse, error) {
	return c.tokens.Create(ctx, req)
}

func (c *Controller) PutService(ctx context.Context, service model.Service) error {
	if err := service.Validate(); err != nil {
		return err
	}
	key := store.ObjectKey{Tenant: service.Tenant, Kind: model.ServiceKind, Name: service.Name}
	_, err := store.UpdateJSON(ctx, c.objects, key, 8, func(record *model.ServiceRecord, _ bool) error { record.Spec = service; return nil })
	return err
}

func (c *Controller) PutBindGrant(ctx context.Context, name string, grant model.BindGrant) error {
	if err := grant.Validate(); err != nil {
		return err
	}
	return c.putDesired(ctx, grant.Tenant, model.BindGrantKind, name, grant)
}

func (c *Controller) PutServiceAccess(ctx context.Context, name string, access model.ServiceAccess) error {
	if err := access.Validate(); err != nil {
		return err
	}
	return c.putDesired(ctx, access.Tenant, model.ServiceAccessKind, name, access)
}

func (c *Controller) putDesired(ctx context.Context, tenant, kind, name string, spec any) error {
	if !model.ExactName(name) {
		return model.ErrInvalid
	}
	data, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		return err
	}
	_, err = c.objects.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: tenant, Kind: kind, Name: name}, Data: data})
	return err
}

func (c *Controller) DeleteService(ctx context.Context, tenant, name string) error {
	err := c.objects.DeleteObject(ctx, store.ObjectKey{Tenant: tenant, Kind: model.ServiceKind, Name: name})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	objects, listErr := c.objects.ListObjects(ctx, tenant, model.EndpointKind)
	if listErr != nil {
		return listErr
	}
	for _, obj := range objects {
		var record model.EndpointRecord
		if json.Unmarshal(obj.Data, &record) != nil || record.Endpoint.Service != name {
			continue
		}
		deleteErr := store.DeleteObjectIfVersion(ctx, c.objects, obj.Key, obj.ResourceVersion)
		if deleteErr != nil && !errors.Is(deleteErr, store.ErrNotFound) && !errors.Is(deleteErr, store.ErrConflict) {
			return deleteErr
		}
	}
	return err
}
func (c *Controller) DeleteBindGrant(ctx context.Context, tenant, name string) error {
	return c.objects.DeleteObject(ctx, store.ObjectKey{Tenant: tenant, Kind: model.BindGrantKind, Name: name})
}
func (c *Controller) DeleteServiceAccess(ctx context.Context, tenant, name string) error {
	return c.objects.DeleteObject(ctx, store.ObjectKey{Tenant: tenant, Kind: model.ServiceAccessKind, Name: name})
}

func (c *Controller) PutWorkloadGroup(ctx context.Context, group model.WorkloadGroup) error {
	if err := group.Validate(); err != nil {
		return err
	}
	return c.putDesired(ctx, group.Tenant, model.WorkloadGroupKind, group.Name, group)
}
func (c *Controller) DeleteWorkloadGroup(ctx context.Context, tenant, name string) error {
	return c.objects.DeleteObject(ctx, store.ObjectKey{Tenant: tenant, Kind: model.WorkloadGroupKind, Name: name})
}
