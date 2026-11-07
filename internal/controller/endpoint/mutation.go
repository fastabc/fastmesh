package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

func (m *Manager) updateEndpoint(ctx context.Context, session model.Session, service, instance string, change func(*model.EndpointRecord, model.Service, time.Time) error) (model.Endpoint, error) {
	if !model.ExactName(session.Tenant) || !model.ExactName(service) || !model.ExactName(instance) || session.NodeID == "" || session.Workload.IsZero() {
		return model.Endpoint{}, ErrDenied
	}
	ctx, cancel := context.WithTimeout(ctx, m.rpcDeadline())
	defer cancel()
	key := model.EndpointKey(session.Tenant, service, session.Workload, instance)
	for attempt := 0; attempt < 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return model.Endpoint{}, err
		}
		_, serviceRecord, err := m.service(ctx, session.Tenant, service)
		if err != nil {
			return model.Endpoint{}, err
		}
		allowed, err := m.allowed(ctx, session.Tenant, service, session.Workload.String())
		if err != nil {
			return model.Endpoint{}, err
		}
		if !allowed {
			return model.Endpoint{}, ErrDenied
		}
		obj, err := m.Objects.GetObject(ctx, key)
		missing := errors.Is(err, store.ErrNotFound)
		if err != nil && !missing {
			return model.Endpoint{}, err
		}
		var record model.EndpointRecord
		if !missing {
			record, err = catalog.DecodeEndpoint(obj)
			if err != nil {
				return model.Endpoint{}, err
			}
		}
		if record.NodeID != "" && record.NodeID != session.NodeID {
			return model.Endpoint{}, ErrDenied
		}
		if err = change(&record, serviceRecord.Spec, m.now()); err != nil {
			return model.Endpoint{}, err
		}
		if record.NodeID == "" {
			record.NodeID = session.NodeID
		}
		// Some local stores do not inspect ctx themselves. Enforce the RPC
		// bound before writing so a delayed request cannot outlive retention.
		if err = ctx.Err(); err != nil {
			return model.Endpoint{}, err
		}
		data, err := json.Marshal(record)
		if err != nil {
			return model.Endpoint{}, err
		}
		obj.Key, obj.Data = key, data
		var saved store.Object
		if missing {
			saved, err = store.CreateObject(ctx, m.Objects, obj)
		} else {
			saved, err = m.Objects.PutObject(ctx, obj)
		}
		if errors.Is(err, store.ErrConflict) {
			if _, fenced := store.LeaderHint(err); !fenced {
				continue
			}
		}
		if err != nil {
			return model.Endpoint{}, err
		}
		// A delete can race the separate endpoint write. Never leave a newly
		// created instance behind after its service has been removed.
		if _, _, err = m.service(ctx, session.Tenant, service); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				_ = store.DeleteObjectIfVersion(ctx, m.Objects, saved.Key, saved.ResourceVersion)
			}
			return model.Endpoint{}, err
		}
		return record.Endpoint, nil
	}
	return model.Endpoint{}, store.ErrConflict
}
