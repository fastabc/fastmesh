package endpoint

import (
	"context"
	"net/netip"
	"time"

	"github.com/fastabc/fastmesh/model"
)

type Registration struct {
	Service, Port, InstanceID string
	Generation                uint64
	Address                   netip.AddrPort
	Ready                     bool
	Serving, Terminating      bool
}

func (m *Manager) Register(ctx context.Context, session model.Session, req Registration) (model.Endpoint, error) {
	req.Serving = req.Serving || req.Ready
	req.Ready = req.Serving && !req.Terminating
	if !model.ExactName(req.InstanceID) || req.Generation == 0 || !req.Address.IsValid() || req.Address.Addr().IsUnspecified() || req.Address.Addr().IsMulticast() || req.Address.Port() == 0 {
		return model.Endpoint{}, model.ErrInvalid
	}
	return m.updateEndpoint(ctx, session, req.Service, req.InstanceID, func(record *model.EndpointRecord, service model.Service, now time.Time) error {
		if _, ok := service.Port(req.Port); !ok {
			return model.ErrInvalid
		}
		old := record.Endpoint
		if req.Generation < old.Generation {
			return ErrGeneration
		}
		if req.Generation == old.Generation {
			if !record.RemovedAt.IsZero() || !now.Before(old.LeaseExpiresAt) || old.Address != req.Address || old.Port != req.Port || old.Ready != req.Ready || (old.Serving || old.Ready) != req.Serving || old.Terminating != req.Terminating {
				return ErrGeneration
			}
			return nil
		}
		*record = model.EndpointRecord{NodeID: session.NodeID, Endpoint: model.Endpoint{Tenant: session.Tenant, Service: req.Service, Port: req.Port, InstanceID: req.InstanceID, Owner: session.Workload, Address: req.Address, Ready: req.Ready, Serving: req.Serving, Terminating: req.Terminating, Generation: req.Generation, LeaseExpiresAt: now.Add(m.ttl())}}
		return nil
	})
}

func (m *Manager) Renew(ctx context.Context, session model.Session, service, instance string, generation uint64) (model.Endpoint, error) {
	return m.updateEndpoint(ctx, session, service, instance, func(record *model.EndpointRecord, spec model.Service, now time.Time) error {
		if !record.RemovedAt.IsZero() || record.Endpoint.Generation != generation || !now.Before(record.Endpoint.LeaseExpiresAt) {
			return ErrGeneration
		}
		if _, exists := spec.Port(record.Endpoint.Port); !exists {
			return model.ErrInvalid
		}
		record.Endpoint.LeaseExpiresAt = now.Add(m.ttl())
		return nil
	})
}

func (m *Manager) Deregister(ctx context.Context, session model.Session, service, instance string, generation uint64) error {
	_, err := m.updateEndpoint(ctx, session, service, instance, func(record *model.EndpointRecord, _ model.Service, now time.Time) error {
		if !record.RemovedAt.IsZero() || record.Endpoint.Generation != generation || generation == 0 {
			return ErrGeneration
		}
		id := record.NodeID
		*record = m.tombstone(record.Endpoint, now)
		record.NodeID = id
		return nil
	})
	return err
}

func (m *Manager) SetServing(ctx context.Context, session model.Session, service, instance string, generation uint64, serving bool) (model.Endpoint, error) {
	return m.updateEndpoint(ctx, session, service, instance, func(r *model.EndpointRecord, _ model.Service, now time.Time) error {
		if !r.RemovedAt.IsZero() || r.Endpoint.Generation != generation || !now.Before(r.Endpoint.LeaseExpiresAt) || r.Endpoint.Terminating {
			return ErrGeneration
		}
		r.Endpoint.Serving, r.Endpoint.Ready = serving, serving
		return nil
	})
}
