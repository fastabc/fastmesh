package endpoint

import (
	"context"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/catalog"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

var (
	ErrDenied     = errors.New("catalog: bind denied")
	ErrGeneration = errors.New("catalog: stale instance generation")
)

// Manager writes must use the controller's fenced ObjectStore.
// Each instance has its own CAS boundary, independent of Service status.
type Manager struct {
	Objects     store.ObjectStore
	Viewer      store.Viewer
	TTL         time.Duration
	RPCDeadline time.Duration
	Now         func() time.Time
	// CanBind reads the controller's materialized catalog when installed.
	CanBind func(context.Context, string, string, string) (bool, error)
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now().UTC()
	}
	return time.Now().UTC()
}

func (m *Manager) ttl() time.Duration {
	if m.TTL > 0 {
		return m.TTL
	}
	return 30 * time.Second
}

func (m *Manager) rpcDeadline() time.Duration {
	if m.RPCDeadline > 0 {
		return m.RPCDeadline
	}
	return 5 * time.Second
}

func (m *Manager) read(ctx context.Context, tenant string) (catalog.Catalog, error) {
	v, err := m.Viewer.View(ctx, catalog.Ranges(tenant))
	if err != nil {
		return catalog.Catalog{}, err
	}
	return catalog.Decode(v, tenant)
}

func (m *Manager) service(ctx context.Context, tenant, name string) (store.Object, model.ServiceRecord, error) {
	obj, err := m.Objects.GetObject(ctx, store.ObjectKey{Tenant: tenant, Kind: model.ServiceKind, Name: name})
	var record model.ServiceRecord
	if err != nil {
		return obj, record, err
	}
	if err = catalog.DecodeObject(obj.Data, &record); err != nil {
		return obj, record, err
	}
	if record.Spec.Validate() != nil || record.Spec.Tenant != tenant || record.Spec.Name != name {
		return obj, record, model.ErrInvalid
	}
	return obj, record, nil
}

func (m *Manager) allowed(ctx context.Context, tenant, service, workload string) (bool, error) {
	if m.CanBind != nil {
		return m.CanBind(ctx, tenant, service, workload)
	}
	c, err := m.read(ctx, tenant)
	if err != nil {
		return false, err
	}
	return c.CanBind(tenant, service, workload), nil
}

func (m *Manager) tombstone(e model.Endpoint, now time.Time) model.EndpointRecord {
	return model.EndpointRecord{Endpoint: e, RemovedAt: now, RetainUntil: now.Add(2*m.ttl() + m.rpcDeadline())}
}
