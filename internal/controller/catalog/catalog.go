package catalog

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func Ranges(tenant string) []store.Range {
	return []store.Range{{Tenant: tenant, Kind: model.ServiceKind}, {Tenant: tenant, Kind: model.BindGrantKind}, {Tenant: tenant, Kind: model.ServiceAccessKind}, {Tenant: tenant, Kind: model.EndpointKind}, {Tenant: tenant, Kind: model.WorkloadGroupKind}}
}

type Catalog struct {
	Services        map[string]model.ServiceRecord
	Objects         map[string]store.Object
	Grants          []model.BindGrant
	Access          []model.ServiceAccess
	EndpointObjects map[store.ObjectKey]store.Object
	Groups          map[string]model.WorkloadGroup
}

// Decode requires all catalog ranges from the same View, including loaded-empty
// ranges. Missing ranges must not be mistaken for an empty policy set.
// Unknown fields are ignored; malformed or invalid objects are isolated so
// they cannot prevent unrelated services and policies from being compiled.
func Decode(view store.View, tenant string) (Catalog, error) {
	c := Catalog{Services: map[string]model.ServiceRecord{}, Objects: map[string]store.Object{}, EndpointObjects: map[store.ObjectKey]store.Object{}, Groups: map[string]model.WorkloadGroup{}}
	for _, r := range Ranges(tenant) {
		objects, ok := view.Objects[r]
		if !ok {
			return c, fmt.Errorf("catalog: missing range %s", r.Kind)
		}
		for _, obj := range objects {
			if obj.Key.Tenant != tenant || obj.Key.Kind != r.Kind {
				slog.Warn("catalog: ignoring object outside its range", "key", obj.Key)
				continue
			}
			switch r.Kind {
			case model.WorkloadGroupKind:
				group, err := DecodeWorkloadGroup(obj)
				if err != nil {
					slog.Warn("catalog: ignoring invalid group", "key", obj.Key, "error", err)
					continue
				}
				c.Groups[group.Name] = group
			case model.EndpointKind:
				if _, err := DecodeEndpoint(obj); err != nil {
					slog.Warn("catalog: ignoring invalid endpoint", "key", obj.Key, "error", err)
					continue
				}
				c.EndpointObjects[obj.Key] = obj
			case model.ServiceKind:
				var record model.ServiceRecord
				if err := DecodeObject(obj.Data, &record); err != nil {
					slog.Warn("catalog: ignoring malformed service", "key", obj.Key, "error", err)
					continue
				}
				if err := record.Spec.Validate(); err != nil {
					slog.Warn("catalog: ignoring invalid service", "key", obj.Key, "error", err)
					continue
				}
				if record.Spec.Tenant != tenant || record.Spec.Name != obj.Key.Name {
					slog.Warn("catalog: ignoring service with mismatched key", "key", obj.Key)
					continue
				}
				record.Status = model.ServiceStatus{}
				c.Services[obj.Key.Name], c.Objects[obj.Key.Name] = record, obj
			case model.BindGrantKind:
				grant, err := DecodeBindGrant(obj)
				if err != nil {
					slog.Warn("catalog: ignoring invalid bind grant", "key", obj.Key, "error", err)
					continue
				}
				c.Grants = append(c.Grants, grant)
			case model.ServiceAccessKind:
				var record struct {
					Spec model.ServiceAccess `json:"spec"`
				}
				if err := DecodeObject(obj.Data, &record); err != nil {
					slog.Warn("catalog: ignoring malformed service access", "key", obj.Key, "error", err)
					continue
				}
				if err := record.Spec.Validate(); err != nil {
					slog.Warn("catalog: ignoring invalid service access", "key", obj.Key, "error", err)
					continue
				}
				if record.Spec.Tenant != tenant {
					slog.Warn("catalog: ignoring service access with mismatched tenant", "key", obj.Key)
					continue
				}
				c.Access = append(c.Access, record.Spec)
			}
		}
	}
	// Assemble endpoint status from independent instance records.
	for _, obj := range c.EndpointObjects {
		record, _ := DecodeEndpoint(obj)
		e := record.Endpoint
		service, ok := c.Services[e.Service]
		if !ok {
			continue
		}
		if record.RemovedAt.IsZero() {
			if _, exists := service.Spec.Port(e.Port); exists {
				service.Status.Endpoints = append(service.Status.Endpoints, e)
			}
		}
		c.Services[e.Service] = service
	}
	for name, service := range c.Services {
		sort.Slice(service.Status.Endpoints, func(i, j int) bool {
			a, b := service.Status.Endpoints[i], service.Status.Endpoints[j]
			return a.Owner.String()+"\n"+a.InstanceID < b.Owner.String()+"\n"+b.InstanceID
		})
		c.Services[name] = service
	}
	return c, nil
}

// DecodeBindGrant is shared by the full compiler and the lease admission index.
func DecodeBindGrant(obj store.Object) (model.BindGrant, error) {
	var record struct {
		Spec model.BindGrant `json:"spec"`
	}
	if err := DecodeObject(obj.Data, &record); err != nil {
		return record.Spec, err
	}
	if obj.Key.Kind != model.BindGrantKind || record.Spec.Tenant != obj.Key.Tenant || record.Spec.Validate() != nil {
		return record.Spec, model.ErrInvalid
	}
	return record.Spec, nil
}

func DecodeEndpoint(obj store.Object) (model.EndpointRecord, error) {
	var record model.EndpointRecord
	if err := DecodeObject(obj.Data, &record); err != nil {
		return record, err
	}
	e := record.Endpoint
	if !model.ExactName(e.Tenant) || !model.ExactName(e.Service) || !model.ExactName(e.InstanceID) || e.Owner.IsZero() || e.Generation == 0 || obj.Key != model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID) {
		return record, model.ErrInvalid
	}
	if !record.RemovedAt.IsZero() {
		if !record.RetainUntil.After(record.RemovedAt) {
			return record, model.ErrInvalid
		}
	} else if !model.ExactName(e.Port) || !e.Address.IsValid() || e.Address.Port() == 0 || e.Address.Addr().IsUnspecified() || e.Address.Addr().IsMulticast() || e.LeaseExpiresAt.IsZero() {
		return record, model.ErrInvalid
	}
	return record, nil
}

func (c Catalog) CanBind(tenant, service, workload string) bool {
	for _, g := range c.Grants {
		if g.Tenant == tenant && g.Service == service && c.matches(g.Workload, workload) {
			return true
		}
	}
	return false
}

// TopologyChanged excludes lease-only renewal and generation tombstones. A
// signed plan renewal can refresh deadlines without rebuilding its topology.
func TopologyChanged(before, after store.Object) bool {
	if before.Key == after.Key && before.Key.Kind == model.EndpointKind {
		a, aerr := DecodeEndpoint(before)
		b, berr := DecodeEndpoint(after)
		if aerr != nil || berr != nil {
			return true
		}
		if !a.RemovedAt.IsZero() && !b.RemovedAt.IsZero() {
			return false
		}
		a.Endpoint.LeaseExpiresAt, b.Endpoint.LeaseExpiresAt = time.Time{}, time.Time{}
		a.RetainUntil, b.RetainUntil = time.Time{}, time.Time{}
		return !reflect.DeepEqual(a, b)
	}
	if before.Key != after.Key || before.Key.Kind != model.ServiceKind {
		return true
	}
	var a, b model.ServiceRecord
	if json.Unmarshal(before.Data, &a) != nil || json.Unmarshal(after.Data, &b) != nil {
		return true
	}
	for i := range a.Status.Endpoints {
		a.Status.Endpoints[i].LeaseExpiresAt = time.Time{}
	}
	for i := range b.Status.Endpoints {
		b.Status.Endpoints[i].LeaseExpiresAt = time.Time{}
	}
	return !reflect.DeepEqual(a, b)
}

func DecodeWorkloadGroup(obj store.Object) (model.WorkloadGroup, error) {
	var r struct {
		Spec model.WorkloadGroup `json:"spec"`
	}
	if err := DecodeObject(obj.Data, &r); err != nil {
		return r.Spec, err
	}
	if r.Spec.Tenant != obj.Key.Tenant || r.Spec.Name != obj.Key.Name || r.Spec.Validate() != nil {
		return r.Spec, model.ErrInvalid
	}
	return r.Spec, nil
}
func (c Catalog) members(p model.Principal) []spiffeid.ID {
	if !p.ID.IsZero() {
		return []spiffeid.ID{p.ID}
	}
	return c.Groups[p.Group].Members
}
func (c Catalog) matches(p model.Principal, workload string) bool {
	for _, id := range c.members(p) {
		if id.String() == workload {
			return true
		}
	}
	return false
}
