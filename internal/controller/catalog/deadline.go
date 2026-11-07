package catalog

import (
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

// EndpointDeadline schedules one persistent lease or retained tombstone.
func EndpointDeadline(obj store.Object) (time.Time, bool) {
	if obj.Key.Kind == model.EndpointKind {
		record, err := DecodeEndpoint(obj)
		if err != nil {
			return time.Time{}, false
		}
		if !record.RemovedAt.IsZero() {
			return record.RetainUntil, true
		}
		return record.Endpoint.LeaseExpiresAt, true
	}
	return time.Time{}, false
}
