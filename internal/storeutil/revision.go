package storeutil

import "github.com/fastabc/fastmesh/store"

// FilterRevision returns matching events with payloads owned by one subscriber.
func FilterRevision(batch store.RevisionBatch, tenant string, kinds store.KindFilter) store.RevisionBatch {
	filtered := store.RevisionBatch{Revision: batch.Revision}
	for _, event := range batch.Events {
		if (tenant == "" || event.Object.Key.Tenant == tenant) && kinds.Matches(event.Object.Key.Kind) {
			event.Object = CloneObject(event.Object)
			filtered.Events = append(filtered.Events, event)
		}
	}
	return filtered
}
