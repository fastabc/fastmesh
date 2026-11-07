package etcd

import (
	"context"
	"fmt"

	"github.com/fastabc/fastmesh/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ store.RevisionWatcher = (*Store)(nil)

// WatchRevisions implements store.RevisionWatcher.
func (s *Store) WatchRevisions(ctx context.Context, tenant string, kinds ...string) (<-chan store.RevisionBatch, error) {
	out := make(chan store.RevisionBatch, 16)
	prefix := s.objectListPrefix(tenant, "")
	if len(kinds) == 1 {
		prefix = s.objectListPrefix(tenant, kinds[0])
	}
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		close(out)
		return out, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	// Do not use WithFragment: clientv3 reassembles wire fragments before
	// publishing a WatchResponse, so a transaction is never partially applied.
	watch := s.client.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
	go func() {
		defer cancel()
		streamRevisionBatches(watchCtx, watch, out, resp.Header.Revision, defaultTenantFilter(tenant), store.NewKindFilter(kinds))
	}()
	return out, nil
}

func streamRevisionBatches(ctx context.Context, watch clientv3.WatchChan, out chan store.RevisionBatch, last int64, tenant string, kinds store.KindFilter) {
	defer close(out)
	for response := range watch {
		if response.Err() != nil || response.Canceled {
			return
		}
		batches, err := revisionBatches(response.Events, last, tenant, kinds)
		if err != nil {
			return
		}
		for _, batch := range batches {
			last = batch.Revision
			if len(batch.Events) == 0 {
				continue
			}
			select {
			case out <- batch:
			case <-ctx.Done():
				return
			default:
				// Losing one transaction invalidates the materialized view.
				return
			}
		}
	}
}

// A catch-up response can include several transaction revisions. The header
// describes the current server revision, not necessarily these transactions.
func revisionBatches(events []*clientv3.Event, last int64, tenant string, kinds store.KindFilter) ([]store.RevisionBatch, error) {
	var batches []store.RevisionBatch
	for _, event := range events {
		if event == nil || event.Kv == nil {
			return nil, fmt.Errorf("etcd store: missing watch event key")
		}
		revision := event.Kv.ModRevision
		if len(batches) == 0 || batches[len(batches)-1].Revision != revision {
			if revision <= last {
				return nil, fmt.Errorf("etcd store: non-increasing watch revision %d", revision)
			}
			batches = append(batches, store.RevisionBatch{Revision: revision})
			last = revision
		}
		mapped, err := mapWatchEvent(event)
		if err != nil {
			return nil, err
		}
		if objectMatches(mapped.Object, tenant, kinds) {
			batch := &batches[len(batches)-1]
			batch.Events = append(batch.Events, mapped)
		}
	}
	return batches, nil
}
