package etcd

import (
	"context"

	"github.com/fastabc/fastmesh/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// WatchObjects satisfies store.WatchableStore.
func (s *Store) WatchObjects(ctx context.Context, tenantName string, kinds ...string) (<-chan store.Event, error) {
	out := make(chan store.Event, 16)
	// One kind narrows the etcd key prefix; several share no prefix, so the
	// tenant is as narrow as the server side gets and the rest is decided here.
	prefix := s.objectListPrefix(tenantName, "")
	if len(kinds) == 1 {
		prefix = s.objectListPrefix(tenantName, kinds[0])
	}
	filter := store.NewKindFilter(kinds)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		close(out)
		return out, err
	}
	watch := s.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
	go func() {
		defer close(out)
		tenantFilter := defaultTenantFilter(tenantName)
		for resp := range watch {
			if err := resp.Err(); err != nil {
				return
			}
			for _, ev := range resp.Events {
				mapped, err := mapWatchEvent(ev)
				if err != nil || !objectMatches(mapped.Object, tenantFilter, filter) {
					continue
				}
				select {
				case out <- mapped:
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}()
	return out, nil
}
