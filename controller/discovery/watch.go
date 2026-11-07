package discovery

import (
	"context"
	"time"

	"github.com/fastabc/fastmesh/store"
)

const maxWatchBackoff = time.Second

type WatchConfig struct {
	Objects  store.WatchableStore
	Kinds    []string
	Backoff  time.Duration
	Relevant func(store.Event) bool
	// OnSubscribe captures a baseline after the watch is established, also
	// after reconnect. Relevant may ignore queued events through that revision.
	OnSubscribe func(context.Context) error
}

// WatchChanges subscribes to host-selected kinds for non-mesh TypeURLs:
// one stream serves one tenant, but the node registration it also depends on
// lives in the system tenant.
//
// Each host subscription watches only relevant kinds. Subscribing to everything meant one
// session key rotation - a write no snapshot can depend on - was delivered to
// every stream in the process, under the store's lock, for each to discard.
func WatchChanges(ctx context.Context, cfg WatchConfig) (<-chan struct{}, error) {
	if cfg.Objects == nil {
		return nil, store.ErrWatchUnsupported
	}
	ctx, cancel := context.WithCancel(ctx)
	events, err := cfg.Objects.WatchObjects(ctx, "", cfg.Kinds...)
	if err != nil {
		cancel()
		return nil, err
	}
	if cfg.OnSubscribe != nil {
		if err := cfg.OnSubscribe(ctx); err != nil {
			cancel()
			return nil, err
		}
	}
	out := make(chan struct{}, 1)
	go func() { defer cancel(); runSnapshotWatch(ctx, cfg, events, out) }()
	return out, nil
}

func runSnapshotWatch(ctx context.Context, cfg WatchConfig, events <-chan store.Event, out chan<- struct{}) {
	defer close(out)
	backoff := cfg.Backoff
	if backoff <= 0 {
		backoff = defaultCoalesceWindow
	}
	forceRebuild := false
	for {
		if forceRebuild {
			notifySnapshotChange(out)
			forceRebuild = false
		}
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if ok {
				if cfg.Relevant == nil || cfg.Relevant(event) {
					notifySnapshotChange(out)
				}
				continue
			}
			forceRebuild = true
		}
		for {
			if !sleepContext(ctx, backoff) {
				return
			}
			next, err := cfg.Objects.WatchObjects(ctx, "", cfg.Kinds...)
			if err == nil && cfg.OnSubscribe != nil {
				if cfg.OnSubscribe(ctx) != nil {
					return
				}
			}
			if err == nil {
				events = next
				backoff = cfg.Backoff
				if backoff <= 0 {
					backoff = defaultCoalesceWindow
				}
				break
			}
			if ctx.Err() != nil {
				return
			}
			notifySnapshotChange(out)
			backoff = nextWatchBackoff(backoff)
		}
	}
}

func notifySnapshotChange(out chan<- struct{}) {
	select {
	case out <- struct{}{}:
	default:
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		d = defaultCoalesceWindow
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextWatchBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultCoalesceWindow
	}
	d *= 2
	if d > maxWatchBackoff {
		return maxWatchBackoff
	}
	return d
}
