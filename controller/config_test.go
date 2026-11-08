package controller

import (
	"context"
	"errors"
	"fmt"
	"github.com/fastabc/fastmesh/store"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/controller/discovery"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store/election"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestControllerDurationValidation(t *testing.T) {
	fields := map[string]func(*Config, time.Duration){
		"lease":     func(c *Config, d time.Duration) { c.LeaseTTL = d },
		"endpoint":  func(c *Config, d time.Duration) { c.EndpointTTL = d },
		"ephemeral": func(c *Config, d time.Duration) { c.EphemeralTTL = d },
		"plan":      func(c *Config, d time.Duration) { c.PlanValidity = d },
	}
	for name, set := range fields {
		for _, injected := range []bool{false, true} {
			for _, duration := range []time.Duration{-1, 0, 1, 2, 3, time.Second} {
				t.Run(name+"/"+duration.String()+"/"+map[bool]string{false: "owned", true: "injected"}[injected], func(t *testing.T) {
					st := memory.New()
					cfg := Config{ID: "test", Objects: st, Leases: st, DevInsecure: true}
					if injected {
						cfg.Elector = election.New(st, election.Config{Holder: "test", TTL: time.Second})
					}
					set(&cfg, duration)
					c, err := New(cfg)
					if duration > 0 && duration < 3*time.Nanosecond {
						if err == nil {
							t.Fatal("accepted duration with zero derived interval")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					_ = c.Run(ctx) // Even cancellation must not hide a zero-ticker panic.
				})
			}
		}
	}
}

func TestCacheConfigReachesBuiltInCompiler(t *testing.T) {
	st := memory.New()
	cfg := Config{ID: "test", Objects: st, Leases: st, DevInsecure: true, Cache: CacheConfig{MaxEntries: 3, MaxPending: 2, Parallelism: 1, MaxTopologyBytes: 4096}}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if c.plans.MaxEntries != 3 || c.plans.MaxPending != 2 || c.plans.Parallelism != 1 || c.plans.MaxTopologyBytes != 4096 {
		t.Fatal("cache limits not installed")
	}
	if c.CacheStats() != (CacheStats{}) {
		t.Fatal("nonempty initial stats")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	done := make(chan error, 1)
	go func() { done <- c.index.Run(ctx) }()
	defer func() { cancel(); <-done }()
	for i := range 5 {
		_, err := c.build(ctx, discovery.BuildRequest{TypeURL: model.PlanTypeURL, Session: discovery.Session{Principal: model.Session{Tenant: "default", NodeID: fmt.Sprint("node-", i), Workload: spiffeid.RequireFromString("spiffe://test/workload")}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if stats := c.CacheStats(); stats.Entries != 3 || stats.Evictions != 2 || stats.Misses != 5 || stats.TopologyBytes == 0 || stats.TopologyBytes > 4096 {
		t.Fatalf("public limits/stats not effective: %+v", stats)
	}

	cfg.Cache.MaxEntries = -1
	if _, err := New(cfg); err == nil {
		t.Fatal("negative cache limit accepted")
	}
}

// A store exposing individual events cannot guarantee complete transaction
// snapshots. Reject it before starting background controller work.
type objectEventStore struct {
	store.Viewer
	store.WatchableStore
}

func TestControllerRequiresRevisionWatches(t *testing.T) {
	backing := memory.New()
	_, err := New(Config{ID: "test", Objects: objectEventStore{Viewer: backing, WatchableStore: backing}, Leases: backing, DevInsecure: true})
	if !errors.Is(err, store.ErrWatchUnsupported) {
		t.Fatalf("unsupported backend: %v", err)
	}
}
