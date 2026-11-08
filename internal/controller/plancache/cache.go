package plancache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/controller/catalog"
	"github.com/fastabc/fastmesh/internal/controller/index"
	"github.com/fastabc/fastmesh/model"
)

type projection struct {
	plan     model.Plan
	topology []byte
}

// ErrCapacity reports that all cold-build admission slots are occupied.
var ErrCapacity = errors.New("plancache: pending capacity reached")

type cacheKey struct {
	snapshot  *index.Snapshot
	recipient model.Session
}
type pendingProjection struct {
	ready, observed chan struct{}
	value           projection
	err             error
	cancel          context.CancelFunc
	waiters         int
	complete        bool
	retained        *list.Element
}

// Cache has bounded completed and pending entries. Configure limits before use;
// zero values select 1024 results, 64 MiB of serialized topology, 128 pending
// builds and GOMAXPROCS workers. The byte limit excludes decoded plans, retained
// snapshots and in-flight builds; it is not a process memory limit.
type Cache struct {
	MaxEntries, MaxPending, Parallelism int
	MaxTopologyBytes                    int
	mu                                  sync.Mutex
	entries                             map[cacheKey]*pendingProjection
	lru                                 list.List
	slots                               chan struct{}
	pending, running                    int
	hits, misses, evictions             uint64
	rejected                            uint64
	queueWait                           time.Duration
	topologyBytes                       int
}

type Stats struct {
	Entries, Pending, Running int
	Hits, Misses, Evictions   uint64
	Rejected                  uint64
	QueueWait                 time.Duration
	TopologyBytes             int
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Entries: c.lru.Len(), Pending: c.pending, Running: c.running,
		Hits: c.hits, Misses: c.misses, Evictions: c.evictions, Rejected: c.rejected,
		QueueWait: c.queueWait, TopologyBytes: c.topologyBytes}
}

// Resource changes only the signed deadline on an unchanged revision. The
// topology bytes are serialized once and copied verbatim into each new body.
// Cancellation releases this caller; a shared build continues for other waiters.
// Compilation itself is synchronous CPU work and finishes once admitted. The
// observer must return promptly; it runs once, after publishing the result.
func (c *Cache) Resource(ctx context.Context, s *index.Snapshot, recipient model.Session, typeURL string, now time.Time, validity time.Duration, record func(time.Duration, error)) (*discoveryv1.Resource, model.Plan, error) {
	if err := ctx.Err(); err != nil {
		return nil, model.Plan{}, err
	}
	key := cacheKey{s, recipient}
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[cacheKey]*pendingProjection)
		if c.MaxEntries <= 0 {
			c.MaxEntries = 1024
		}
		if c.MaxPending <= 0 {
			c.MaxPending = 128
		}
		if c.MaxTopologyBytes <= 0 {
			c.MaxTopologyBytes = 64 << 20
		}
		if c.Parallelism <= 0 {
			c.Parallelism = runtime.GOMAXPROCS(0)
		}
		c.slots = make(chan struct{}, c.Parallelism)
	}
	pending, exists := c.entries[key]
	if !exists {
		if c.pending >= c.MaxPending {
			c.rejected++
			c.mu.Unlock()
			return nil, model.Plan{}, ErrCapacity
		}
		workCtx, cancel := context.WithCancel(context.Background())
		pending = &pendingProjection{ready: make(chan struct{}), observed: make(chan struct{}), cancel: cancel}
		c.entries[key] = pending
		c.pending++
		c.misses++
		go c.build(workCtx, key, pending, now, validity, record)
	} else {
		c.hits++
		if pending.retained != nil {
			c.lru.MoveToFront(pending.retained)
		}
	}
	pending.waiters++
	c.mu.Unlock()
	defer c.release(key, pending)
	select {
	case <-ctx.Done():
		return nil, model.Plan{}, ctx.Err()
	case <-pending.ready:
	}
	// Preserve synchronous observer completion for the initiating call, without
	// making cache hits or other waiters depend on it (including reentrant calls).
	if !exists {
		select {
		case <-ctx.Done():
			return nil, model.Plan{}, ctx.Err()
		case <-pending.observed:
		}
	}
	if pending.err != nil {
		return nil, model.Plan{}, pending.err
	}
	return resource(pending.value, typeURL, now, validity)
}

func (c *Cache) release(key cacheKey, p *pendingProjection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p.waiters--
	if p.waiters == 0 && !p.complete {
		p.cancel()
		if c.entries[key] == p {
			delete(c.entries, key)
		}
	}
}

func (c *Cache) build(ctx context.Context, key cacheKey, pending *pendingProjection, now time.Time, validity time.Duration, record func(time.Duration, error)) {
	started := time.Now()
	var p projection
	compiled := false
	err := ctx.Err()
	if err == nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case c.slots <- struct{}{}:
			c.mu.Lock()
			c.running++
			c.queueWait += time.Since(started)
			c.mu.Unlock()
			err = ctx.Err()
			if err == nil {
				compiled = true
				var decoded catalog.Catalog
				decoded, err = key.snapshot.Catalog()
				if err == nil {
					p.plan, err = catalog.Compile(decoded, key.snapshot.Revision, key.recipient, now, validity)
				}
				if err == nil {
					p.topology, err = json.Marshal(p.plan.Wire().Topology)
					if err == nil && len(p.topology) > model.MaxPlanBodyBytes {
						err = model.ErrPlanTooLarge
					}
				}
			}
			<-c.slots
			c.mu.Lock()
			c.running--
			c.mu.Unlock()
		}
	}
	elapsed := time.Since(started)
	c.mu.Lock()
	c.pending--
	pending.value, pending.err, pending.complete = p, err, true
	pending.cancel()
	if c.entries[key] == pending {
		if err != nil || pending.waiters == 0 {
			delete(c.entries, key)
		} else {
			// Keep only the newest completed generation of each tenant. An old
			// captured view can finish, but must not evict current projections.
			retain := true
			for e := c.lru.Front(); e != nil; {
				next := e.Next()
				other := e.Value.(cacheKey)
				if other.snapshot.Tenant == key.snapshot.Tenant && other.snapshot != key.snapshot {
					if other.snapshot.Revision > key.snapshot.Revision {
						retain = false
					} else {
						c.evict(e)
					}
				}
				e = next
			}
			if retain {
				pending.retained = c.lru.PushFront(key)
				c.topologyBytes += len(p.topology)
			} else {
				delete(c.entries, key)
			}
			for c.lru.Len() > c.MaxEntries || c.topologyBytes > c.MaxTopologyBytes {
				c.evict(c.lru.Back())
			}
		}
	}
	close(pending.ready)
	c.mu.Unlock()
	if compiled && record != nil {
		record(elapsed, err)
	}
	close(pending.observed)
}

// evict runs under mu; existing waiters keep their completed projection.
func (c *Cache) evict(e *list.Element) {
	key := e.Value.(cacheKey)
	p := c.entries[key]
	c.topologyBytes -= len(p.value.topology)
	p.retained = nil
	delete(c.entries, key)
	c.lru.Remove(e)
	c.evictions++
}
