package controller

import (
	"time"
)

// CacheConfig bounds the built-in mesh compiler. Zero selects 1024 completed
// entries, 64 MiB of serialized topology, 128 pending builds and GOMAXPROCS workers.
// MaxTopologyBytes excludes decoded plans, retained snapshots and pending work.
type CacheConfig struct {
	MaxEntries       int
	MaxPending       int
	Parallelism      int
	MaxTopologyBytes int
}

// CacheStats is a process-local snapshot; queue time includes completed build
// admission waits, and excludes compilation, signing and transport.
type CacheStats struct {
	Entries, Pending, Running         int
	Hits, Misses, Evictions, Rejected uint64
	QueueWait                         time.Duration
	TopologyBytes                     int
}

func (c *Controller) CacheStats() CacheStats {
	s := c.plans.Stats()
	return CacheStats{Entries: s.Entries, Pending: s.Pending, Running: s.Running, Hits: s.Hits, Misses: s.Misses, Evictions: s.Evictions, Rejected: s.Rejected, QueueWait: s.QueueWait, TopologyBytes: s.TopologyBytes}
}
