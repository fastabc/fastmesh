package controller

import (
	"time"
)

// CacheConfig bounds the built-in mesh compiler. Zero selects 1024 completed
// entries, 128 pending builds and GOMAXPROCS workers. Counts are not byte limits.
type CacheConfig struct {
	MaxEntries  int
	MaxPending  int
	Parallelism int
}

// CacheStats is a process-local snapshot; queue time includes completed build
// admission waits, and excludes compilation, signing and transport.
type CacheStats struct {
	Entries, Pending, Running         int
	Hits, Misses, Evictions, Rejected uint64
	QueueWait                         time.Duration
}

func (c *Controller) CacheStats() CacheStats {
	s := c.plans.Stats()
	return CacheStats{Entries: s.Entries, Pending: s.Pending, Running: s.Running, Hits: s.Hits, Misses: s.Misses, Evictions: s.Evictions, Rejected: s.Rejected, QueueWait: s.QueueWait}
}
