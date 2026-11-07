package store

import (
	"context"
	"errors"
)

// ErrWatchUnsupported is returned by wrappers when their backing ObjectStore
// cannot stream object changes.
var ErrWatchUnsupported = errors.New("store: object watches are unsupported")

// EventType identifies the kind of object store change.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
)

// Event is emitted after a stored object changes. Delete events carry the
// deleted object's key and the deletion ResourceVersion.
type Event struct {
	Type   EventType
	Object Object
}

// WatchableStore is an ObjectStore that can stream changes after the watch is
// established. Callers that need a coherent baseline must establish a revision
// watch before taking a View, then discard batches through the View's revision.
// Listing before subscribing can lose writes between the two operations.
// Individual object events do not describe complete transaction boundaries.
//
// A watch names the kinds it is about. Passing none - or an empty kind - means
// every kind, which is what a subscriber gets when it does not say. Naming them
// is not an optimization for the subscriber, which would filter anyway: it is
// what stops an unrelated write from being delivered to every subscriber in the
// process, under the store's lock, so that each can decide it does not care.
type WatchableStore interface {
	ObjectStore
	WatchObjects(ctx context.Context, tenant string, kinds ...string) (<-chan Event, error)
}

// RevisionBatch contains all matching object events committed by one
// transaction. Its revision is strictly increasing within a watch, but may
// skip revisions consumed by leases or objects outside the subscription.
// Events and their payloads are owned by the receiver.
type RevisionBatch struct {
	Revision int64
	Events   []Event
}

// RevisionWatcher streams complete transaction batches after establishment.
// Establish the watch before taking a View, then discard batches at or below
// the View's revision. A closed stream means continuity was lost (including
// overflow, uncertain commit, malformed backend data or cancellation); rebuild
// from a fresh watch and View. WatchObjects exposes individual object events.
type RevisionWatcher interface {
	ObjectStore
	WatchRevisions(ctx context.Context, tenant string, kinds ...string) (<-chan RevisionBatch, error)
}

// KindFilter matches the kinds one watch subscribes to. A nil filter matches
// every kind.
type KindFilter map[string]struct{}

// NewKindFilter builds the filter for a subscription. No kinds, or any empty
// kind among them, yields nil: match everything.
func NewKindFilter(kinds []string) KindFilter {
	if len(kinds) == 0 {
		return nil
	}
	filter := make(KindFilter, len(kinds))
	for _, kind := range kinds {
		if kind == "" {
			return nil
		}
		filter[kind] = struct{}{}
	}
	return filter
}

// Matches reports whether an event of this kind belongs to the subscription.
func (f KindFilter) Matches(kind string) bool {
	if f == nil {
		return true
	}
	_, ok := f[kind]
	return ok
}
