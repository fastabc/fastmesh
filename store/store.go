package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fastabc/fastmesh/store/key"
)

var (
	// ErrCommitUncertain means rename succeeded but durability could not be
	// confirmed. The write may be visible; reconcile before retrying.
	ErrCommitUncertain = errors.New("store: commit durability uncertain")

	// ErrNotFound is returned when a requested object or lease does not exist.
	ErrNotFound = errors.New("store: not found")

	// ErrConflict is returned when a write cannot be applied because another
	// controller owns the current version or lease token.
	ErrConflict = errors.New("store: conflict")

	// ErrConditionalDeleteUnsupported is returned when a store cannot compare
	// an object's resource version and delete it atomically.
	ErrConditionalDeleteUnsupported = errors.New("store: conditional delete unsupported")

	// ErrAtomicCreateUnsupported is returned when a store cannot atomically
	// require that an object does not already exist.
	ErrAtomicCreateUnsupported = errors.New("store: atomic create unsupported")
)

// ObjectKey identifies one control-plane object.
type ObjectKey = key.Key

// Object is an opaque stored control-plane object.
type Object struct {
	Key             ObjectKey
	ResourceVersion int64
	UpdatedAt       time.Time
	Data            []byte
	Labels          map[string]string
}

// ObjectStore persists arbitrary control-plane objects.
type ObjectStore interface {
	GetObject(ctx context.Context, key ObjectKey) (Object, error)
	ListObjects(ctx context.Context, tenant, kind string) ([]Object, error)
	PutObject(ctx context.Context, obj Object) (Object, error)
	DeleteObject(ctx context.Context, key ObjectKey) error
}

// AtomicObjectCreator creates an object only when its key does not exist.
type AtomicObjectCreator interface {
	CreateObject(ctx context.Context, obj Object) (Object, error)
}

// CreateObject calls the store's atomic create primitive.
func CreateObject(ctx context.Context, objects ObjectStore, obj Object) (Object, error) {
	if obj.ResourceVersion != 0 {
		return Object{}, ErrConflict
	}
	creator, ok := objects.(AtomicObjectCreator)
	if !ok {
		return Object{}, ErrAtomicCreateUnsupported
	}
	return creator.CreateObject(ctx, obj)
}

// ConditionalObjectDeleter atomically deletes an object only when its current
// resource version matches. REST If-Match uses this instead of a racy
// get-compare-delete sequence.
type ConditionalObjectDeleter interface {
	DeleteObjectIfVersion(ctx context.Context, key ObjectKey, resourceVersion int64) error
}

// DeleteObjectIfVersion calls the store's atomic conditional-delete primitive.
func DeleteObjectIfVersion(ctx context.Context, objects ObjectStore, key ObjectKey, resourceVersion int64) error {
	if resourceVersion <= 0 {
		return ErrConflict
	}
	deleter, ok := objects.(ConditionalObjectDeleter)
	if !ok {
		return ErrConditionalDeleteUnsupported
	}
	return deleter.DeleteObjectIfVersion(ctx, key, resourceVersion)
}

// Lease is the active-passive fencing record for one controller duty.
type Lease struct {
	Name       string
	Holder     string
	LeaderHint string
	Token      int64
	ExpiresAt  time.Time
	UpdatedAt  time.Time
}

// LeaseRequest carries one lease acquisition or renewal attempt.
type LeaseRequest struct {
	Name       string
	Holder     string
	LeaderHint string
	Token      int64
	TTL        time.Duration
	Now        time.Time
}

// NotLeaderError reports the active controller address while retaining the
// existing ErrConflict fencing classification.
type NotLeaderError struct {
	Hint string
}

func (e *NotLeaderError) Error() string {
	if e.Hint == "" {
		return ErrConflict.Error()
	}
	return fmt.Sprintf("%s: leader is %s", ErrConflict, e.Hint)
}

func (*NotLeaderError) Unwrap() error { return ErrConflict }

// LeaderHint extracts an advertised leader address from err.
func LeaderHint(err error) (string, bool) {
	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) || notLeader.Hint == "" {
		return "", false
	}
	return notLeader.Hint, true
}

// LeaseStore provides the lease primitive used by leader election.
// RenewLease must reject an expired lease; reacquisition issues a new token.
type LeaseStore interface {
	GetLease(ctx context.Context, name string) (Lease, error)
	AcquireLease(ctx context.Context, req LeaseRequest) (Lease, bool, error)
	RenewLease(ctx context.Context, req LeaseRequest) (Lease, bool, error)
	ReleaseLease(ctx context.Context, req LeaseRequest) error
}

// DefaultTenant is the namespace used when a backend receives an empty tenant.
const DefaultTenant = "default"
