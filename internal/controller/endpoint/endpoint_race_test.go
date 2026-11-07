package endpoint_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
)

type heldEndpointWriter struct {
	store.ObjectStore
	entered, resume chan struct{}
	once            sync.Once
}

func (s *heldEndpointWriter) PutObject(ctx context.Context, obj store.Object) (store.Object, error) {
	if obj.Key.Kind == model.EndpointKind {
		s.once.Do(func() {
			close(s.entered)
			select {
			case <-s.resume:
			case <-ctx.Done():
			}
		})
	}
	if err := ctx.Err(); err != nil {
		return store.Object{}, err
	}
	return s.ObjectStore.PutObject(ctx, obj)
}

func TestInFlightRenewalCannotOverwriteNewGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backing := memory.New()
	session, req := seed(t, backing)
	current := &endpoint.Manager{Objects: backing, Viewer: backing}
	if _, err := current.Register(ctx, session, req); err != nil {
		t.Fatal(err)
	}
	held := &heldEndpointWriter{ObjectStore: backing, entered: make(chan struct{}), resume: make(chan struct{})}
	delayed := &endpoint.Manager{Objects: held, Viewer: backing}
	done := make(chan struct{})
	var renewErr error
	go func() {
		defer close(done)
		_, renewErr = delayed.Renew(ctx, session, req.Service, req.InstanceID, req.Generation)
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-held.entered:
	case <-ctx.Done():
		t.Fatal("renewal did not reach write barrier")
	}
	next := req
	next.Generation++
	if _, err := current.Register(ctx, session, next); err != nil {
		t.Fatal(err)
	}
	key := model.EndpointKey(session.Tenant, req.Service, session.Workload, req.InstanceID)
	before, err := backing.GetObject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	close(held.resume)
	select {
	case <-done:
		if !errors.Is(renewErr, endpoint.ErrGeneration) {
			t.Fatalf("delayed renewal did not reject superseded generation: %v", renewErr)
		}
	case <-ctx.Done():
		t.Fatal("delayed renewal did not complete")
	}
	after, err := backing.GetObject(ctx, key)
	if err != nil || before.ResourceVersion != after.ResourceVersion {
		t.Fatalf("delayed renewal overwrote new registration: %v", err)
	}
}
