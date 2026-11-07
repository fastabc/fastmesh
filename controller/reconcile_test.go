package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/internal/controller/index"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStandbyAdmissionIncludesLeaderHint(t *testing.T) {
	backing := memory.New()
	for _, id := range []string{"leader", "standby"} {
		c, err := New(Config{ID: id, Objects: backing, Leases: backing, DevInsecure: true})
		if err != nil {
			t.Fatal(err)
		}
		c.elector.SetAdvertise(id + ":9443")
		active, err := c.elector.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if id == "leader" {
			if !active {
				t.Fatal("leader did not acquire lease")
			}
			continue
		}
		_, err = c.authorize(context.Background(), "node", "acme")
		if status.Code(err) != codes.Unavailable || !strings.Contains(status.Convert(err).Message(), "leader:9443") {
			t.Fatalf("standby admission omitted leader address: %v", err)
		}
	}
}

type transientReconcileStore struct {
	*memory.Store
	failView  bool
	failures  int32
	calls     atomic.Int32
	recovered chan struct{}
	attempts  chan time.Time
}

func (s *transientReconcileStore) failOnce() error {
	if s.attempts != nil {
		s.attempts <- time.Now()
	}
	failures := max(s.failures, 1)
	call := s.calls.Add(1)
	if call <= failures {
		return errors.New("temporary store failure")
	}
	if call == failures+1 {
		close(s.recovered)
	}
	return nil
}

func TestReconciliationBackoffKeepsRenewingControllerLease(t *testing.T) {
	backing := memory.New()
	s := &transientReconcileStore{Store: backing, failView: true, failures: 4, recovered: make(chan struct{}), attempts: make(chan time.Time, 16)}
	c, err := New(Config{ID: "controller", Objects: s, Leases: backing, DevInsecure: true, LeaseTTL: 30 * time.Millisecond, ReconcileInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.elector.TryAcquire(ctx); err != nil {
		t.Fatal(err)
	}
	token := c.elector.Lease().Token
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-s.recovered:
	case err := <-done:
		t.Fatalf("controller exited: %v", err)
	case <-ctx.Done():
		t.Fatal("reconciliation did not recover")
	}
	previous := <-s.attempts
	for _, minimum := range []time.Duration{10, 20, 40, 80} {
		attempt := <-s.attempts
		if attempt.Sub(previous) < minimum*time.Millisecond {
			t.Errorf("retry interval=%s, want at least %s", attempt.Sub(previous), minimum*time.Millisecond)
		}
		previous = attempt
	}
	if !c.Active() || c.elector.Lease().Token != token {
		t.Error("reconciliation backoff interrupted controller lease renewal")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
}

func (s *transientReconcileStore) ListObjects(ctx context.Context, tenant, kind string) ([]store.Object, error) {
	if !s.failView {
		if err := s.failOnce(); err != nil {
			return nil, err
		}
	}
	return s.Store.ListObjects(ctx, tenant, kind)
}

func (s *transientReconcileStore) View(ctx context.Context, ranges []store.Range) (store.View, error) {
	if s.failView {
		if err := s.failOnce(); err != nil {
			return store.View{}, err
		}
	}
	return s.Store.View(ctx, ranges)
}

func TestRunSurvivesTransientReconciliationErrors(t *testing.T) {
	for _, failView := range []bool{true} {
		name := "list"
		if failView {
			name = "expiry-view"
		}
		t.Run(name, func(t *testing.T) {
			backing := memory.New()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			if _, err := backing.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "acme", Kind: model.ServiceKind, Name: "payments"}, Data: []byte(`{"spec":{"tenant":"acme","name":"payments","ports":[{"name":"api","number":8080,"protocol":"tcp"}]}}`)}); err != nil {
				t.Fatal(err)
			}
			s := &transientReconcileStore{Store: backing, failView: failView, recovered: make(chan struct{})}
			c, err := New(Config{ID: "controller", Objects: s, Leases: backing, DevInsecure: true, ReconcileInterval: 5 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx) }()
			select {
			case err := <-done:
				t.Fatalf("controller exited on transient failure: %v", err)
			case <-s.recovered:
			case <-ctx.Done():
				t.Fatal("reconciliation did not retry")
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("shutdown: %v", err)
			}
		})
	}
}

type failingExpiryReadStore struct {
	*memory.Store
	failures atomic.Int32
}

func (s *failingExpiryReadStore) GetObject(ctx context.Context, key store.ObjectKey) (store.Object, error) {
	if key.Kind == model.EndpointKind && s.failures.Add(1) == 1 {
		return store.Object{}, errors.New("temporary endpoint read failure")
	}
	return s.Store.GetObject(ctx, key)
}
func TestExpiryBatchRetriesAllRemainingTasksAfterStoreFailure(t *testing.T) {
	ctx := context.Background()
	s := &failingExpiryReadStore{Store: memory.New()}
	c := &Controller{index: index.New(s, s), endpoints: &endpoint.Manager{Objects: s, Viewer: s}}
	owner := spiffeid.RequireFromString("spiffe://test/owner")
	var objects []store.Object
	for _, instance := range []string{"one", "two"} {
		e := model.Endpoint{Tenant: "default", Service: "api", Port: "tcp", InstanceID: instance, Owner: owner, Address: netip.MustParseAddrPort("127.0.0.1:80"), Generation: 1, Ready: true, Serving: true, LeaseExpiresAt: time.Now().Add(-time.Second)}
		data, err := json.Marshal(model.EndpointRecord{Endpoint: e})
		if err != nil {
			t.Fatal(err)
		}
		obj, err := s.PutObject(ctx, store.Object{Key: model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID), Data: data})
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, obj)
	}
	c.index.Expiry.Reset(objects)
	if err := c.reconcile(ctx); err == nil {
		t.Fatal("injected failure not returned")
	}
	if err := c.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	for _, obj := range objects {
		saved, err := s.GetObject(ctx, obj.Key)
		if err != nil {
			t.Fatal(err)
		}
		var record model.EndpointRecord
		if err := json.Unmarshal(saved.Data, &record); err != nil {
			t.Fatal(err)
		}
		if record.RemovedAt.IsZero() {
			t.Fatalf("expiry task lost after preceding failure: %s", obj.Key.Name)
		}
	}
}

func TestReconciliationYieldsToElectionWhileIndexIsUnavailable(t *testing.T) {
	s := memory.New()
	c, err := New(Config{ID: "controller", Objects: s, Leases: s, DevInsecure: true, LeaseTTL: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// The index has no snapshot yet, as during a failed rebuild. Expiry must
	// return within the election budget rather than wait for Run's whole ctx.
	e := model.Endpoint{Tenant: "default", Service: "api", Port: "tcp", InstanceID: "one", Owner: spiffeid.RequireFromString("spiffe://test/owner"), Address: netip.MustParseAddrPort("127.0.0.1:80"), Generation: 1, Ready: true, Serving: true, LeaseExpiresAt: time.Now().Add(-time.Second)}
	data, _ := json.Marshal(model.EndpointRecord{Endpoint: e})
	obj, err := s.PutObject(context.Background(), store.Object{Key: model.EndpointKey(e.Tenant, e.Service, e.Owner, e.InstanceID), Data: data})
	if err != nil {
		t.Fatal(err)
	}
	c.index.Expiry.Reset([]store.Object{obj})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := c.reconcile(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reconciliation did not yield: %v", err)
	}
	if time.Since(start) >= 500*time.Millisecond {
		t.Fatal("reconciliation consumed election renewal budget")
	}
	if len(c.index.Expiry.Due(time.Now().Add(time.Second))) != 1 {
		t.Fatal("timed-out expiry was lost")
	}
}
