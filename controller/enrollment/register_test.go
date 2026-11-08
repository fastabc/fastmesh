package enrollment

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	etcdstore "github.com/fastabc/fastmesh/store/etcd"
	filestore "github.com/fastabc/fastmesh/store/file"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/fastabc/fastmesh/test/fixture"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type registrationFault struct {
	store.ObjectStore
	failure error
	after   bool
}

func (s *registrationFault) ApplyObjects(ctx context.Context, changes []store.ObjectChange) error {
	if s.failure != nil && !s.after {
		return s.failure
	}
	if err := store.ApplyObjects(ctx, s.ObjectStore, changes); err != nil {
		return err
	}
	return s.failure
}
func registrationBackend(t *testing.T, backend string) store.ObjectStore {
	t.Helper()
	switch backend {
	case "memory":
		return memory.New()
	case "file":
		s, err := filestore.Open(filepath.Join(t.TempDir(), "store.json"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	default:
		endpoints := fixture.EtcdEndpoints(t)
		if endpoints == "" {
			t.Skip("set FASTMESH_ETCD_ENDPOINTS for registration transactions")
		}
		client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return etcdstore.New(client, ("/fastmesh-register-test/" + strings.ReplaceAll(t.Name(), "/", "-") + "/" + time.Now().Format("150405.000000000")))
	}
}

func TestRegistrationAtomicityAndRetry(t *testing.T) {
	for _, backend := range []string{"memory", "file", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			objects := registrationBackend(t, backend)
			fault := &registrationFault{ObjectStore: objects}
			m := New(fault)
			ctx := context.Background()
			token, err := m.Create(ctx, CreateRequest{Tenant: "acme", MaxUses: 1})
			if err != nil {
				t.Fatal(err)
			}
			id := "0123456789abcdef0123456789abcdef"
			workload := "spiffe://test/app"
			build := func(tok Token) (model.Node, error) {
				return model.Node{ID: "node-one", Tenant: tok.Tenant, SpiffeID: workload, Intent: model.IntentActive, Status: model.NodeStatus{ReportedCapabilities: []string{model.PlanCapability}}}, nil
			}
			fault.failure = errors.New("before commit")
			if _, err := m.Register(ctx, token.Raw, workload, id, build); !errors.Is(err, fault.failure) {
				t.Fatal(err)
			}
			if tok, err := m.Validate(ctx, token.Raw); err != nil || tok.Uses != 0 {
				t.Fatalf("token consumed: %+v %v", tok, err)
			}
			if _, err := objects.GetObject(ctx, model.NodeKey("node-one")); !errors.Is(err, store.ErrNotFound) {
				t.Fatal(err)
			}
			fault.failure = store.ErrCommitUncertain
			fault.after = true
			if _, err := m.Register(ctx, token.Raw, workload, id, build); !errors.Is(err, store.ErrCommitUncertain) {
				t.Fatal(err)
			}
			fault.failure = nil
			node, err := m.Register(ctx, token.Raw, workload, id, func(Token) (model.Node, error) { t.Fatal("retry rebuilt registration"); return model.Node{}, nil })
			if err != nil || node.ID != "node-one" || len(node.Status.ReportedCapabilities) != 1 {
				t.Fatalf("retry: %+v %v", node, err)
			}
			tok, err := m.Get(ctx, token.Token.ID)
			if err != nil || tok.Uses != 1 {
				t.Fatalf("uses: %+v %v", tok, err)
			}
			n, _ := objects.GetObject(ctx, model.NodeKey(node.ID))
			tk, _ := objects.GetObject(ctx, store.ObjectKey{Tenant: model.SystemTenant, Kind: KindJoinToken, Name: tok.ID})
			if n.ResourceVersion != tk.ResourceVersion {
				t.Fatal("node and token not in one revision")
			}
			if _, err := m.Register(ctx, token.Raw, "spiffe://test/other", id, build); !errors.Is(err, ErrExhaustedToken) {
				t.Fatalf("cross workload replay: %v", err)
			}
			if err := m.Revoke(ctx, tok.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := m.Register(ctx, token.Raw, workload, id, build); !errors.Is(err, ErrRevokedToken) {
				t.Fatalf("revoked replay: %v", err)
			}
		})
	}
}

func TestRegistrationConcurrentReplays(t *testing.T) {
	m := New(memory.New())
	ctx := context.Background()
	token, err := m.Create(ctx, CreateRequest{MaxUses: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			node, err := m.Register(ctx, token.Raw, "spiffe://test/app", "0123456789abcdef0123456789abcdef", func(tok Token) (model.Node, error) {
				return model.Node{ID: "one", Tenant: tok.Tenant, SpiffeID: "spiffe://test/app", Intent: model.IntentActive}, nil
			})
			if err != nil || node.ID != "one" {
				t.Errorf("replay: %+v %v", node, err)
			}
		})
	}
	wg.Wait()
	tok, err := m.Get(ctx, token.Token.ID)
	if err != nil || tok.Uses != 1 {
		t.Fatalf("uses=%d err=%v", tok.Uses, err)
	}
}

func TestRegistrationDistinctAttemptsCannotExceedTokenUses(t *testing.T) {
	objects := memory.New()
	m := New(objects)
	ctx := context.Background()
	token, err := m.Create(ctx, CreateRequest{MaxUses: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var successes atomic.Int64
	for i := range 100 {
		wg.Go(func() {
			_, err := m.Register(ctx, token.Raw, "spiffe://test/app", fmt.Sprintf("%032x", i), func(tok Token) (model.Node, error) {
				return model.Node{ID: fmt.Sprint(i), Tenant: tok.Tenant, SpiffeID: "spiffe://test/app", Intent: model.IntentActive}, nil
			})
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrExhaustedToken) {
				t.Errorf("registration: %v", err)
			}
		})
	}
	wg.Wait()
	nodes, err := objects.ListObjects(ctx, model.SystemTenant, model.NodeKind)
	if err != nil || len(nodes) != 1 || successes.Load() != 1 {
		t.Fatalf("nodes=%d successes=%d err=%v", len(nodes), successes.Load(), err)
	}
}

func TestRegistrationReplayFailsClosed(t *testing.T) {
	for _, change := range []string{"expired", "deleted", "disabled", "invalid_request"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now()
			objects := memory.New()
			m := New(objects, WithClock(func() time.Time { return now }))
			token, err := m.Create(ctx, CreateRequest{TTL: time.Minute, MaxUses: 1})
			if err != nil {
				t.Fatal(err)
			}
			id := "0123456789abcdef0123456789abcdef"
			build := func(tok Token) (model.Node, error) {
				return model.Node{ID: "one", Tenant: tok.Tenant, SpiffeID: "spiffe://test/app", Intent: model.IntentActive}, nil
			}
			if _, err := m.Register(ctx, token.Raw, "spiffe://test/app", id, build); err != nil {
				t.Fatal(err)
			}
			var want error
			switch change {
			case "expired":
				now = now.Add(time.Minute)
				want = ErrExpiredToken
			case "deleted":
				if err := objects.DeleteObject(ctx, model.NodeKey("one")); err != nil {
					t.Fatal(err)
				}
				want = store.ErrNotFound
			case "disabled":
				_, err := store.UpdateJSON(ctx, objects, model.NodeKey("one"), 1, func(n *model.Node, _ bool) error { n.Intent = model.IntentDisabled; return nil })
				if err != nil {
					t.Fatal(err)
				}
				want = model.ErrDenied
			case "invalid_request":
				id = "invalid"
				want = ErrInvalidToken
			}
			if _, err := m.Register(ctx, token.Raw, "spiffe://test/app", id, build); !errors.Is(err, want) {
				t.Fatalf("replay error %v, want %v", err, want)
			}
			tok, err := m.Get(ctx, token.Token.ID)
			if err != nil || tok.Uses != 1 {
				t.Fatalf("uses=%d err=%v", tok.Uses, err)
			}
		})
	}
}

func TestRegistrationRejectsLostFence(t *testing.T) {
	for _, backend := range []string{"memory", "file", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			objects := registrationBackend(t, backend)
			leases := objects.(store.LeaseStore)
			ctx := context.Background()
			first, ok, err := leases.AcquireLease(ctx, store.LeaseRequest{Name: "leader", Holder: "old", TTL: time.Minute})
			if err != nil || !ok {
				t.Fatalf("acquire: %v", err)
			}
			fenced := store.Fenced(objects, leases, "leader", "old", first.Token)
			m := New(fenced)
			token, err := m.Create(ctx, CreateRequest{MaxUses: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := leases.ReleaseLease(ctx, store.LeaseRequest{Name: "leader", Holder: "old", Token: first.Token}); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := leases.AcquireLease(ctx, store.LeaseRequest{Name: "leader", Holder: "new", TTL: time.Minute}); err != nil || !ok {
				t.Fatalf("new lease: %v", err)
			}
			_, err = m.Register(ctx, token.Raw, "spiffe://test/app", "0123456789abcdef0123456789abcdef", func(tok Token) (model.Node, error) {
				return model.Node{ID: "one", Tenant: tok.Tenant, SpiffeID: "spiffe://test/app", Intent: model.IntentActive}, nil
			})
			var lost *store.NotLeaderError
			if !errors.As(err, &lost) {
				t.Fatalf("stale fence: %v", err)
			}
			if tok, err := m.Get(ctx, token.Token.ID); err != nil || tok.Uses != 0 {
				t.Fatalf("token consumed: %+v %v", tok, err)
			}
			if _, err := objects.GetObject(ctx, model.NodeKey("one")); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("partial node: %v", err)
			}
		})
	}
}
