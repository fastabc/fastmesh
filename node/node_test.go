package node

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

type rotatingIdentity struct {
	mu    sync.RWMutex
	value lifecycleIdentity
}

func (s *rotatingIdentity) GetSVID(ctx context.Context) (spiffe.SVID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value.GetSVID(ctx)
}

func (s *rotatingIdentity) set(value lifecycleIdentity) {
	s.mu.Lock()
	s.value = value
	s.mu.Unlock()
}

func TestEmbeddedNodesRenewReconnectAndOperateWithoutController(t *testing.T) {
	issue, _ := lifecycleIssuer(t)
	controllerIdentity, ordersIdentity, paymentsIdentity := issue("controller", 2), issue("orders", 3), issue("payments", 4)
	controllerSource, ordersSource, paymentsSource := &rotatingIdentity{value: controllerIdentity}, &rotatingIdentity{value: ordersIdentity}, &rotatingIdentity{value: paymentsIdentity}
	backing := memory.New()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	address := "127.0.0.1:0"
	startController := func(id string) (*controller.Controller, context.CancelFunc, <-chan error) {
		t.Helper()
		lis, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		address = lis.Addr().String()
		c, err := controller.New(controller.Config{ID: id, Objects: backing, Leases: backing, Identity: controllerSource, TrustDomain: "test", PlanValidity: 3 * time.Second, EndpointTTL: 400 * time.Millisecond, ReconcileInterval: 20 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		cctx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.Serve(cctx, lis) }()
		for !c.Active() {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
		return c, stop, done
	}
	c, stopController, controllerDone := startController("one")
	controllerStopped := false
	defer func() {
		if !controllerStopped {
			stopController()
			<-controllerDone
		}
	}()
	ordersID, paymentsID := spiffeid.RequireFromString(ordersIdentity.svid.ID.String()), spiffeid.RequireFromString(paymentsIdentity.svid.ID.String())
	if err := c.PutService(ctx, model.Service{Tenant: "default", Name: "payments", RequiresL7: true, Ports: []model.ServicePort{{Name: "api", Number: 8443, Protocol: "http"}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutBindGrant(ctx, "payments", model.BindGrant{Tenant: "default", Service: "payments", Workload: model.Principal{ID: paymentsID}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutServiceAccess(ctx, "orders", model.ServiceAccess{Tenant: "default", Service: "payments", Source: model.Principal{ID: ordersID}, L7: &model.HTTPMatch{Method: "GET", Path: "/pay"}}); err != nil {
		t.Fatal(err)
	}
	newNode := func(identity spiffe.Source, dir string) *Node {
		t.Helper()
		token, err := c.CreateJoinToken(ctx, enrollment.CreateRequest{Tenant: "default"})
		if err != nil {
			t.Fatal(err)
		}
		n, err := New(Config{Identity: identity, Controllers: []string{address}, AllowedControllerIDs: []string{controllerIdentity.svid.ID.String()}, TrustDomain: "test", Tenant: "default", JoinToken: token.Raw, StateDir: dir, ConnectTimeout: 300 * time.Millisecond, DrainTimeout: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n
	}
	orders, payments := newNode(ordersSource, t.TempDir()), newNode(paymentsSource, t.TempDir())
	ref := ServiceRef{Service: "payments", Port: "api"}
	listener, err := payments.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if peer, ok := PeerFromContext(req.Context()); !ok || peer != ordersID {
			t.Errorf("HTTPHandler peer=%s verified=%v", peer, ok)
		}
		_, _ = io.WriteString(w, "paid")
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client := orders.HTTPClient(ref)
	defer client.CloseIdleConnections()
	request := func(path string) (int, error) {
		response, err := client.Get("https://payments" + path)
		if err != nil {
			return 0, err
		}
		_, err = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response.StatusCode, err
	}
	for {
		if code, err := request("/pay"); err == nil && code == http.StatusOK {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("nodes did not converge")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if code, err := request("/other"); err != nil || code != http.StatusForbidden {
		t.Fatalf("L7 deny status=%d err=%v", code, err)
	}
	readEndpoint := func() model.Endpoint {
		t.Helper()
		objects, err := backing.ListObjects(ctx, "default", model.EndpointKind)
		if err != nil {
			t.Fatal(err)
		}
		var live []model.Endpoint
		for _, obj := range objects {
			var record model.EndpointRecord
			if err := json.Unmarshal(obj.Data, &record); err != nil {
				t.Fatal(err)
			}
			if record.RemovedAt.IsZero() {
				live = append(live, record.Endpoint)
			}
		}
		if len(live) == 0 {
			return model.Endpoint{}
		}
		if len(live) != 1 {
			t.Fatalf("endpoint count=%d", len(live))
		}
		return live[0]
	}
	first := readEndpoint()
	if first.InstanceID == "" {
		t.Fatal("registered endpoint missing")
	}
	// Observe an actual renewal without topology/generation replacement.
	for {
		e := readEndpoint()
		if e.LeaseExpiresAt.After(first.LeaseExpiresAt) {
			if e.Generation != first.Generation {
				t.Fatal("renewal replaced topology")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("endpoint not renewed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	stopController()
	if err := <-controllerDone; err != nil {
		t.Fatal(err)
	}
	controllerStopped = true
	// Clear idle connections to prove a fresh call uses the local plan and
	// directory without contacting the controller on the request path.
	client.CloseIdleConnections()
	if code, err := request("/pay"); err != nil || code != http.StatusOK {
		t.Fatalf("offline call status=%d err=%v", code, err)
	}
	// Let the endpoint expire, then restart a controller over the same store.
	timer := time.NewTimer(500 * time.Millisecond)
	select {
	case <-ctx.Done():
		timer.Stop()
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
	c, stopController, controllerDone = startController("two")
	controllerStopped = false
	for {
		e := readEndpoint()
		if e.Generation > first.Generation {
			if e.InstanceID != first.InstanceID {
				t.Fatal("re-registration changed instance")
			}
			break
		}

		select {
		case <-ctx.Done():
			t.Fatal("instance not republished")
		case <-time.After(10 * time.Millisecond):
		}
	}
	for {
		client.CloseIdleConnections()
		if code, err := request("/pay"); err == nil && code == http.StatusOK {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("reconnected nodes did not converge")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Process restart restores NodeID and the durable generation counter.
	previous := readEndpoint()
	previousNodeID := payments.nodeID
	_ = listener.Close()
	_ = payments.Close()
	restarted, err := New(payments.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	listener, err = restarted.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { _, _ = io.WriteString(w, "paid") }))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	afterRestart := readEndpoint()
	if restarted.nodeID != previousNodeID || afterRestart.InstanceID != previous.InstanceID || afterRestart.Generation <= previous.Generation {
		t.Fatal("process restart lost node/instance generation")
	}
	// Reissue all three SVIDs twice, reconnect control streams, and inspect
	// fresh data handshakes to ensure live sources replace the actual leaves.
	for rotation := int64(0); rotation < 2; rotation++ {
		serial := 10 + rotation*3
		controllerSource.set(issue("controller", serial))
		ordersSource.set(issue("orders", serial+1))
		paymentsSource.set(issue("payments", serial+2))
		stopController()
		if err := <-controllerDone; err != nil {
			t.Fatal(err)
		}
		controllerStopped = true
		c, stopController, controllerDone = startController("rotated")
		controllerStopped = false
		for {
			conn, err := orders.Dial(ctx, ref)
			if err == nil {
				state := conn.(*dataplane.Conn).TLSState()
				_ = conn.Close()
				if state.PeerCertificates[0].SerialNumber.Int64() != serial+2 {
					t.Fatal("handshake used stale SVID")
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("rotated nodes did not reconnect")
			case <-time.After(10 * time.Millisecond):
			}
		}
		// The new controller's signer must reach the node on the reconnected
		// ADS stream, rather than merely relying on its last-good data plan.
		before := orders.runtime.CurrentPlan().NotAfter
		for !orders.runtime.CurrentPlan().NotAfter.After(before) {
			select {
			case <-ctx.Done():
				t.Fatal("rotated control stream did not renew")
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}
