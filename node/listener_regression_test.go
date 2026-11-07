package node

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/test/fixture"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A real TLS echo pair exercises connection revocation, not just the predicate.
func runtimeNodePair(t *testing.T, timeout time.Duration, httpMode bool) (*Node, *Node, *dataplane.Listener, model.Plan) {
	t.Helper()
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientPlan, serverPlan := runtimePlans(spiffeid.RequireFromString(clientIdentity.svid.ID.String()), spiffeid.RequireFromString(serverIdentity.svid.ID.String()))
	if httpMode {
		serverPlan.Inbound[0].Port.Protocol = "http"
	}
	clientRuntime, serverRuntime := dataplane.New(clientIdentity, timeout), dataplane.New(serverIdentity, timeout)
	installPlan(t, serverRuntime, serverPlan)
	ref := ServiceRef{Service: "payments", Port: "api"}
	lis, err := serverRuntime.Listen(context.Background(), ref, "127.0.0.1:0", httpMode)
	if err != nil {
		t.Fatal(err)
	}
	e := model.Endpoint{Tenant: "default", Service: ref.Service, Port: ref.Port, InstanceID: "one", Owner: serverPlan.Workload, Address: netip.MustParseAddrPort(lis.Addr().String()), Ready: true, Serving: true, Generation: 1, LeaseExpiresAt: time.Now().Add(time.Minute)}
	clientPlan.Outbound = []model.OutboundService{{Service: ref.Service, Port: serverPlan.Inbound[0].Port, Endpoints: []model.Endpoint{e}}}
	installPlan(t, clientRuntime, clientPlan)
	client := &Node{cfg: Config{Identity: clientIdentity, ConnectTimeout: timeout}, runtime: clientRuntime}
	server := &Node{cfg: Config{Identity: serverIdentity, ConnectTimeout: timeout}, runtime: serverRuntime}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = clientRuntime.Shutdown(ctx)
		_ = serverRuntime.Shutdown(ctx)
	})
	return client, server, lis, clientPlan
}

func assertEcho(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := io.ReadFull(c, b[:]); err != nil || b[0] != 'x' {
		t.Fatalf("echo=%q err=%v", b, err)
	}
}

func serveEcho(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
	}
}

func TestGenerationOnlyPlanPreservesConnectionAndBreaker(t *testing.T) {
	client, _, lis, p := runtimeNodePair(t, time.Second, false)
	go serveEcho(lis)
	c, err := client.Dial(context.Background(), ServiceRef{Service: "payments", Port: "api"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	assertEcho(t, c)
	before := client.runtime.Circuit(p.Outbound[0].Endpoints[0])
	p.Outbound[0].Endpoints[0].Generation++
	installPlan(t, client.runtime, p)
	assertEcho(t, c)
	if client.runtime.Circuit(p.Outbound[0].Endpoints[0]) != before {
		t.Fatal("generation discarded breaker")
	}
}

func TestSlowClientHelloDoesNotBlockAccept(t *testing.T) {
	timeout := time.Second
	client, _, lis, _ := runtimeNodePair(t, timeout, false)
	go serveEcho(lis)
	for range 100 {
		c, err := net.Dial("tcp", lis.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	c, err := client.Dial(ctx, ServiceRef{Service: "payments", Port: "api"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	assertEcho(t, c)
	if time.Since(start) >= timeout {
		t.Fatal("legal client exceeded ConnectTimeout")
	}
}

func TestHTTPClientCompletesStreamingBeyondConnectTimeout(t *testing.T) {
	timeout := 100 * time.Millisecond
	client, server, lis, _ := runtimeNodePair(t, timeout, true)
	ref := ServiceRef{Service: "payments", Port: "api"}
	srv := &http.Server{ConnContext: dataplane.PeerContext, Handler: server.runtime.HTTPHandler(ref, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for range 4 {
			_, _ = io.WriteString(w, "x")
			w.(http.Flusher).Flush()
			time.Sleep(timeout)
		}
	}))}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Close()
	hc := client.HTTPClient(ref)
	defer hc.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://payments/", nil)
	response, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "xxxx" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func TestTransientRenewalsKeepGenerationAndConnection(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.Aborted} {
		t.Run(code.String(), func(t *testing.T) {
			client, server, lis, p := runtimeNodePair(t, time.Second, false)
			go serveEcho(lis)
			c, err := client.Dial(context.Background(), ServiceRef{Service: "payments", Port: "api"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			fake := &fakeCatalogServer{failure: code, done: make(chan struct{})}
			buf := bufconn.Listen(1024 * 1024)
			gs := grpc.NewServer()
			catalogv1.RegisterCatalogServer(gs, fake)
			go func() { _ = gs.Serve(buf) }()
			defer gs.Stop()
			cc, err := grpc.NewClient("passthrough:///catalog", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return buf.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			server.conns = []*grpc.ClientConn{cc}
			server.state = fixture.OpenState(t, t.TempDir())
			server.nodeID = "server"
			b := &Listener{listener: lis, node: server, instance: "one", generation: 1, done: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go b.renew(ctx, &catalogv1.EndpointLease{ExpiresAt: timestamppb.New(time.Now().Add(2 * time.Second))})
			select {
			case <-fake.done:
			case <-time.After(3 * time.Second):
				t.Fatal("three failures not retried with same lease")
			}
			cancel()
			<-b.done
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.registrations != 0 || b.generation != 1 {
				t.Fatalf("registers=%d generation=%d", fake.registrations, b.generation)
			}
			for _, generation := range fake.generations {
				if generation != 1 {
					t.Fatalf("renewed generation=%d", generation)
				}
			}
			// Each eventual signed renewal keeps the established outbound session usable.
			p.Outbound[0].Endpoints[0].Generation = b.generation
			installPlan(t, client.runtime, p)
			assertEcho(t, c)
		})
	}
}

func TestPeerFromOrdinaryContextIsAbsent(t *testing.T) {
	if _, ok := PeerFromContext(context.Background()); ok {
		t.Fatal("unverified context has peer")
	}
	if _, ok := PeerID(nil); ok {
		t.Fatal("unverified connection has peer")
	}
}

func TestDialSharesBudgetAcrossCandidates(t *testing.T) {
	client, _, lis, p := runtimeNodePair(t, 400*time.Millisecond, false)
	go serveEcho(lis)
	stalled, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	go func() {
		c, err := stalled.Accept()
		if err == nil {
			defer c.Close()
			_, _ = io.Copy(io.Discard, c)
		}
	}()
	bad := p.Outbound[0].Endpoints[0]
	bad.InstanceID = "stalled"
	bad.Address = netip.MustParseAddrPort(stalled.Addr().String())
	p.Outbound[0].Endpoints = append([]model.Endpoint{bad}, p.Outbound[0].Endpoints...)
	installPlan(t, client.runtime, p)
	client.cfg.DialTimeout = 400 * time.Millisecond
	start := time.Now()
	c, err := client.Dial(context.Background(), ServiceRef{Service: "payments", Port: "api"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	assertEcho(t, c)
	if time.Since(start) >= client.cfg.DialTimeout {
		t.Fatal("Dial exceeded total budget")
	}
}

func TestLostLeaseReservesNewGeneration(t *testing.T) {
	for _, test := range []struct {
		name string
		code codes.Code
		ttl  time.Duration
	}{
		{"failed-precondition", codes.FailedPrecondition, 2 * time.Second},
		{"local-expiry", codes.Unavailable, 80 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, server, lis, _ := runtimeNodePair(t, time.Second, false)
			fake := &fakeCatalogServer{failure: test.code, done: make(chan struct{})}
			buf := bufconn.Listen(1024 * 1024)
			gs := grpc.NewServer()
			catalogv1.RegisterCatalogServer(gs, fake)
			go func() { _ = gs.Serve(buf) }()
			defer gs.Stop()
			cc, err := grpc.NewClient("passthrough:///catalog", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return buf.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			server.conns = []*grpc.ClientConn{cc}
			server.state = fixture.OpenState(t, t.TempDir())
			server.nodeID = "server"
			generation, err := server.state.NextMeshGeneration()
			if err != nil {
				t.Fatal(err)
			}
			b := &Listener{listener: lis, node: server, instance: "one", generation: generation, done: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go b.renew(ctx, &catalogv1.EndpointLease{ExpiresAt: timestamppb.New(time.Now().Add(test.ttl))})
			select {
			case <-fake.done:
			case <-time.After(2 * time.Second):
				t.Fatal("lost lease not registered")
			}
			cancel()
			<-b.done
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.registrations != 1 || b.generation <= generation {
				t.Fatalf("registers=%d generation=%d", fake.registrations, b.generation)
			}
		})
	}
}

func TestListenerClosesPendingHandshakesAndEnforcesLimit(t *testing.T) {
	_, server, lis, _ := runtimeNodePair(t, 5*time.Second, false)
	accepted := make(chan error, 1)
	go func() { _, err := lis.Accept(); accepted <- err }()
	for range dataplane.MaxPendingHandshakes + 20 {
		c, err := net.Dial("tcp", lis.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	deadline := time.Now().Add(time.Second)
	for {
		count := lis.PendingCount()
		if count > dataplane.MaxPendingHandshakes {
			t.Fatal("pending handshake limit exceeded")
		}
		if count == dataplane.MaxPendingHandshakes {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending handshakes were not accepted")
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Close waited for handshake timeout")
	}
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("closed listener accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("Accept remained blocked")
	}
	if count := server.runtime.ActiveConnections(); count != 0 {
		t.Fatalf("%d pending connections leaked", count)
	}
}

type delayedDialIdentity struct{ spiffe.Source }

func (s delayedDialIdentity) GetSVID(ctx context.Context) (spiffe.SVID, error) {
	select {
	case <-time.After(30 * time.Millisecond):
		return s.Source.GetSVID(ctx)
	case <-ctx.Done():
		return spiffe.SVID{}, ctx.Err()
	}
}

func TestDialBudgetExcludesUnavailableTail(t *testing.T) {
	for _, reason := range []string{"expired", "breaker-open"} {
		t.Run(reason, func(t *testing.T) {
			client, _, lis, p := runtimeNodePair(t, 500*time.Millisecond, false)
			go serveEcho(lis)
			client.runtime = dataplane.New(delayedDialIdentity{client.cfg.Identity}, 500*time.Millisecond)
			for i := range 80 {
				tail := p.Outbound[0].Endpoints[0]
				tail.InstanceID = fmt.Sprintf("tail-%d", i)
				if reason == "expired" {
					tail.LeaseExpiresAt = time.Now().Add(-time.Second)
				}
				p.Outbound[0].Endpoints = append(p.Outbound[0].Endpoints, tail)
			}
			installPlan(t, client.runtime, p)
			if reason == "breaker-open" {
				for _, e := range p.Outbound[0].Endpoints[1:] {
					circuit := client.runtime.Circuit(e)
					for range 5 {
						circuit.OnFailure()
					}
				}
			}
			c, err := client.Dial(context.Background(), ServiceRef{Service: "payments", Port: "api"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			assertEcho(t, c)
		})
	}
}
