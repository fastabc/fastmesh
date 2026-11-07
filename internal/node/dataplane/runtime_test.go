package dataplane

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/breaker"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/test/fixture"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func installPlan(t *testing.T, r *Runtime, p model.Plan) {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var cloned model.Plan
	if err := json.Unmarshal(data, &cloned); err != nil {
		t.Fatal(err)
	}
	p = cloned
	for i := range p.Outbound {
		if p.Outbound[i].Owners == nil {
			for _, endpoint := range p.Outbound[i].Endpoints {
				p.Outbound[i].Owners = append(p.Outbound[i].Owners, endpoint.Owner)
			}
		}
	}
	p.Hash = p.ContentHash()
	pipeline := apply.NewPipeline(apply.NewHolder(), nil)
	pipeline.AddCommitHook(r)
	if err := pipeline.Apply(context.Background(), &snapshot.Snapshot{MeshPlan: &p}); err != nil {
		t.Fatal(err)
	}
}

func runtimePlans(clientID, serverID spiffeid.ID) (model.Plan, model.Plan) {
	base := model.Plan{TypeURL: model.PlanTypeURL, Tenant: "default", NodeID: "server", Workload: serverID, InputRevision: 1, NotAfter: time.Now().Add(time.Minute), AccessSet: true, EndpointsSet: true}
	base.Inbound = []model.InboundService{{Service: "payments", Port: model.ServicePort{Name: "api", Number: 8443, Protocol: "tcp"}, Allow: []model.InboundAllow{{Source: clientID}}}}
	client := base
	client.NodeID, client.Workload, client.Inbound = "client", clientID, nil
	return client, base
}

func TestRuntimeIdentityExpiryRenewalAndRevocation(t *testing.T) {
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientID := spiffeid.RequireFromString(clientIdentity.svid.ID.String())
	serverID := spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	clientPlan, serverPlan := runtimePlans(clientID, serverID)
	server, client := New(serverIdentity, time.Second), New(clientIdentity, time.Second)
	ctx := context.Background()
	defer func() {
		stop, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_ = client.Shutdown(stop)
		_ = server.Shutdown(stop)
	}()
	installPlan(t, server, serverPlan)
	ref := ServiceRef{Service: "payments", Port: "api"}
	lis, err := server.Listen(ctx, ref, "127.0.0.1:0", false)
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			accepted <- c
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	endpoint := model.Endpoint{Tenant: "default", Service: "payments", Port: "api", InstanceID: "one", Owner: serverID, Address: netip.MustParseAddrPort(lis.Addr().String()), Ready: true, Serving: true, Generation: 1, LeaseExpiresAt: time.Now().Add(time.Minute)}
	clientPlan.Outbound = []model.OutboundService{{Service: "payments", Port: serverPlan.Inbound[0].Port, Endpoints: []model.Endpoint{endpoint}}}
	installPlan(t, client, clientPlan)
	conn, err := client.Dial(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var inboundConn net.Conn
	select {
	case inboundConn = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("TLS connection not accepted")
	}
	if id, ok := PeerID(inboundConn); !ok || id != clientID {
		t.Fatal("unverified or wrong peer identity")
	}
	echo := func() {
		t.Helper()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := conn.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		var data [1]byte
		if _, err := io.ReadFull(conn, data[:]); err != nil || data[0] != 'x' {
			t.Fatalf("echo=%q err=%v", data, err)
		}
	}
	echo()
	wrongPeer := New(clientIdentity, time.Second)
	wrongPlan := clientPlan
	wrongPlan.Outbound = append([]model.OutboundService(nil), clientPlan.Outbound...)
	wrongPlan.Outbound[0].Endpoints = append([]model.Endpoint(nil), clientPlan.Outbound[0].Endpoints...)
	wrongPlan.Outbound[0].Endpoints[0].Owner = clientID
	installPlan(t, wrongPeer, wrongPlan)
	if unexpected, err := wrongPeer.Dial(ctx, ref); err == nil {
		_ = unexpected.Close()
		t.Fatal("wrong endpoint identity accepted")
	}
	for range 4 {
		if c, err := wrongPeer.Dial(ctx, ref); err == nil {
			_ = c.Close()
			t.Fatal("wrong peer accepted")
		}
	}
	if _, err := wrongPeer.Dial(ctx, ref); !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("repeated handshake failures did not trip shared breaker: %v", err)
	}
	// A raw TLS client has valid credentials but no policy grant. Bypassing
	// the SDK's outbound filter must still fail at the server handshake.
	tlsConfig, err := spiffe.ClientTLSConfig(ctx, serverIdentity, []string{serverID.String()}, "test")
	if err != nil {
		t.Fatal(err)
	}
	rogue, err := (&tls.Dialer{Config: tlsConfig, NetDialer: &net.Dialer{Timeout: time.Second}}).DialContext(ctx, "tcp", lis.Addr().String())
	if err == nil {
		_ = rogue.SetDeadline(time.Now().Add(time.Second))
		_, _ = rogue.Write([]byte("x"))
		var b [1]byte
		_, err = rogue.Read(b[:])
		_ = rogue.Close()
		if err == nil {
			t.Fatal("bypassed inbound authorization")
		}
	}
	// Renewal changes only the signed deadline: the listener and existing
	// connection survive, and no second bind is performed.
	serverPlan.NotAfter = serverPlan.NotAfter.Add(time.Minute)
	installPlan(t, server, serverPlan)
	if got := server.addrs(); len(got) != 1 || got[0] != lis.Addr().String() {
		t.Fatal("renewal rebuilt listener")
	}
	echo()
	// Both plan and endpoint expiry deny new calls while preserving the old
	// connection. Expiry itself is not an active policy revocation.
	clientPlan.NotAfter = time.Now().Add(-time.Second)
	installPlan(t, client, clientPlan)
	if _, err := client.Dial(ctx, ref); !errors.Is(err, model.ErrDenied) {
		t.Fatalf("expired plan: %v", err)
	}
	echo()
	clientPlan.NotAfter = time.Now().Add(time.Minute)
	clientPlan.Outbound[0].Endpoints[0].LeaseExpiresAt = time.Now().Add(-time.Second)
	installPlan(t, client, clientPlan)
	if _, err := client.Dial(ctx, ref); !errors.Is(err, model.ErrDenied) {
		t.Fatalf("expired endpoint: %v", err)
	}
	echo()
	serverPlan.Inbound[0].Allow = nil
	start := time.Now()
	installPlan(t, server, serverPlan)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := conn.Read(b[:]); err == nil {
		t.Fatal("revoked connection stayed open")
	}
	if time.Since(start) > time.Second {
		t.Fatal("revocation exceeded one second")
	}
}

func TestRuntimeHTTPRulesFailClosedAtRawListener(t *testing.T) {
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientID := spiffeid.RequireFromString(clientIdentity.svid.ID.String())
	serverID := spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	_, p := runtimePlans(clientID, serverID)
	p.Inbound[0].RequiresL7 = true
	p.Inbound[0].Port.Protocol = "http"
	p.Inbound[0].Allow[0].L7 = &model.HTTPMatch{Method: "GET", Path: "/allowed"}
	r := New(serverIdentity, time.Second)
	installPlan(t, r, p)
	ref := ServiceRef{Service: "payments", Port: "api"}
	if _, err := r.Listen(context.Background(), ref, "127.0.0.1:0", false); !errors.Is(err, model.ErrDenied) {
		t.Fatalf("L7 service bound as raw: %v", err)
	}
	if AllowedInbound(&p, ref, clientID, false, nil) {
		t.Fatal("L7 allow became L4 allow")
	}
	lis, err := r.Listen(context.Background(), ref, "127.0.0.1:0", true)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: r.HTTPHandler(ref, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { w.WriteHeader(http.StatusNoContent) })), ConnContext: PeerContext}
	go func() { _ = httpServer.Serve(lis) }()
	defer httpServer.Close()
	tlsConfig, err := spiffe.ClientTLSConfig(context.Background(), clientIdentity, []string{serverID.String()}, "test")
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	for path, code := range map[string]int{"/allowed": http.StatusNoContent, "/denied": http.StatusForbidden} {
		response, err := client.Get("https://" + lis.Addr().String() + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != code {
			t.Fatalf("%s status=%d", path, response.StatusCode)
		}
	}
}

func TestRuntimeShutdownBoundsExistingAndPendingConnections(t *testing.T) {
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientID := spiffeid.RequireFromString(clientIdentity.svid.ID.String())
	serverID := spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	_, p := runtimePlans(clientID, serverID)
	r := New(serverIdentity, time.Second)
	installPlan(t, r, p)
	for _, ready := range []bool{false, true} {
		local, peer := net.Pipe()
		defer peer.Close()
		c := &Conn{conn: local, runtime: r, ref: ServiceRef{Service: "payments", Port: "api"}, peer: clientID, ready: ready}
		if err := r.admit(c); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := r.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown=%v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("shutdown exceeded bound")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.active) != 0 {
		t.Fatal("connections survived bounded drain")
	}
}

type lifecycleIdentity struct{ svid spiffe.SVID }

func (s lifecycleIdentity) GetSVID(context.Context) (spiffe.SVID, error) { return s.svid, nil }
func lifecycleIdentities(t *testing.T) (lifecycleIdentity, lifecycleIdentity, *x509.CertPool) {
	issue, roots := fixture.Issuer(t)
	return lifecycleIdentity{issue("controller", 2)}, lifecycleIdentity{issue("payments", 3)}, roots
}
