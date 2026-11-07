package dataplane

import (
	"context"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestP2CBalancesActiveConnectionsAndFiltersCandidates(t *testing.T) {
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientID, serverID := spiffeid.RequireFromString(clientIdentity.svid.ID.String()), spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	cp, sp := runtimePlans(clientID, serverID)
	cp.TypeURL, sp.TypeURL = model.PlanTypeURL, model.PlanTypeURL
	client, server := New(clientIdentity, time.Second), New(serverIdentity, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
		_ = server.Shutdown(ctx)
	})
	installPlan(t, server, sp)
	ref := ServiceRef{Service: "payments", Port: "api"}
	endpoints := make([]model.Endpoint, 2)
	for n := range endpoints {
		lis, err := server.Listen(ctx, ref, "127.0.0.1:0", false)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				c, err := lis.Accept()
				if err != nil {
					return
				}
				go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
			}
		}()
		endpoints[n] = model.Endpoint{Tenant: "default", Service: ref.Service, Port: ref.Port, InstanceID: []string{"one", "two"}[n], Owner: serverID, Address: netip.MustParseAddrPort(lis.Addr().String()), Ready: true, Serving: true, Generation: 1, LeaseExpiresAt: time.Now().Add(time.Minute)}
	}
	cp.Outbound = []model.OutboundService{{Service: ref.Service, Port: sp.Inbound[0].Port, Owners: []spiffeid.ID{serverID}, Endpoints: endpoints}}
	installPlan(t, client, cp)
	var connections []net.Conn
	for range 20 {
		c, err := client.Dial(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, c)
	}
	client.mu.Lock()
	a, b := client.connections[keyOf(endpoints[0])], client.connections[keyOf(endpoints[1])]
	client.mu.Unlock()
	if a != 10 || b != 10 {
		t.Fatalf("P2C load=%d/%d", a, b)
	}
	for _, c := range connections {
		_ = c.Close()
		_ = c.Close()
	}
	client.mu.Lock()
	remaining := len(client.connections)
	client.mu.Unlock()
	if remaining != 0 {
		t.Fatal("closed connections retained in P2C load")
	}
	dialInstance := func(want string) {
		t.Helper()
		c, err := client.Dial(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if c.(*Conn).endpoint.InstanceID != want {
			t.Fatalf("selected %s, want %s", c.(*Conn).endpoint.InstanceID, want)
		}
	}
	endpoints[0].Terminating = true
	endpoints[0].Ready = false
	installPlan(t, client, cp)
	for range 10 {
		dialInstance("two")
	}
	// Breaker-open normal targets do not turn a draining instance into fallback.
	client.mu.Lock()
	circuit := client.circuits[keyOf(endpoints[1])]
	client.mu.Unlock()
	for range 5 {
		circuit.OnFailure()
	}
	if c, err := client.Dial(ctx, ref); err == nil {
		_ = c.Close()
		t.Fatal("selected draining fallback while a normal instance exists")
	}
	endpoints[1].Terminating = true
	endpoints[1].Ready = false
	installPlan(t, client, cp)
	dialInstance("one")
	endpoints[0].LeaseExpiresAt = time.Now().Add(-time.Second)
	installPlan(t, client, cp)
	if c, err := client.Dial(ctx, ref); err == nil {
		_ = c.Close()
		t.Fatal("selected expired or breaker-open instance")
	}
}
