package dataplane

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestPlanConnectionsFollowOwnersRatherThanEndpointTopology(t *testing.T) {
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientID, serverID := spiffeid.RequireFromString(clientIdentity.svid.ID.String()), spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	cp, sp := runtimePlans(clientID, serverID)
	cp.TypeURL, sp.TypeURL = model.PlanTypeURL, model.PlanTypeURL
	server, client := New(serverIdentity, time.Second), New(clientIdentity, time.Second)
	ctx := context.Background()
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(ctx, 50*time.Millisecond)
		defer stop()
		_ = client.Shutdown(ctx)
		_ = server.Shutdown(ctx)
	})
	installPlan(t, server, sp)
	ref := ServiceRef{Service: "payments", Port: "api"}
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
	e := model.Endpoint{Tenant: "default", Service: ref.Service, Port: ref.Port, InstanceID: "one", Owner: serverID, Address: netip.MustParseAddrPort(lis.Addr().String()), Ready: true, Serving: true, Generation: 1, LeaseExpiresAt: time.Now().Add(time.Minute)}
	cp.Outbound = []model.OutboundService{{Service: ref.Service, Port: sp.Inbound[0].Port, Owners: []spiffeid.ID{serverID}, Endpoints: []model.Endpoint{e}}}
	installPlan(t, client, cp)
	c, err := client.Dial(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	echo := func() {
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
	for _, change := range []string{"generation", "terminating", "expired", "deleted"} {
		t.Run(change, func(t *testing.T) {
			switch change {
			case "generation":
				cp.Outbound[0].Endpoints[0].Generation++
			case "terminating":
				cp.Outbound[0].Endpoints[0].Ready, cp.Outbound[0].Endpoints[0].Terminating = false, true
			case "expired":
				cp.Outbound[0].Endpoints[0].LeaseExpiresAt = time.Now().Add(-time.Second)
			case "deleted":
				cp.Outbound[0].Endpoints = nil
			}
			installPlan(t, client, cp)
			echo()
			if change == "terminating" {
				conn, err := client.Dial(ctx, ref)
				if err != nil {
					t.Fatalf("all-terminating serving fallback: %v", err)
				}
				_ = conn.Close()
			} else if change != "generation" {
				if conn, err := client.Dial(ctx, ref); !errors.Is(err, model.ErrDenied) {
					if conn != nil {
						_ = conn.Close()
					}
					t.Fatalf("new connection selected %s: %v", change, err)
				}
			}
		})
	}
	// Bind revocation changes Owners even when topology was already empty.
	cp.Outbound[0].Owners = nil
	start := time.Now()
	observer := &blockingRevocationObserver{entered: make(chan struct{}, 1), release: make(chan struct{})}
	client.SetObserver(observer)
	// Publish a revoked owner set, then reconcile with notification blocked.
	revoked := *client.CurrentPlan()
	revoked.Outbound = append([]model.OutboundService(nil), revoked.Outbound...)
	revoked.Outbound[0].Owners = nil
	client.plan.Store(&revoked)
	done := make(chan error, 1)
	go func() { done <- client.Reconcile() }()
	<-observer.entered
	defer func() {
		close(observer.release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	_ = c.SetReadDeadline(start.Add(time.Second))
	var b [1]byte
	if _, err := c.Read(b[:]); err == nil || time.Since(start) >= time.Second {
		t.Fatalf("Bind revocation did not promptly close connection: %v", err)
	}
}
