package node

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/store/memory"
)

func TestFirstEnrollmentAdoptsControllerAddresses(t *testing.T) {
	issue, _ := lifecycleIssuer(t)
	nodeIdentity, controllerIdentity := issue("payments", 2), issue("controller", 3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	advertised := lis.Addr().String()
	_, port, _ := net.SplitHostPort(advertised)
	backing := memory.New()
	c, err := controller.New(controller.Config{ID: "enroll", Objects: backing, Leases: backing, Identity: controllerIdentity, TrustDomain: "test", ControllerAddrs: []string{advertised}, ReconcileInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Serve(cctx, lis) }()
	defer func() { stop(); <-done }()
	for !c.Active() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	token, err := c.CreateJoinToken(ctx, enrollment.CreateRequest{Tenant: "default"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(Config{Identity: nodeIdentity, Controllers: []string{net.JoinHostPort("localhost", port)}, AllowedControllerIDs: []string{controllerIdentity.svid.ID.String()}, TrustDomain: "test", Tenant: "default", JoinToken: token.Raw, StateDir: t.TempDir(), ConnectTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(n.cfg.Controllers, []string{advertised}) {
		t.Fatalf("controllers = %v, want %v", n.cfg.Controllers, advertised)
	}
	if len(n.conns) != 1 || n.conns[0].Target() != advertised {
		t.Fatalf("connections were not replaced with registration addresses")
	}
	if id, err := n.state.LoadNodeID(); err != nil || id != n.nodeID {
		t.Fatalf("persisted node id = %q, %v; want %q", id, err, n.nodeID)
	}
}
