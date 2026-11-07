package node

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestDialFromCapturedPlanCannotBypassLiveRevocation(t *testing.T) {
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientID := spiffeid.RequireFromString(clientIdentity.svid.ID.String())
	serverID := spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	clientPlan, serverPlan := runtimePlans(clientID, serverID)
	server, client := dataplane.New(serverIdentity, time.Second), dataplane.New(clientIdentity, time.Second)
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
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); _ = conn.Close() }()
		}
	}()
	endpoint := model.Endpoint{Tenant: "default", Service: "payments", Port: "api", InstanceID: "one", Owner: serverID, Address: netip.MustParseAddrPort(lis.Addr().String()), Ready: true, Serving: true, Generation: 1, LeaseExpiresAt: time.Now().Add(time.Minute)}
	clientPlan.Outbound = []model.OutboundService{{Service: "payments", Port: serverPlan.Inbound[0].Port, Endpoints: []model.Endpoint{endpoint}}}
	installPlan(t, client, clientPlan)
	captured := client.CurrentPlan()
	n := &Node{cfg: Config{ConnectTimeout: time.Second}, runtime: client}
	observer := &countingObserver{}
	client.SetObserver(observer)
	conn, err := DialFromPlan(ctx, n, ref, captured)
	if err != nil {
		t.Fatal(err)
	}
	if got := reflect.TypeOf(conn).String(); got != "*dataplane.Conn" {
		t.Fatalf("connection type = %s", got)
	}
	_ = conn.Close()
	clientPlan.Outbound = nil
	installPlan(t, client, clientPlan)
	if conn, err := DialFromPlan(ctx, n, ref, captured); !errors.Is(err, ErrDenied) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("retired view bypassed revoked live plan: %v", err)
	}
	if _, err := DialFromPlan(ctx, n, ref, nil); !errors.Is(err, ErrDenied) {
		t.Fatalf("unloaded plan: %v", err)
	}
	if got := observer.denied.Load(); got != 1 {
		t.Fatalf("denials = %d, want only the live-policy revocation", got)
	}
}

func TestHostDialAvailabilityAndCancellationAreNotDenials(t *testing.T) {
	identity, _, _ := lifecycleIdentities(t)
	r := dataplane.New(identity, time.Second)
	o := &countingObserver{}
	r.SetObserver(o)
	n := &Node{cfg: Config{ConnectTimeout: time.Second}, runtime: r}
	ref := ServiceRef{Service: "payments", Port: "api"}
	p := model.Plan{NotAfter: time.Now().Add(time.Minute), Outbound: []model.OutboundService{{Service: ref.Service, Port: model.ServicePort{Name: ref.Port}}}}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		plan *model.Plan
		want error
	}{
		{"no plan", context.Background(), nil, ErrPlanUnavailable},
		{"absent service", context.Background(), &model.Plan{NotAfter: p.NotAfter}, ErrServiceUnavailable},
		{"no endpoints", context.Background(), &p, ErrNoEndpoints},
		{"canceled", canceled, &p, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DialFromPlan(tc.ctx, n, ref, tc.plan); !errors.Is(err, tc.want) {
				t.Fatalf("dial error = %v, want %v", err, tc.want)
			}
		})
	}
	if got := o.denied.Load(); got != 0 {
		t.Fatalf("availability failures counted as denials: %d", got)
	}
}
