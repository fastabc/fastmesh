package node

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestExplicitClientBeforePlanAndProtocolMismatch(t *testing.T) {
	n := &Node{runtime: dataplane.New(nil, time.Second)}
	ref := ServiceRef{Service: "payments", Port: "api"}
	client, err := n.HTTPClientWithProtocol(ref, "grpc")
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = client.Get("http://payments/pay")
	if !errors.Is(err, ErrPlanUnavailable) {
		t.Fatal(err)
	}
	p, _ := runtimePlans(spiffeid.RequireFromString("spiffe://test/client"), spiffeid.RequireFromString("spiffe://test/server"))
	p.Outbound = []model.OutboundService{{Service: ref.Service, Port: model.ServicePort{Name: ref.Port, Protocol: "http"}}}
	installPlan(t, n.runtime, p)
	_, err = client.Get("http://payments/pay")
	if !errors.Is(err, ErrProtocolMismatch) {
		t.Fatal(err)
	}
	if _, err = n.HTTPClientWithProtocol(ref, "invalid"); !errors.Is(err, ErrProtocolMismatch) {
		t.Fatal(err)
	}
}

func TestExplicitClientWaitsForLateEndpoint(t *testing.T) {
	client, server, _, ctx := startNodePair(t, false)
	ref := ServiceRef{Service: "payments", Port: "api"}
	hc, err := client.HTTPClientWithProtocol(ref, "http")
	if err != nil {
		t.Fatal(err)
	}
	defer hc.CloseIdleConnections()
	short, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if err := client.WaitService(short, ref); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing endpoint=%v", err)
	}
	lis, err := server.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	if err := client.WaitService(ctx, ref); err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Get("http://payments/pay")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatal(resp.StatusCode)
	}
	bs := server.Status().Bindings[ref]
	if !bs.Serving || !bs.LeaseValid {
		t.Fatalf("binding=%+v", bs)
	}
	if err := lis.SetServing(false); err != nil {
		t.Fatal(err)
	}
	if server.Status().Bindings[ref].Serving {
		t.Fatal("stale serving status")
	}
}

func TestErrorReasonsPreserveCompatibility(t *testing.T) {
	for _, err := range []error{ErrPlanUnavailable, ErrServiceUnavailable, ErrNoEndpoints} {
		if !errors.Is(err, ErrDenied) || Reason(err) == "denied" {
			t.Fatalf("classification %v", err)
		}
	}
	if Reason(context.Canceled) != "canceled" || Reason(ErrCircuitOpen) != "circuit_open" {
		t.Fatal("wrong reason")
	}
}
