package dataplane

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/breaker"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestCanceledDialDoesNotOpenCircuits(t *testing.T) {
	r := New(nil, time.Second)
	ref := ServiceRef{Service: "payments", Port: "api"}
	e := model.Endpoint{Service: ref.Service, Port: ref.Port, Address: netip.MustParseAddrPort("127.0.0.1:1"), Serving: true, LeaseExpiresAt: time.Now().Add(time.Minute)}
	p := &model.Plan{NotAfter: time.Now().Add(time.Minute), Outbound: []model.OutboundService{{Service: ref.Service, Port: model.ServicePort{Name: ref.Port}, Endpoints: []model.Endpoint{e}}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 5 {
		if _, err := r.DialPlan(ctx, ref, p); !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	}
	if r.Circuit(e).State() != breaker.StateClosed {
		t.Fatal("canceled requests opened circuit")
	}
}

type blockingRevocationObserver struct {
	Observer
	entered, release chan struct{}
}

func (o *blockingRevocationObserver) ConnRevoked(model.Direction, ServiceRef, spiffeid.ID) {
	select {
	case o.entered <- struct{}{}:
	default:
	}
	<-o.release
}

func TestRevocationClosesAllBeforeObserver(t *testing.T) {
	r := New(nil, time.Second)
	var peers []net.Conn
	for range 3 {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		r.active[&Conn{conn: a, runtime: r, ready: true}] = struct{}{}
		peers = append(peers, b)
	}
	r.zero = make(chan struct{})
	r.plan.Store(&model.Plan{NotAfter: time.Now().Add(time.Minute)})
	o := &blockingRevocationObserver{entered: make(chan struct{}, 1), release: make(chan struct{})}
	r.SetObserver(o)
	done := make(chan error, 1)
	go func() { done <- r.Reconcile() }()
	<-o.entered
	defer func() {
		close(o.release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for _, p := range peers {
		_ = p.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		var b [1]byte
		_, err := p.Read(b[:])
		if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Error("revoked connection remains open during callback")
		}
	}
}

func TestDialCancellationDuringHandshakeAndEndpointTimeout(t *testing.T) {
	for _, mode := range []string{"cancel", "parent_timeout", "endpoint_timeout"} {
		t.Run(mode, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer lis.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				c, err := lis.Accept()
				if err == nil {
					accepted <- c
				}
			}()
			r := New(nil, 30*time.Millisecond)
			e := model.Endpoint{Service: "payments", Port: "api", Address: netip.MustParseAddrPort(lis.Addr().String()), Serving: true, LeaseExpiresAt: time.Now().Add(time.Minute)}
			circuit := breaker.New(breaker.Config{FailureThreshold: 1, Cooldown: time.Nanosecond})
			if mode != "endpoint_timeout" {
				circuit.OnFailure()
			}
			r.circuits[keyOf(e)] = circuit
			p := &model.Plan{NotAfter: time.Now().Add(time.Minute), Outbound: []model.OutboundService{{Service: e.Service, Port: model.ServicePort{Name: e.Port}, Endpoints: []model.Endpoint{e, e}}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "parent_timeout" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 10*time.Millisecond)
				defer stop()
				r.timeout = time.Second
			}
			done := make(chan error, 1)
			go func() { _, err := r.DialPlan(ctx, ServiceRef{Service: e.Service, Port: e.Port}, p); done <- err }()
			var conn net.Conn
			select {
			case conn = <-accepted:
				defer conn.Close()
			case <-time.After(time.Second):
				t.Fatal("no attempt")
			}
			if mode == "cancel" {
				cancel()
			}
			err = <-done
			if mode == "endpoint_timeout" {
				if circuit.State() != breaker.StateOpen {
					t.Fatalf("state=%v error=%v", circuit.State(), err)
				}
			} else {
				if !errors.Is(err, ctx.Err()) {
					t.Fatalf("error=%v context=%v", err, ctx.Err())
				}
				if circuit.State() != breaker.StateHalfOpen || !circuit.CanAttempt() {
					t.Fatalf("abandoned probe not released: %v", circuit.State())
				}
			}
		})
	}
}
