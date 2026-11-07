package dataplane

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/breaker"
)

func TestWaitServicePublicationExpiryAndCooldown(t *testing.T) {
	r := New(nil, time.Second)
	ref := ServiceRef{Service: "payments", Port: "api"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.WaitService(ctx, ref) }()
	p := &model.Plan{NotAfter: time.Now().Add(time.Minute), Outbound: []model.OutboundService{{Service: ref.Service, Port: model.ServicePort{Name: ref.Port}, Endpoints: []model.Endpoint{{Service: ref.Service, Port: ref.Port, Serving: true, LeaseExpiresAt: time.Now().Add(time.Minute)}}}}}
	e := p.Outbound[0].Endpoints[0]
	circuit := breaker.New(breaker.Config{FailureThreshold: 1, Cooldown: 30 * time.Millisecond})
	circuit.OnFailure()
	r.mu.Lock()
	r.circuits[keyOf(e)] = circuit
	r.mu.Unlock()
	r.plan.Store(p)
	if err := r.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	expired := *p
	expired.NotAfter = time.Now().Add(-time.Second)
	r.plan.Store(&expired)
	short, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if err := r.WaitService(short, ref); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired=%v", err)
	}
	r.plan.Store(nil)
	go func() { done <- r.WaitService(ctx, ref) }()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed=%v", err)
	}
}
