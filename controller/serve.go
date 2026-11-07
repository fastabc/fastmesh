package controller

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Serve runs a standalone controller until cancellation. Embedded products use
// RegisterServices to attach its RPC services to their own server.
func (c *Controller) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("mesh controller: listener required")
	}
	if len(c.cfg.ControllerAddrs) == 0 {
		c.cfg.ControllerAddrs = []string{listener.Addr().String()}
	}
	opts := []grpc.ServerOption{grpc.MaxConcurrentStreams(128)}
	if !c.cfg.DevInsecure {
		cfg, err := spiffe.ServerTLSConfig(ctx, c.cfg.Identity, c.cfg.TrustDomain, false)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(cfg)))
	}
	if c.ownsElection {
		c.elector.SetAdvertise(c.cfg.ControllerAddrs[0])
		if _, err := c.elector.TryAcquire(ctx); err != nil {
			return err
		}
	}
	gs := grpc.NewServer(opts...)
	c.RegisterServices(gs)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 2)
	go func() { done <- gs.Serve(listener) }()
	go func() { done <- c.Run(ctx) }()
	var err error
	completed := 0
	select {
	case err = <-done:
		completed++
	case <-ctx.Done():
		err = ctx.Err()
	}
	cancel()
	gs.Stop()
	for completed < 2 {
		<-done
		completed++
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Run owns expiry reconciliation and, unless injected, lease renewal. Each
// write still checks the current fence, including after a leader handoff.
func (c *Controller) Run(ctx context.Context) error {
	indexCtx, stopIndex := context.WithCancel(ctx)
	indexDone := make(chan error, 1)
	go func() { indexDone <- c.index.Run(indexCtx) }()
	defer func() { stopIndex(); <-indexDone }()
	if c.ownsElection {
		if !c.Active() {
			if _, err := c.elector.TryAcquire(ctx); err != nil {
				slog.Warn("mesh controller: lease acquisition failed", "error", err)
			}
		}
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = c.elector.Release(releaseCtx)
		}()
	}
	interval := c.cfg.ReconcileInterval
	if c.ownsElection {
		interval = min(interval, c.cfg.LeaseTTL/3)
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	backoff := min(c.cfg.ReconcileInterval, 30*time.Second)
	var retryAt time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		if c.ownsElection {
			if c.Active() {
				_, _ = c.elector.Renew(ctx)
			} else {
				_, _ = c.elector.TryAcquire(ctx)
			}
		}
		if !c.Active() {
			continue
		}
		if time.Now().Before(retryAt) {
			continue
		}
		if err := c.reconcile(ctx); err != nil {
			slog.Warn("mesh controller: reconciliation failed", "error", err, "retry_after", backoff)
			retryAt = time.Now().Add(backoff)
			backoff = min(2*backoff, 30*time.Second)
			continue
		}
		backoff = min(c.cfg.ReconcileInterval, 30*time.Second)
		retryAt = time.Time{}
	}
}
