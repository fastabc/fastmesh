package control

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/fastabc/fastmesh/policy/apply"
	"google.golang.org/grpc"
)

// Run connects to the controllers in addrs, streams snapshots, and applies them
// until ctx is cancelled. Each stream failure rotates to the next address
// (failover) after a backoff, so it returns only when ctx is done.
func (c *Client) Run(ctx context.Context, addrs []string) error {
	if len(addrs) == 0 {
		return errors.New("ads: no controller addresses")
	}
	backoff := c.cfg.Backoff
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.address = addrs[i%len(addrs)]
		err := c.connectAndStream(ctx, c.address)
		if c.cfg.ControlContact != nil {
			c.cfg.ControlContact(c.address, err)
		}
		if c.cfg.StopOnError != nil && c.cfg.StopOnError(err) {
			return err
		}
		var cfgErr *dialConfigError
		if errors.As(err, &cfgErr) {
			return cfgErr.Unwrap()
		}
		if errors.Is(err, apply.ErrRecoveryRequired) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.contacted.Swap(false) {
			backoff = c.cfg.Backoff
		}
		delay := reconnectDelay(backoff)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			backoff = min(backoff*2, 30*time.Second)
		}
	}
}

func reconnectDelay(backoff time.Duration) time.Duration {
	return backoff/2 + time.Duration(rand.Int64N(max(int64(backoff-backoff/2), 1)))
}

func (c *Client) connectAndStream(ctx context.Context, addr string) error {
	opts, err := c.cfg.DialOptions(ctx)
	if err != nil {
		return &dialConfigError{err: err}
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	return c.Session(ctx, conn)
}

type dialConfigError struct {
	err error
}

func (e *dialConfigError) Error() string { return e.err.Error() }

func (e *dialConfigError) Unwrap() error { return e.err }
