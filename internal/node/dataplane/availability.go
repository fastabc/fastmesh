package dataplane

import (
	"context"
	"net"
	"time"
)

// signalAvailability requires mu. It wakes waiters without retaining events.
func (r *Runtime) signalAvailability() { close(r.changed); r.changed = make(chan struct{}) }
func (r *Runtime) notifyAvailability() { r.mu.Lock(); r.signalAvailability(); r.mu.Unlock() }

// WaitService observes a local selectable endpoint; it does not test peer health.
func (r *Runtime) WaitService(ctx context.Context, ref ServiceRef) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		changed, closed := r.changed, r.closed
		var retry time.Time
		for key, c := range r.circuits {
			if key.Service != ref.Service || key.Port != ref.Port {
				continue
			}
			at := c.RetryAt()
			if at.After(time.Now()) && (retry.IsZero() || at.Before(retry)) {
				retry = at
			}
		}
		r.mu.Unlock()
		if closed {
			return net.ErrClosed
		}
		if r.DialableCounts(r.CurrentPlan(), time.Now())[ref] > 0 {
			return nil
		}
		var timer *time.Timer
		var tick <-chan time.Time
		if !retry.IsZero() {
			timer = time.NewTimer(time.Until(retry))
			tick = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()
		case <-changed:
		case <-tick:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}
