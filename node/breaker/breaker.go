// Package breaker provides a per-target circuit breaker used by the upstream
// balancer to fail fast on a failing target and recover it through a single
// controlled probe. The state machine is the canonical three-state:
//
//	Closed   — requests flow; consecutive failures are counted.
//	Open     — requests fail fast (Allow returns ErrOpen); no upstream call.
//	HalfOpen — a single probe is admitted; success closes, failure re-opens.
//
// A Breaker is safe for concurrent use.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// State is the three-state breaker machine.
type State uint32

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Config tunes the breaker thresholds. Zero values use sensible defaults: 5
// consecutive failures and a 30s cooldown.
type Config struct {
	FailureThreshold int
	Cooldown         time.Duration
}

// ErrOpen is returned by Allow when the breaker rejects a call.
var ErrOpen = errors.New("breaker: open")

// Breaker is one circuit for one upstream target. Methods are safe for
// concurrent use.
type Breaker struct {
	cfg Config

	mu             sync.Mutex
	state          State
	consecFailures int
	openedAt       time.Time
	halfOpenInUse  bool
	now            func() time.Time // overridable for tests
}

// New constructs a Breaker with cfg defaults applied.
func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 30 * time.Second
	}
	return &Breaker{cfg: cfg, now: time.Now}
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// CanAttempt reports current eligibility without reserving a half-open probe.
// The caller must still call Allow immediately before attempting the target.
func (b *Breaker) CanAttempt() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateOpen:
		return b.now().Sub(b.openedAt) >= b.cfg.Cooldown
	case StateHalfOpen:
		return !b.halfOpenInUse
	default:
		return true
	}
}

// Allow reports whether a call is permitted: nil to proceed, ErrOpen to fail
// fast. When Open and the cooldown has elapsed, the first caller is promoted to
// HalfOpen and admitted as the single probe; concurrent callers still see
// ErrOpen until that probe reports an outcome.
func (b *Breaker) Allow() error {
	_, err := b.AllowWithProbe()
	return err
}

// AllowWithProbe also reports ownership of the single half-open probe. Callers
// that abandon that probe without an endpoint outcome must call CancelProbe.
func (b *Breaker) AllowWithProbe() (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return false, nil
	case StateOpen:
		if b.now().Sub(b.openedAt) < b.cfg.Cooldown {
			return false, ErrOpen
		}
		b.state = StateHalfOpen
		b.halfOpenInUse = true
		return true, nil
	case StateHalfOpen:
		if b.halfOpenInUse {
			return false, ErrOpen
		}
		b.halfOpenInUse = true
		return true, nil
	}
	return false, nil
}

// CancelProbe releases an abandoned probe without recording success or failure.
func (b *Breaker) CancelProbe() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateHalfOpen {
		b.halfOpenInUse = false
	}
}

// OnSuccess records a successful call: resets the failure counter and (when
// HalfOpen) closes the breaker.
func (b *Breaker) OnSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecFailures = 0
	if b.state == StateHalfOpen {
		b.state = StateClosed
		b.halfOpenInUse = false
	}
}

// OnFailure records a failed call. In Closed it bumps the consecutive-failure
// counter and opens at the threshold; in HalfOpen any failure re-opens.
func (b *Breaker) OnFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		b.consecFailures++
		if b.consecFailures >= b.cfg.FailureThreshold {
			b.state = StateOpen
			b.openedAt = b.now()
		}
	case StateHalfOpen:
		b.state = StateOpen
		b.openedAt = b.now()
		b.halfOpenInUse = false
	}
}

// RetryAt returns the next cooldown boundary, or zero when time alone will not
// change eligibility. Half-open completion must be signaled by its caller.
func (b *Breaker) RetryAt() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateOpen {
		return b.openedAt.Add(b.cfg.Cooldown)
	}
	return time.Time{}
}
