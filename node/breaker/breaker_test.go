package breaker

import (
	"errors"
	"testing"
	"time"
)

func TestClosedOpensAtThreshold(t *testing.T) {
	b := New(Config{FailureThreshold: 3})
	for i := 0; i < 2; i++ {
		b.OnFailure()
		if b.State() != StateClosed {
			t.Fatalf("state after %d failures = %v, want closed", i+1, b.State())
		}
		if b.Allow() != nil {
			t.Fatal("closed breaker should allow")
		}
	}
	b.OnFailure()
	if b.State() != StateOpen {
		t.Fatalf("state = %v, want open after threshold", b.State())
	}
}

func TestOpenFailsFastDuringCooldown(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(Config{FailureThreshold: 1, Cooldown: 10 * time.Second})
	b.now = func() time.Time { return now }
	b.OnFailure() // -> open at now
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow at open = %v, want ErrOpen", err)
	}
	now = now.Add(5 * time.Second)
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow mid-cooldown = %v, want ErrOpen", err)
	}
}

func TestCooldownPromotesSingleHalfOpenProbe(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(Config{FailureThreshold: 1, Cooldown: 10 * time.Second})
	b.now = func() time.Time { return now }
	b.OnFailure()
	now = now.Add(10 * time.Second)
	if err := b.Allow(); err != nil {
		t.Fatalf("first Allow after cooldown = %v, want nil (probe)", err)
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v, want half_open", b.State())
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("second Allow = %v, want ErrOpen (single probe)", err)
	}
}

func TestHalfOpenSuccessCloses(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(Config{FailureThreshold: 1, Cooldown: time.Second})
	b.now = func() time.Time { return now }
	b.OnFailure()
	now = now.Add(time.Second)
	_ = b.Allow() // half-open probe
	b.OnSuccess()
	if b.State() != StateClosed {
		t.Fatalf("state = %v, want closed after probe success", b.State())
	}
	if b.Allow() != nil {
		t.Fatal("closed breaker should allow")
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(Config{FailureThreshold: 1, Cooldown: time.Second})
	b.now = func() time.Time { return now }
	b.OnFailure()
	now = now.Add(time.Second)
	_ = b.Allow() // half-open probe
	b.OnFailure()
	if b.State() != StateOpen {
		t.Fatalf("state = %v, want open after probe failure", b.State())
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{
		StateClosed: "closed", StateOpen: "open",
		StateHalfOpen: "half_open", State(99): "unknown",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("State(%d).String() = %q, want %q", s, got, want)
		}
	}
}

func TestCanAttemptPreservesCooldownProbe(t *testing.T) {
	b := New(Config{FailureThreshold: 1, Cooldown: time.Second})
	now := time.Now()
	b.now = func() time.Time { return now }
	if !b.CanAttempt() {
		t.Fatal("closed breaker unavailable")
	}
	b.OnFailure()
	if b.CanAttempt() {
		t.Fatal("open breaker available before cooldown")
	}
	now = now.Add(time.Second)
	if !b.CanAttempt() || !b.CanAttempt() {
		t.Fatal("eligibility query reserved cooldown probe")
	}
	if err := b.Allow(); err != nil {
		t.Fatal(err)
	}
	if b.CanAttempt() {
		t.Fatal("in-use half-open probe available")
	}
	b.OnSuccess()
	if !b.CanAttempt() {
		t.Fatal("successful probe did not restore eligibility")
	}
}

func TestCancelProbePreservesFailuresAndAllowsNextProbe(t *testing.T) {
	b := New(Config{FailureThreshold: 1, Cooldown: time.Second})
	now := time.Unix(1, 0)
	b.now = func() time.Time { return now }
	b.OnFailure()
	now = now.Add(time.Second)
	probe, err := b.AllowWithProbe()
	if err != nil || !probe {
		t.Fatalf("probe=%v error=%v", probe, err)
	}
	b.CancelProbe()
	if b.State() != StateHalfOpen || !b.CanAttempt() {
		t.Fatal("probe not released")
	}
	probe, err = b.AllowWithProbe()
	if err != nil || !probe {
		t.Fatal("next probe denied")
	}
	b.OnFailure()
	if b.State() != StateOpen {
		t.Fatal("real failure did not reopen")
	}
}
