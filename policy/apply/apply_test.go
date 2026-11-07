package apply

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/test/fixture"

	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/state"
)

func snap(rev snapshot.Revision) *snapshot.Snapshot {
	return &snapshot.Snapshot{Tenant: "default", NodeID: "node-1", Revision: rev}
}

func TestApplyCommitsToHolder(t *testing.T) {
	h := NewHolder()
	p := NewPipeline(h, nil)
	if err := p.Apply(context.Background(), snap("sha256:r1")); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := h.Revision(); got != "sha256:r1" {
		t.Errorf("holder revision = %q, want sha256:r1", got)
	}
	if got := p.Current(); got != "sha256:r1" {
		t.Errorf("Current() = %q, want sha256:r1", got)
	}
}

func TestApplyRejectsOnValidatorError(t *testing.T) {
	h := NewHolder()
	boom := ValidatorFunc(func(context.Context, *snapshot.Snapshot) error {
		return errors.New("invalid route")
	})
	p := NewPipeline(h, nil, boom)
	err := p.Apply(context.Background(), snap("sha256:r1"))
	if err == nil {
		t.Fatal("Apply() = nil, want validator error")
	}
	if h.Load() != nil {
		t.Error("holder was mutated despite validation failure")
	}
}

func TestApplyPersistsLastGood(t *testing.T) {
	dir := t.TempDir()
	st := fixture.OpenState(t, dir)
	p := NewPipeline(NewHolder(), st)
	if err := p.Apply(context.Background(), snap("sha256:r1")); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got, err := st.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got.Revision != "sha256:r1" {
		t.Errorf("persisted revision = %q, want sha256:r1", got.Revision)
	}
}

type hook struct {
	prepared  bool
	commits   int
	rollbacks int
}

func TestApplyCommitsHookBeforeHolderSwap(t *testing.T) {
	h := NewHolder()
	p := NewPipeline(h, nil)
	ph := &hook{}
	p.AddCommitHook(ph)

	if err := p.Apply(context.Background(), snap("sha256:r1")); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !ph.prepared || ph.commits != 1 || ph.rollbacks != 0 {
		t.Fatalf("hook state prepared=%v commits=%d rollbacks=%d", ph.prepared, ph.commits, ph.rollbacks)
	}
	if h.Revision() != "sha256:r1" {
		t.Fatalf("holder revision=%q want sha256:r1", h.Revision())
	}
}

func TestApplyDoesNotAdvanceHolderWhenHookCommitFails(t *testing.T) {
	h := NewHolder()
	h.Store(snap("sha256:live"))
	p := NewPipeline(h, nil)
	boom := errors.New("commit rejected")
	var events []string
	p.AddCommitHook(commitHookFunc(func(context.Context, *snapshot.Snapshot) (PreparedChange, error) {
		return PreparedChangeFunc{
			CommitFunc: func(context.Context) error {
				events = append(events, "first commit")
				return boom
			},
			RollbackFunc: func(context.Context) error {
				events = append(events, "first rollback")
				return nil
			},
		}, nil
	}))
	p.AddCommitHook(commitHookFunc(func(context.Context, *snapshot.Snapshot) (PreparedChange, error) {
		return PreparedChangeFunc{
			CommitFunc: func(context.Context) error {
				events = append(events, "second commit")
				return nil
			},
			RollbackFunc: func(context.Context) error {
				events = append(events, "second rollback")
				return nil
			},
		}, nil
	}))

	err := p.Apply(context.Background(), snap("sha256:rejected"))
	if !errors.Is(err, boom) {
		t.Fatalf("Apply error = %v, want commit error", err)
	}
	if got := h.Revision(); got != "sha256:live" {
		t.Fatalf("holder revision = %q, want previous live revision", got)
	}
	want := []string{"first commit", "second rollback", "first rollback"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

func TestApplyCommitFailureAfterDecisionRequiresForwardRecovery(t *testing.T) {
	dir := t.TempDir()
	st := fixture.OpenState(t, dir)
	old := snap("sha256:old")
	if err := st.SaveLastGood(old, nil); err != nil {
		t.Fatal(err)
	}
	holder := NewHolder()
	holder.Store(old)
	p := NewPipeline(holder, st)
	commits := 0
	p.AddCommitHook(commitHookFunc(func(context.Context, *snapshot.Snapshot) (PreparedChange, error) {
		return PreparedChangeFunc{CommitFunc: func(context.Context) error {
			commits++
			return nil
		}}, nil
	}))
	boom := errors.New("publish failed")
	p.AddCommitHook(commitHookFunc(func(context.Context, *snapshot.Snapshot) (PreparedChange, error) {
		return PreparedChangeFunc{CommitFunc: func(context.Context) error { return boom }}, nil
	}))

	target := snap("sha256:target")
	err := p.Apply(context.Background(), target)
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Apply error = %v, want ErrRecoveryRequired", err)
	}
	if commits != 1 {
		t.Fatalf("first hook commits = %d, want 1", commits)
	}
	if got := holder.Revision(); got != old.Revision {
		t.Fatalf("holder revision = %q, want old %q until recovery", got, old.Revision)
	}
	got, err := st.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != target.Revision {
		t.Fatalf("recoverable revision = %q, want committed target %q", got.Revision, target.Revision)
	}
	journal, err := st.LoadApplyJournal()
	if err != nil {
		t.Fatal(err)
	}
	if journal.Decision != state.ApplyDecisionCommitted {
		t.Fatalf("journal decision = %q, want committed", journal.Decision)
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	restoredHolder := NewHolder()
	restored := NewPipeline(restoredHolder, fixture.OpenState(t, dir))
	restored.AddCommitHook(&hook{})
	rev, err := restored.Restore()
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if rev != target.Revision || restoredHolder.Revision() != target.Revision {
		t.Fatalf("Restore revisions = (%q, %q), want %q", rev, restoredHolder.Revision(), target.Revision)
	}
	if _, err := st.LoadApplyJournal(); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("journal after Restore = %v, want ErrNotFound", err)
	}
}

func TestApplyPrepareFailureLeavesOldLastGoodAndNoJournal(t *testing.T) {
	st := fixture.OpenState(t, t.TempDir())
	old := snap("sha256:old")
	if err := st.SaveLastGood(old, nil); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(NewHolder(), st)
	p.AddCommitHook(commitHookFunc(func(context.Context, *snapshot.Snapshot) (PreparedChange, error) {
		return nil, errors.New("prepare failed")
	}))
	if err := p.Apply(context.Background(), snap("sha256:target")); err == nil {
		t.Fatal("Apply = nil, want prepare error")
	}
	got, err := st.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != old.Revision {
		t.Fatalf("last-good revision = %q, want %q", got.Revision, old.Revision)
	}
	if _, err := st.LoadApplyJournal(); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("journal after prepare failure = %v, want ErrNotFound", err)
	}
}

// A snapshot must not become live until it has been durably persisted as
// last-good: "live implies durable". Otherwise a restart races the persist and
// can come up with an empty data plane while the controller is offline, and a
// persist failure would leave the data plane serving a revision the controller
// was told (via NACK) had been rejected.
func TestApplyDoesNotExposeSnapshotWhenPersistFails(t *testing.T) {
	base := t.TempDir()
	st := fixture.OpenState(t, base)
	// A directory at the pending journal's destination makes atomic rename fail.
	if err := os.Mkdir(filepath.Join(base, "apply-journal.json.gz"), 0700); err != nil {
		t.Fatal(err)
	}

	h := NewHolder()
	p := NewPipeline(h, st)
	ph := &hook{}
	p.AddCommitHook(ph)

	if err := p.Apply(context.Background(), snap("sha256:r1")); err == nil {
		t.Fatal("Apply() = nil, want persistence error")
	}
	if h.Load() != nil {
		t.Error("holder exposed a snapshot that was never durably persisted")
	}
	if ph.commits != 0 || ph.rollbacks != 1 {
		t.Fatalf("hook commits=%d rollbacks=%d, want 0/1", ph.commits, ph.rollbacks)
	}
}

func TestRestoreLoadsLastGoodIntoHolder(t *testing.T) {
	dir := t.TempDir()
	st := fixture.OpenState(t, dir)
	if err := st.SaveLastGood(snap("sha256:r9"), nil); err != nil {
		t.Fatalf("seed last-good: %v", err)
	}
	h := NewHolder()
	p := NewPipeline(h, st)
	ph := &hook{}
	p.AddCommitHook(ph)
	rev, err := p.Restore()
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if rev != "sha256:r9" {
		t.Errorf("restored revision = %q, want sha256:r9", rev)
	}
	if h.Revision() != "sha256:r9" {
		t.Errorf("holder revision = %q, want sha256:r9", h.Revision())
	}
	if ph.commits != 1 || ph.rollbacks != 0 {
		t.Fatalf("hook commits=%d rollbacks=%d, want 1/0", ph.commits, ph.rollbacks)
	}
}

func TestRestoreDiscardsPendingJournalAndLoadsOldLastGood(t *testing.T) {
	st := fixture.OpenState(t, t.TempDir())
	old := snap("sha256:old")
	if err := st.SaveLastGood(old, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveApplyPending(snap("sha256:pending"), nil); err != nil {
		t.Fatal(err)
	}
	holder := NewHolder()
	p := NewPipeline(holder, st)
	rev, err := p.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if rev != old.Revision || holder.Revision() != old.Revision {
		t.Fatalf("Restore revisions = (%q, %q), want old %q", rev, holder.Revision(), old.Revision)
	}
	if _, err := st.LoadApplyJournal(); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("journal after Restore = %v, want ErrNotFound", err)
	}
}

func TestRestoreNoLastGoodReturnsEmpty(t *testing.T) {
	p := NewPipeline(NewHolder(), fixture.OpenState(t, t.TempDir()))
	rev, err := p.Restore()
	if err != nil {
		t.Fatalf("Restore with no last-good = %v, want nil", err)
	}
	if rev != "" {
		t.Errorf("restored revision = %q, want empty", rev)
	}
}

func TestWaitBlocksUntilApplyFinishes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := NewPipeline(NewHolder(), fixture.OpenState(t, t.TempDir()))
	p.AddCommitHook(commitHookFunc(func(context.Context, *snapshot.Snapshot) (PreparedChange, error) {
		close(started)
		<-release
		return PreparedChangeFunc{}, nil
	}))
	applyDone := make(chan error, 1)
	go func() { applyDone <- p.Apply(context.Background(), snap("sha256:wait")) }()
	<-started
	waitDone := make(chan struct{})
	go func() {
		p.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		t.Fatal("Wait returned while Apply was active")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-applyDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after Apply")
	}
}

type commitHookFunc func(context.Context, *snapshot.Snapshot) (PreparedChange, error)

func (h *hook) Prepare(context.Context, *snapshot.Snapshot) (PreparedChange, error) {
	h.prepared = true
	return PreparedChangeFunc{
		CommitFunc: func(context.Context) error {
			h.commits++
			return nil
		},
		RollbackFunc: func(context.Context) error {
			h.rollbacks++
			return nil
		},
	}, nil
}

func (f commitHookFunc) Prepare(ctx context.Context, snap *snapshot.Snapshot) (PreparedChange, error) {
	return f(ctx, snap)
}

func TestSentinelMessagesUsePackagePrefix(t *testing.T) {
	for err, want := range map[error]string{ErrRecoveryRequired: "apply: committed revision requires recovery"} {
		if err.Error() != want {
			t.Errorf("%q, want %q", err, want)
		}
	}
}
