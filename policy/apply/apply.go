package apply

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/fastabc/fastmesh/policy/material"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/state"
)

// ErrRecoveryRequired means a durable commit decision may already name the
// target revision, but one or more local publication/checkpoint steps did not
// finish. The control stream must fail closed without ACKing or NACKing the
// target; startup Restore will complete the committed decision forward.
var ErrRecoveryRequired = errors.New("apply: committed revision requires recovery")

// Validator runs a pre-commit check. Validators run in order; the first error
// aborts the apply and the agent reports a NACK upstream.
type Validator interface {
	Validate(ctx context.Context, snap *snapshot.Snapshot) error
}

// ValidatorFunc adapts a plain function to Validator.
type ValidatorFunc func(ctx context.Context, snap *snapshot.Snapshot) error

// Validate implements Validator.
func (f ValidatorFunc) Validate(ctx context.Context, snap *snapshot.Snapshot) error {
	return f(ctx, snap)
}

// CommitHook stages side effects that must track the live snapshot revision.
type CommitHook interface {
	Prepare(ctx context.Context, snap *snapshot.Snapshot) (PreparedChange, error)
}

// Applier commits a runtime snapshot and reports the live revision. The ADS
// client depends on this narrow interface so it can be tested without a real
// data plane.
type Applier interface {
	Apply(ctx context.Context, snap *snapshot.Snapshot) error
	Current() snapshot.Revision
	Snapshot() *snapshot.Snapshot
}

// MaterialResolver optionally captures host-resolved secrets for recovery.
// Mesh-only pipelines do not need a resolver.
type MaterialResolver interface {
	BeginApply()
	BeginRestore([]material.ResolvedMaterial)
	SeedSnapshotMaterials(*snapshot.Snapshot)
	Captured() []material.ResolvedMaterial
	Finish()
}

// Pipeline is the concrete Applier backed by a data-plane holder and optional
// last-good persistence.
type Pipeline struct {
	mu         sync.Mutex
	holder     *Holder
	state      *state.Store
	materials  MaterialResolver
	validators []Validator
	hooks      []CommitHook
}

var _ Applier = (*Pipeline)(nil)

// NewPipeline returns a Pipeline that commits into holder. If st is non-nil,
// each successful apply is persisted as last-good and Restore can reload it.
func NewPipeline(holder *Holder, st *state.Store, validators ...Validator) *Pipeline {
	return &Pipeline{holder: holder, state: st, validators: validators}
}

// AddCommitHook registers a hook that stages side effects for each successful
// snapshot commit. Configure hooks before Apply, Update or Restore can run.
func (p *Pipeline) AddCommitHook(h CommitHook) {
	if h != nil {
		p.hooks = append(p.hooks, h)
	}
}

// SetMaterialResolver binds apply-time capture and restore-time local-only
// resolution to this pipeline. The same resolver must be passed to hooks that
// resolve snapshot SecretRefs. Configure it before Apply, Update or Restore.
func (p *Pipeline) SetMaterialResolver(resolver MaterialResolver) {
	p.materials = resolver
}

// Apply validates and prepares snap, records a durable commit decision, then
// publishes every hook and the holder. Validation, prepare, or pending-journal
// failures leave the old revision live and may be NACKed. Once the decision is
// committed, any later failure is ErrRecoveryRequired and must be completed
// forward rather than reported as a rejected revision.
func (p *Pipeline) Apply(ctx context.Context, snap *snapshot.Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.apply(ctx, snap)
}

// Update merges an independently verified input with the current local record
// under the same lock as persistence and publication. update must not call the
// pipeline recursively. This prevents concurrent control streams losing fields.
func (p *Pipeline) Update(ctx context.Context, update func(*snapshot.Snapshot) (*snapshot.Snapshot, error)) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap, err := update(p.holder.Load())
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return p.apply(ctx, snap)
}

func (p *Pipeline) apply(ctx context.Context, snap *snapshot.Snapshot) error {
	for _, v := range p.validators {
		if err := v.Validate(ctx, snap); err != nil {
			return err
		}
	}
	if p.materials != nil {
		p.materials.BeginApply()
		defer p.materials.Finish()
		// Material the controller resolved for this node travels inside the
		// snapshot; seed it before Prepare so refs whose provider only the
		// control plane can reach never hit the local provider set.
		p.materials.SeedSnapshotMaterials(snap)
	}
	changes, err := p.prepare(ctx, snap)
	if err != nil {
		return err
	}
	decisionCommitted := false
	if p.state != nil {
		var materials []material.ResolvedMaterial
		if p.materials != nil {
			materials = p.materials.Captured()
			defer material.ClearResolvedMaterials(materials)
		}
		if err := p.state.SaveApplyPending(snap, materials); err != nil {
			rollback(ctx, changes)
			return err
		}
		if err := p.state.CommitApply(); err != nil {
			rollback(ctx, changes)
			if errors.Is(err, state.ErrCommitDecisionUncertain) {
				return recoveryRequired(err)
			}
			return errors.Join(err, p.state.ClearApplyJournal())
		}
		decisionCommitted = true
	}
	if err := commit(ctx, changes); err != nil {
		if decisionCommitted {
			return recoveryRequired(err)
		}
		return err
	}
	p.holder.Store(snap)
	if decisionCommitted {
		if err := p.state.CompleteApply(); err != nil {
			return recoveryRequired(err)
		}
	}
	return nil
}

// Current returns the live revision, or empty if nothing has been applied.
func (p *Pipeline) Current() snapshot.Revision { return p.holder.Revision() }

// Snapshot returns the current live snapshot, or nil if nothing has been
// applied. The returned snapshot is treated as immutable by callers.
func (p *Pipeline) Snapshot() *snapshot.Snapshot { return p.holder.Load() }

// Wait blocks until any in-flight Apply or Restore has left the transactional
// section. Process shutdown uses it before closing the live data plane so no
// last-good writer can outlive the agent lifecycle.
func (p *Pipeline) Wait() {
	p.mu.Lock()
	//lint:ignore SA2001 Lock/unlock is an intentional barrier for an in-flight transaction.
	p.mu.Unlock()
}

// Restore loads the persisted last-good snapshot into the holder so the data
// plane can serve immediately at startup. It returns the restored revision, or
// empty if no last-good snapshot exists.
func (p *Pipeline) Restore() (snapshot.Revision, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == nil {
		return "", nil
	}
	var (
		snap              *snapshot.Snapshot
		materials         []material.ResolvedMaterial
		decisionCommitted bool
	)
	journal, err := p.state.LoadApplyJournal()
	switch {
	case err == nil && journal.Decision == state.ApplyDecisionCommitted:
		snap = journal.Snapshot
		materials = journal.Materials
		decisionCommitted = true
	case err == nil && journal.Decision == state.ApplyDecisionPending:
		material.ClearResolvedMaterials(journal.Materials)
		if err := p.state.ClearApplyJournal(); err != nil {
			return "", err
		}
		snap, materials, err = p.state.LoadLastGood()
	case errors.Is(err, state.ErrNotFound):
		snap, materials, err = p.state.LoadLastGood()
	default:
		return "", err
	}
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	defer material.ClearResolvedMaterials(materials)
	if p.materials != nil {
		p.materials.BeginRestore(materials)
		defer p.materials.Finish()
	}
	ctx := context.Background()
	changes, err := p.prepare(ctx, snap)
	if err != nil {
		return "", err
	}
	if err := commit(ctx, changes); err != nil {
		if decisionCommitted {
			return "", recoveryRequired(err)
		}
		return "", err
	}
	p.holder.Store(snap)
	if decisionCommitted {
		if err := p.state.CompleteApply(); err != nil {
			return "", recoveryRequired(err)
		}
	}
	return snap.Revision, nil
}

func recoveryRequired(cause error) error {
	return fmt.Errorf("%w: %v", ErrRecoveryRequired, cause)
}

func (p *Pipeline) prepare(ctx context.Context, snap *snapshot.Snapshot) ([]PreparedChange, error) {
	changes := make([]PreparedChange, 0, len(p.hooks))
	for _, h := range p.hooks {
		change, err := h.Prepare(ctx, snap)
		if err != nil {
			rollback(ctx, changes)
			return nil, err
		}
		if change != nil {
			changes = append(changes, change)
		}
	}
	return changes, nil
}

func commit(ctx context.Context, changes []PreparedChange) error {
	for i, change := range changes {
		if err := change.Commit(ctx); err != nil {
			// The failed change did not publish. Release it and every change
			// that is still only prepared; earlier changes have committed.
			rollback(ctx, changes[i:])
			return err
		}
	}
	return nil
}

func rollback(ctx context.Context, changes []PreparedChange) {
	for i := len(changes) - 1; i >= 0; i-- {
		_ = changes[i].Rollback(ctx)
	}
}
