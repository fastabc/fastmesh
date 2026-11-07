package node

import (
	"context"
	"errors"

	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/state"

	"github.com/fastabc/fastmesh/model"
)

// WithProjection attaches the node to a host's existing journal and pipeline.
// The host restores the pipeline before starting either control stream and
// uses HostApplier for its independently verified host updates. Do not
// also register projection as a pipeline hook: this option registers it once.
func WithProjection(pipeline *apply.Pipeline, store *state.Store, projection ComposedProjection) Option {
	return func(n *Node) error {
		if pipeline == nil || store == nil || projection == nil {
			return errors.New("mesh: incomplete composed projection")
		}
		if n.composition != nil {
			return errors.New("mesh: duplicate composed projection")
		}
		n.state, n.pipeline = store, pipeline
		n.runtime.SetPlanSource(projection.MeshPlan)
		n.composition = &composition{pipeline: pipeline, node: n, projection: projection}
		pipeline.AddCommitHook(n.composition)
		return nil
	}
}

// HostApplier merges verified host records with the latest verified mesh
// plan under the journal lock. Its Snapshot excludes the local-only MeshPlan,
// so host reconstruction and canonical hashes remain unchanged.
func HostApplier(n *Node) apply.Applier {
	if n.composition == nil {
		return nil
	}
	return composedInput{n.composition, false}
}

type composition struct {
	pipeline   *apply.Pipeline
	node       *Node
	projection ComposedProjection
}

func (c *composition) Prepare(ctx context.Context, snap *snapshot.Snapshot) (apply.PreparedChange, error) {
	p, err := c.node.verifyStoredPlan(ctx, snap)
	if err != nil {
		return nil, err
	}
	// Recovery reconstructs the execution copy from authenticated raw bytes.
	snap.MeshPlan = p
	if p := snap.MeshPlan; p != nil {
		if !p.InputValid() {
			return nil, model.ErrInvalid
		}
		id, err := c.node.state.LoadNodeID()
		if err != nil {
			return nil, err
		}
		svid, err := c.node.cfg.Identity.GetSVID(ctx)
		if err != nil {
			return nil, err
		}
		if p.NodeID != id || p.Tenant != c.node.cfg.Tenant || p.Workload.String() != svid.ID.String() {
			return nil, ErrDenied
		}
	}
	// Only the host's serving tuple receives monotonic deadlines. Keep the
	// signed input intact in the journal/holder, including its wire encoding.
	serving := *snap
	serving.MeshPlan = c.node.runtime.ExecutionPlan(snap.MeshPlan)
	prepared, err := c.projection.Prepare(ctx, &serving)
	if err != nil {
		return nil, err
	}
	return apply.PreparedChangeFunc{CommitFunc: func(ctx context.Context) error {
		if err := prepared.Commit(ctx); err != nil {
			return err
		}
		c.node.runtime.CommitInputPlan(snap.MeshPlan)
		if err := c.node.runtime.Reconcile(); err != nil {
			return err
		}
		c.node.runtime.Applied(snap.MeshPlan)
		return nil
	}, RollbackFunc: prepared.Rollback}, nil
}

type composedInput struct {
	*composition
	mesh bool
}

func (a composedInput) Snapshot() *snapshot.Snapshot {
	current := a.pipeline.Snapshot()
	if current == nil || a.mesh {
		return current
	}
	if current.Revision == "" {
		return nil
	}
	copy := *current
	copy.MeshPlan = nil
	copy.MeshPlanEnvelope = nil
	return &copy
}
func (a composedInput) Current() snapshot.Revision {
	if s := a.Snapshot(); s != nil {
		return s.Revision
	}
	return ""
}
func (a composedInput) Apply(ctx context.Context, incoming *snapshot.Snapshot) error {
	if incoming == nil {
		return model.ErrInvalid
	}
	if !a.mesh && (incoming.MeshPlan != nil || incoming.MeshPlanEnvelope != nil) {
		return errors.New("mesh: host input contains local plan")
	}
	return a.pipeline.Update(ctx, func(current *snapshot.Snapshot) (*snapshot.Snapshot, error) {
		next := *incoming
		if a.mesh {
			next = snapshot.Snapshot{Tenant: incoming.Tenant, NodeID: incoming.NodeID}
			if current != nil {
				next = *current
			}
			next.MeshPlan = incoming.MeshPlan
			next.MeshPlanEnvelope = incoming.MeshPlanEnvelope
		} else if current != nil {
			next.MeshPlan = current.MeshPlan
			next.MeshPlanEnvelope = current.MeshPlanEnvelope
		}
		return &next, nil
	})
}

// ComposedProjection materializes a host snapshot and exposes its mesh plan.
type ComposedProjection interface {
	apply.Projection
	MeshPlan() *model.Plan
}
