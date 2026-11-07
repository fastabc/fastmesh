package node

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/protocol/planwire"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func (n *Node) verifier(ctx context.Context) (verify.SnapshotVerifier, error) {
	v, svid, err := n.snapshotVerifier(ctx)
	if err != nil {
		return verify.SnapshotVerifier{}, err
	}
	if svid.ID.String() != n.runtime.Recipient().Workload.String() {
		return verify.SnapshotVerifier{}, ErrDenied
	}
	return v, nil
}

func (n *Node) snapshotVerifier(ctx context.Context) (verify.SnapshotVerifier, spiffe.SVID, error) {
	svid, err := n.cfg.Identity.GetSVID(ctx)
	if err != nil {
		return verify.SnapshotVerifier{}, spiffe.SVID{}, err
	}
	roots, err := spiffe.BundlePool(svid)
	if err != nil {
		return verify.SnapshotVerifier{}, spiffe.SVID{}, err
	}
	return verify.SnapshotVerifier{Roots: roots, TrustDomain: svid.ID.TrustDomain, AllowedControllerIDs: n.cfg.AllowedControllerIDs}, svid, nil
}

// verifyStoredPlan reconstructs execution state from the authenticated input,
// both on normal apply and journal recovery. Parsed local fields are not trusted.
func (n *Node) verifyStoredPlan(ctx context.Context, snap *snapshot.Snapshot) (*model.Plan, error) {
	if snap == nil {
		return nil, model.ErrInvalid
	}
	envelope := snap.MeshPlanEnvelope
	if envelope == nil {
		if snap.MeshPlan != nil {
			return nil, errors.New("mesh: missing raw signed plan")
		}
		return snap.MeshPlan, nil
	}
	v, svid, err := n.snapshotVerifier(ctx)
	if err != nil {
		return nil, err
	}
	restoreNotAfter, err := verify.StoredPlanBody(ctx, envelope.TypeURL, envelope.Body, envelope.Signature, v)
	if err != nil {
		return nil, err
	}
	p, err := planwire.DecodePlanBody(envelope.Body)
	if err != nil {
		return nil, err
	}
	id, err := n.state.LoadNodeID()
	if err != nil {
		return nil, err
	}
	workload, err := spiffeid.FromString(svid.ID.String())
	if err != nil {
		return nil, err
	}
	highWater, err := n.state.InputRevision()
	if err != nil {
		return nil, err
	}
	recipient := model.Session{Tenant: n.cfg.Tenant, NodeID: id, Workload: workload}
	// Expired input can be restored without admitting new connections. Check
	// every other header invariant while preserving its original deadline.
	if err := verify.PlanRecipient(p, recipient, highWater, p.NotAfter.Add(-time.Nanosecond)); err != nil {
		return nil, err
	}
	p.RestoreNotAfter = restoreNotAfter
	// Reapplying the same authenticated input must preserve the runtime's
	// credential-refresh path. Equality is checked only after full validation;
	// altered or absent parsed state is still replaced from the signed body.
	if snap.MeshPlan != nil && reflect.DeepEqual(p, *snap.MeshPlan) {
		return snap.MeshPlan, nil
	}
	return &p, nil
}
