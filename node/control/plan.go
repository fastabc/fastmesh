package control

import (
	"context"
	"errors"
	"sync"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/internal/protocol/planwire"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
)

// PlanApplierConfig connects the independent plan envelope to the existing
// transactional pipeline. Verifier should read current identity roots on each
// call; HighWater reads the durable journal watermark, including after restart.
type PlanApplierConfig struct {
	Recipient model.Session
	Pipeline  apply.Applier
	Verifier  func(context.Context) (verify.SnapshotVerifier, error)
	HighWater func() (int64, error)
}

type PlanApplier struct {
	mu  sync.Mutex
	cfg PlanApplierConfig
}

func NewPlanApplier(cfg PlanApplierConfig) (*PlanApplier, error) {
	if cfg.Pipeline == nil || cfg.Verifier == nil || cfg.HighWater == nil || cfg.Recipient.NodeID == "" || !model.ExactName(cfg.Recipient.Tenant) || cfg.Recipient.Workload.IsZero() {
		return nil, errors.New("mesh: incomplete plan applier configuration")
	}
	return &PlanApplier{cfg: cfg}, nil
}

func (a *PlanApplier) DiscoveryApplier() Applier {
	return Applier{Loaded: a.Loaded, Current: a.Current, Full: a.Full, Delta: a.Delta}
}

func (a *PlanApplier) Loaded() bool {
	s := a.cfg.Pipeline.Snapshot()
	return s != nil && s.MeshPlan != nil && s.MeshPlan.EnvelopeValid() && dataplane.Fresh(s.MeshPlan)
}

func (a *PlanApplier) Current() string {
	s := a.cfg.Pipeline.Snapshot()
	if s == nil || s.MeshPlan == nil {
		return ""
	}
	return s.MeshPlan.Hash
}

func (a *PlanApplier) Full(ctx context.Context, r *discoveryv1.DiscoveryResponse) error {
	return a.accept(ctx, r.GetTypeUrl(), r.GetVersionInfo(), r.GetResources(), r.GetSnapshotSignature())
}

func (a *PlanApplier) Delta(ctx context.Context, r *discoveryv1.DeltaDiscoveryResponse) error {
	if len(r.GetRemovedResources()) != 0 {
		return errors.New("mesh: plan must be replaced by a complete signed envelope")
	}
	return a.accept(ctx, r.GetTypeUrl(), r.GetSystemVersionInfo(), r.GetResources(), r.GetSnapshotSignature())
}

func (a *PlanApplier) accept(ctx context.Context, typeURL, revision string, resources []*discoveryv1.Resource, signature *discoveryv1.SnapshotSignature) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if typeURL != model.PlanTypeURL || len(resources) != 1 || signature == nil || resources[0].GetResource().GetTypeUrl() != typeURL {
		return errors.New("mesh: missing signed plan envelope")
	}
	highWater, err := a.cfg.HighWater()
	if err != nil {
		return err
	}
	current := a.cfg.Pipeline.Snapshot()
	if current != nil && current.MeshPlan != nil {
		highWater = max(highWater, current.MeshPlan.InputRevision)
	}
	v, err := a.cfg.Verifier(ctx)
	if err != nil {
		return err
	}
	sig := snapshot.Signature{Algorithm: signature.Algorithm, KeyID: signature.KeyId, Value: signature.Value, CertChain: signature.CertChain}
	var p model.Plan
	var envelope *snapshot.SignedMeshPlan
	body := resources[0].GetResource().GetValue()
	if err := verify.PlanBody(ctx, typeURL, body, sig, v); err != nil {
		return err
	}
	p, err = planwire.DecodePlan(resources[0])
	if err == nil {
		err = verify.PlanRecipient(p, a.cfg.Recipient, highWater, time.Now())
	}
	sig.Value = append([]byte(nil), sig.Value...)
	chain := make([][]byte, len(sig.CertChain))
	for i, cert := range sig.CertChain {
		chain[i] = append([]byte(nil), cert...)
	}
	sig.CertChain = chain
	envelope = &snapshot.SignedMeshPlan{TypeURL: typeURL, Body: append([]byte(nil), body...), Signature: sig}
	if err != nil {
		return err
	}
	if revision != p.Hash {
		return errors.New("mesh: envelope revision mismatch")
	}
	if current != nil && current.MeshPlan != nil {
		old := current.MeshPlan
		if old.RestoreNotAfter.IsZero() && p.TypeURL == old.TypeURL && p.InputRevision == old.InputRevision && p.Hash == old.Hash && !p.NotAfter.After(old.NotAfter) {
			// ACK a verified duplicate without extending its signed deadline.
			return nil
		}
	}
	// Composition stays local: the host resources retain their wire hash.
	// Every accepted deadline/watermark update passes the durable pipeline;
	// its projection can renew deadlines without rebuilding unchanged topology.
	next := snapshot.Snapshot{Tenant: p.Tenant, NodeID: p.NodeID, Revision: snapshot.Revision(p.Hash)}
	if current != nil {
		next = *current
	}
	next.MeshPlan = &p
	next.MeshPlanEnvelope = envelope
	return a.cfg.Pipeline.Apply(ctx, &next)
}
