package node

import (
	"context"
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/verify"
)

func TestAuthenticatedStoredPlanReapplicationPreservesPointer(t *testing.T) {
	_, signer, identity := historicalIdentities(t)
	record, _ := historicalRecord(t, signer, "orders", identity)
	n, err := newPlanTestNode(t, Config{Identity: identity, Controllers: []string{"unused"}, AllowedControllerIDs: []string{signer.svid.ID.String()}, Tenant: record.Tenant, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.state.SaveNodeID(record.NodeID); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	record.MeshPlan, err = n.verifyStoredPlan(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	p, err := n.verifyStoredPlan(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if p != record.MeshPlan {
		t.Fatal("verified equal execution input replaced its pointer")
	}
	record.MeshPlan.Hash = "tampered parsed state"
	p, err = n.verifyStoredPlan(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if p == record.MeshPlan || p.Hash != "historical-topology" {
		t.Fatal("parsed tampering bypassed raw reconstruction")
	}
	record.MeshPlan = p
	record.MeshPlan.RestoreNotAfter = p.NotAfter
	p, err = n.verifyStoredPlan(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if p == record.MeshPlan || !p.RestoreNotAfter.IsZero() {
		t.Fatal("parsed restoration deadline bypassed reconstruction")
	}
	record.MeshPlan = p
	record.MeshPlanEnvelope.Body[0] ^= 1
	if _, err := n.verifyStoredPlan(ctx, record); !errors.Is(err, verify.ErrVerificationFailed) {
		t.Fatalf("pointer reuse bypassed raw signature verification: %v", err)
	}
	if record.MeshPlan.TypeURL != model.PlanTypeURL {
		t.Fatal("reapplication changed contract")
	}
}
