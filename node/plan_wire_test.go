package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/test/fixture"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/control"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestWirePlanRawAuthenticationEvolutionAndRecovery(t *testing.T) {
	ctx := context.Background()
	controllerID, identity, roots := lifecycleIdentities(t)
	workload := controllerIDToID(identity)
	_, p := runtimePlans(controllerIDToID(controllerID), workload)
	p.TypeURL, p.Hash = model.PlanTypeURL, "opaque-controller-topology"
	good := p.Inbound[0]
	unknown := good
	unknown.Service, unknown.Port.Protocol = "future", "future-protocol"
	p.Inbound = append(p.Inbound, unknown)
	p.Outbound = []model.OutboundService{{Service: "future", Port: unknown.Port}}
	encoded, err := json.Marshal(p.Wire())
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	body["future_field"] = map[string]any{"value": 17}
	body["header"].(map[string]any)["future_header"] = true
	topology := body["topology"].(map[string]any)
	topology["future_field"] = 23
	entries := topology["inbound"].([]any)
	// A malformed sibling and an unknown L7 execution requirement fail closed.
	entries = append(entries, map[string]any{"service": "malformed", "port": map[string]any{"name": "api", "number": 443, "protocol": "http"}, "allow": []any{map[string]any{"source": 17}}})
	entries = append(entries, map[string]any{"service": "future-l7", "port": map[string]any{"name": "api", "number": 443, "protocol": "http"}, "allow": []any{map[string]any{"source": controllerID.svid.ID.String(), "l7": map[string]any{"future_condition": "required"}}}})
	entries = append(entries, map[string]any{"service": "future-allow", "port": map[string]any{"name": "api", "number": 443, "protocol": "tcp"}, "allow": []any{map[string]any{"source": controllerID.svid.ID.String(), "future_condition": "required"}}})
	topology["inbound"] = entries
	topology["outbound"] = append(topology["outbound"].([]any),
		map[string]any{"service": "future-owner", "port": map[string]any{"name": "api", "number": 443, "protocol": "tcp"}, "owners": []string{controllerID.svid.ID.String()}, "endpoints": []any{}, "future_metadata": "extra"},
		map[string]any{"service": "bad-owner", "port": map[string]any{"name": "api", "number": 443, "protocol": "tcp"}, "owners": []any{17}},
		map[string]any{"service": "bad-requirement", "port": map[string]any{"name": "api", "number": 443, "protocol": "tcp"}, "requires_l7": "unknown"})
	raw, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	response := func(raw []byte) *discoveryv1.DiscoveryResponse {
		t.Helper()
		payload, err := verify.PlanBodySigningPayload(model.PlanTypeURL, raw)
		if err != nil {
			t.Fatal(err)
		}
		sig, err := snapshot.SignPayload(ctx, controllerID, payload, nil)
		if err != nil {
			t.Fatal(err)
		}
		return &discoveryv1.DiscoveryResponse{TypeUrl: model.PlanTypeURL, VersionInfo: p.Hash, Resources: []*discoveryv1.Resource{{Name: p.NodeID, Version: p.Hash, Resource: &anypb.Any{TypeUrl: model.PlanTypeURL, Value: raw}}}, SnapshotSignature: &discoveryv1.SnapshotSignature{Algorithm: sig.Algorithm, KeyId: sig.KeyID, Value: sig.Value, CertChain: sig.CertChain}}
	}
	cfg := Config{Identity: identity, Tenant: p.Tenant, Controllers: []string{"unused"}, AllowedControllerIDs: []string{controllerID.svid.ID.String()}, StateDir: t.TempDir()}
	n, err := newPlanTestNode(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.state.SaveNodeID(p.NodeID); err != nil {
		t.Fatal(err)
	}
	a, err := control.NewPlanApplier(control.PlanApplierConfig{Recipient: model.Session{Tenant: p.Tenant, NodeID: p.NodeID, Workload: workload}, Pipeline: n.pipeline, HighWater: n.state.InputRevision, Verifier: func(context.Context) (verify.SnapshotVerifier, error) {
		return verify.SnapshotVerifier{Roots: roots, TrustDomain: "test", AllowedControllerIDs: cfg.AllowedControllerIDs}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	r := response(raw)
	if err := a.Full(ctx, r); err != nil {
		t.Fatal(err)
	}
	serving := n.runtime.CurrentPlan()
	if !serving.IsCurrent() || serving.Hash != p.Hash || len(serving.Outbound) != 1 || serving.Outbound[0].Service != "future-owner" || len(serving.Outbound[0].Owners) != 1 || serving.Outbound[0].Owners[0] != controllerIDToID(controllerID) || !dataplane.AllowedInbound(serving, ServiceRef{Service: good.Service, Port: good.Port.Name}, controllerIDToID(controllerID), false, nil) {
		t.Fatalf("valid sibling/opaque hash lost: %+v", serving)
	}
	if dataplane.AllowedInbound(serving, ServiceRef{Service: "future-l7", Port: "api"}, controllerIDToID(controllerID), true, nil) {
		t.Fatal("unknown execution requirement authorized")
	}
	if dataplane.AllowedInbound(serving, ServiceRef{Service: "future", Port: good.Port.Name}, controllerIDToID(controllerID), false, nil) {
		t.Fatal("unknown protocol authorized")
	}
	if dataplane.AllowedInbound(serving, ServiceRef{Service: "future-allow", Port: "api"}, controllerIDToID(controllerID), false, nil) {
		t.Fatal("unknown allow attribute authorized")
	}
	stored := n.pipeline.Snapshot()
	if !bytes.Equal(stored.MeshPlanEnvelope.Body, raw) {
		t.Fatal("original unknown fields/whitespace not journaled")
	}
	r.SnapshotSignature.Value[0] ^= 1
	r.SnapshotSignature.CertChain[0][0] ^= 1
	if bytes.Equal(stored.MeshPlanEnvelope.Signature.Value, r.SnapshotSignature.Value) || bytes.Equal(stored.MeshPlanEnvelope.Signature.CertChain[0], r.SnapshotSignature.CertChain[0]) {
		t.Fatal("response mutation changed authenticated journal-holder signature")
	}
	r.Resources[0].Resource.Value[0] ^= 1
	if bytes.Equal(stored.MeshPlanEnvelope.Body, r.Resources[0].Resource.Value) {
		t.Fatal("response mutation changed authenticated journal-holder body")
	}
	lastGood, _, err := n.state.LoadLastGood()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(lastGood.MeshPlanEnvelope.Body, stored.MeshPlanEnvelope.Body) || !bytes.Equal(lastGood.MeshPlanEnvelope.Signature.Value, stored.MeshPlanEnvelope.Signature.Value) || !bytes.Equal(lastGood.MeshPlanEnvelope.Signature.CertChain[0], stored.MeshPlanEnvelope.Signature.CertChain[0]) {
		t.Fatal("response mutation changed persisted authenticated input")
	}
	r.Resources[0].Resource.Value[0] ^= 1
	bad := response([]byte("{"))
	bad.SnapshotSignature.Value[0] ^= 1
	if err := a.Full(ctx, bad); !errors.Is(err, verify.ErrVerificationFailed) {
		t.Fatalf("untrusted malformed JSON parsed before verification: %v", err)
	}
	if n.pipeline.Snapshot() != stored {
		t.Fatal("untrusted input changed live state")
	}
	header := body["header"].(map[string]any)
	header["tenant"] = "invalid tenant"
	invalidHeader, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Full(ctx, response(invalidHeader)); err == nil {
		t.Fatal("authenticated invalid header accepted")
	}
	if n.pipeline.Snapshot() != stored {
		t.Fatal("invalid authenticated header changed live state")
	}
	for _, mutation := range []func(*snapshot.Snapshot){
		func(s *snapshot.Snapshot) { s.MeshPlan.Hash = "tampered-local-copy" },
		func(s *snapshot.Snapshot) { s.MeshPlan = nil },
	} {
		record, _, err := n.state.LoadLastGood()
		if err != nil {
			t.Fatal(err)
		}
		mutation(record)
		if err := n.state.SaveLastGood(record, nil); err != nil {
			t.Fatal(err)
		}
		if err := n.state.Close(); err != nil {
			t.Fatal(err)
		}
		restarted, err := newPlanTestNode(t, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restarted.pipeline.Restore(); err != nil {
			t.Fatal(err)
		}
		if restarted.runtime.CurrentPlan().Hash != p.Hash {
			t.Fatal("restore trusted unauthenticated execution copy")
		}
		if err := restarted.state.Close(); err != nil {
			t.Fatal(err)
		}
		n.state = fixture.OpenState(t, cfg.StateDir)
	}
	ownerT := t
	for _, target := range []string{"last-good", "committed-journal"} {
		t.Run(target, func(t *testing.T) {
			record := *stored
			envelope := *stored.MeshPlanEnvelope
			envelope.Body = append([]byte(nil), raw...)
			envelope.Body[len(envelope.Body)-1] ^= 1
			record.MeshPlanEnvelope = &envelope
			if target == "last-good" {
				if err := n.state.SaveLastGood(&record, nil); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := n.state.SaveApplyPending(&record, nil); err != nil {
					t.Fatal(err)
				}
				if err := n.state.CommitApply(); err != nil {
					t.Fatal(err)
				}
			}
			if err := n.state.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := newPlanTestNode(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.pipeline.Restore(); !errors.Is(err, verify.ErrVerificationFailed) {
				t.Fatalf("tampered raw recovered: %v", err)
			}
			if restarted.runtime.CurrentPlan() != nil || restarted.pipeline.Snapshot() != nil {
				t.Fatal("failed verification published state")
			}
			if err := restarted.state.Close(); err != nil {
				t.Fatal(err)
			}
			n.state = fixture.OpenState(ownerT, cfg.StateDir)
		})
	}
}
