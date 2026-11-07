package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/test/fixture"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/control"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestSignedUnknownEntriesFailClosedAfterVerification(t *testing.T) {
	controllerID, identity, roots := lifecycleIdentities(t)
	workload := spiffeid.RequireFromString(identity.svid.ID.String())
	_, p := runtimePlans(spiffeid.RequireFromString(controllerID.svid.ID.String()), workload)
	good := p.Inbound[0]
	unknown := good
	unknown.Service = "future"
	unknown.Port.Protocol = "future-protocol"
	malformed := good
	malformed.Service = "invalid"
	malformed.Allow = []model.InboundAllow{{Source: controllerIDToID(controllerID), L7: &model.HTTPMatch{Path: "no-leading-slash"}}}
	p.Inbound = append(p.Inbound, unknown, malformed)
	p.Outbound = []model.OutboundService{{Service: "future", Port: unknown.Port}}
	p.Hash = p.ContentHash()
	body, err := json.Marshal(p.Wire())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := verify.PlanBodySigningPayload(model.PlanTypeURL, body)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := snapshot.SignPayload(context.Background(), controllerID, payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := &discoveryv1.DiscoveryResponse{TypeUrl: model.PlanTypeURL, VersionInfo: p.Hash, Resources: []*discoveryv1.Resource{{Name: p.NodeID, Version: p.Hash, Resource: &anypb.Any{TypeUrl: model.PlanTypeURL, Value: body}}}, SnapshotSignature: &discoveryv1.SnapshotSignature{Algorithm: sig.Algorithm, KeyId: sig.KeyID, Value: sig.Value, CertChain: sig.CertChain}}
	st := fixture.OpenState(t, t.TempDir())
	runtime := dataplane.New(identity, time.Second)
	pipeline := apply.NewPipeline(apply.NewHolder(), st)
	pipeline.AddCommitHook(runtime)
	config := control.PlanApplierConfig{Recipient: model.Session{Tenant: p.Tenant, NodeID: p.NodeID, Workload: p.Workload}, Pipeline: pipeline, HighWater: st.InputRevision, Verifier: func(context.Context) (verify.SnapshotVerifier, error) {
		return verify.SnapshotVerifier{Roots: roots, TrustDomain: "test", AllowedControllerIDs: []string{controllerID.svid.ID.String()}}, nil
	}}
	applier, err := control.NewPlanApplier(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Full(context.Background(), response); err != nil {
		t.Fatal(err)
	}
	if !applier.Loaded() {
		t.Fatal("accepted plan not loaded")
	}
	serving := runtime.CurrentPlan()
	if len(serving.Inbound) != 2 || serving.Inbound[0].Service != "payments" || len(serving.Inbound[1].Allow) != 0 || len(serving.Outbound) != 0 {
		t.Fatalf("unsafe serving plan: %+v", serving)
	}
	if !dataplane.AllowedInbound(serving, ServiceRef{Service: "payments", Port: "api"}, controllerIDToID(controllerID), false, nil) {
		t.Fatal("valid sibling denied")
	}
	if len(pipeline.Snapshot().MeshPlan.Inbound) != 3 || pipeline.Snapshot().MeshPlan.Hash != p.Hash {
		t.Fatal("signed journal input was filtered")
	}
	restoredRuntime := dataplane.New(identity, time.Second)
	restored := apply.NewPipeline(apply.NewHolder(), st)
	restored.AddCommitHook(restoredRuntime)
	if _, err := restored.Restore(); err != nil {
		t.Fatal(err)
	}
	if len(restoredRuntime.CurrentPlan().Inbound) != 2 || len(restoredRuntime.CurrentPlan().Inbound[1].Allow) != 0 {
		t.Fatal("restart restored unsafe entries")
	}
	response.SnapshotSignature.Value[0] ^= 1
	before := pipeline.Snapshot()
	if err := applier.Full(context.Background(), response); err == nil {
		t.Fatal("forged signature accepted")
	}
	if pipeline.Snapshot() != before {
		t.Fatal("forged plan changed journal")
	}
}
func controllerIDToID(identity lifecycleIdentity) spiffeid.ID {
	return spiffeid.RequireFromString(identity.svid.ID.String())
}
