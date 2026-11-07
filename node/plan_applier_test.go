package node

import (
	"context"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/test/fixture"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/protocol/planwire"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/control"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestPlanApplierPersistsWatermarkAndRejectsInvalidEnvelopes(t *testing.T) {
	for _, typeURL := range []string{model.PlanTypeURL} {
		t.Run(typeURL, func(t *testing.T) { testPlanApplierPersistsWatermark(t, typeURL) })
	}
}

func testPlanApplierPersistsWatermark(t *testing.T, typeURL string) {
	ctx := context.Background()
	controllerID, workloadID, roots := lifecycleIdentities(t)
	recipient := model.Session{Tenant: "default", NodeID: "node-payments", Workload: spiffeid.RequireFromString(workloadID.svid.ID.String())}
	st := fixture.OpenState(t, t.TempDir())
	pipeline := apply.NewPipeline(apply.NewHolder(), st)
	verifier := func(context.Context) (verify.SnapshotVerifier, error) {
		return verify.SnapshotVerifier{Roots: roots, TrustDomain: "test", AllowedControllerIDs: []string{controllerID.svid.ID.String()}}, nil
	}
	newApplier := func(pipeline *apply.Pipeline) *control.PlanApplier {
		t.Helper()
		a, err := control.NewPlanApplier(control.PlanApplierConfig{Recipient: recipient, Pipeline: pipeline, Verifier: verifier, HighWater: st.InputRevision})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	a := newApplier(pipeline)
	if a.Loaded() {
		t.Fatal("unloaded plan reported ready")
	}
	p := model.Plan{TypeURL: model.PlanTypeURL, Tenant: recipient.Tenant, NodeID: recipient.NodeID, Workload: recipient.Workload, InputRevision: 100, NotAfter: time.Now().Add(time.Minute), AccessSet: true, EndpointsSet: true}
	if typeURL == model.PlanTypeURL {
		p.TypeURL = typeURL
	}
	response := func(p model.Plan) *discoveryv1.DiscoveryResponse {
		t.Helper()
		p.Hash = p.ContentHash()
		resource, err := planwire.PackPlan(p)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := verify.PlanBodySigningPayload(typeURL, resource.Resource.Value)
		if err != nil {
			t.Fatal(err)
		}
		sig, err := snapshot.SignPayload(ctx, controllerID, payload, nil)
		if err != nil {
			t.Fatal(err)
		}
		return &discoveryv1.DiscoveryResponse{TypeUrl: typeURL, VersionInfo: p.Hash, Resources: []*discoveryv1.Resource{resource}, SnapshotSignature: &discoveryv1.SnapshotSignature{Algorithm: sig.Algorithm, KeyId: sig.KeyID, Value: sig.Value, CertChain: sig.CertChain}}
	}
	first := response(p)
	if err := a.Full(ctx, first); err != nil {
		t.Fatal(err)
	}
	if !a.Loaded() || a.Current() != p.ContentHash() {
		t.Fatal("loaded-empty plan lost")
	}
	if got, err := st.InputRevision(); err != nil || got != 100 {
		t.Fatalf("watermark=%d err=%v", got, err)
	}
	before := pipeline.Snapshot()
	if err := a.Full(ctx, first); err != nil {
		t.Fatal(err)
	}
	if pipeline.Snapshot() != before {
		t.Fatal("replay reapplied or extended deadline")
	}
	for name, mutate := range map[string]func(*model.Plan){
		"old revision":   func(p *model.Plan) { p.InputRevision = 99 },
		"wrong workload": func(p *model.Plan) { p.Workload = spiffeid.RequireFromString("spiffe://test/other") },
		"wrong node":     func(p *model.Plan) { p.NodeID = "other" },
		"wrong tenant":   func(p *model.Plan) { p.Tenant = "other" },
		"expired":        func(p *model.Plan) { p.NotAfter = time.Now().Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := p
			mutate(&bad)
			if err := a.Full(ctx, response(bad)); err == nil {
				t.Fatal("invalid plan accepted")
			}
			if pipeline.Snapshot() != before {
				t.Fatal("invalid plan reached pipeline")
			}
		})
	}
	if err := a.Delta(ctx, &discoveryv1.DeltaDiscoveryResponse{TypeUrl: typeURL, SystemVersionInfo: first.VersionInfo}); err == nil {
		t.Fatal("empty heartbeat accepted")
	}
	bad := response(p)
	bad.TypeUrl = "example.snapshot.v1"
	if err := a.Full(ctx, bad); err == nil {
		t.Fatal("wrong type accepted")
	}
	bad = response(p)
	bad.SnapshotSignature.Value[0] ^= 1
	if err := a.Full(ctx, bad); err == nil {
		t.Fatal("forged signature accepted")
	}
	p.InputRevision++
	p.NotAfter = p.NotAfter.Add(time.Minute)
	renewal := response(p)
	if err := a.Delta(ctx, &discoveryv1.DeltaDiscoveryResponse{TypeUrl: renewal.TypeUrl, SystemVersionInfo: renewal.VersionInfo, Resources: renewal.Resources, SnapshotSignature: renewal.SnapshotSignature}); err != nil {
		t.Fatal(err)
	}
	if !pipeline.Snapshot().MeshPlan.NotAfter.Equal(p.NotAfter) {
		t.Fatal("signed renewal lost")
	}
	restarted := apply.NewPipeline(apply.NewHolder(), st)
	if _, err := restarted.Restore(); err != nil {
		t.Fatal(err)
	}
	a = newApplier(restarted)
	if err := a.Full(ctx, first); err == nil {
		t.Fatal("restart forgot durable watermark")
	}
	if !a.Loaded() || restarted.Snapshot().MeshPlan.InputRevision != 101 {
		t.Fatal("restart lost plan")
	}
	if !restarted.Snapshot().MeshPlan.NotAfter.Equal(p.NotAfter) {
		t.Fatal("restart changed signed expiry")
	}
}
