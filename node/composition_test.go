package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/test/fixture"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

type composedTestProjection struct {
	current atomic.Pointer[snapshot.Snapshot]
	fail    bool
}

func (p *composedTestProjection) Prepare(_ context.Context, s *snapshot.Snapshot) (apply.PreparedChange, error) {
	if p.fail {
		return nil, errors.New("prepare failed")
	}
	return apply.PreparedChangeFunc{CommitFunc: func(context.Context) error { p.current.Store(s); return nil }}, nil
}
func (p *composedTestProjection) Addrs() []string                { return nil }
func (p *composedTestProjection) Shutdown(context.Context) error { return nil }
func (p *composedTestProjection) MeshPlan() *model.Plan {
	if s := p.current.Load(); s != nil {
		return s.MeshPlan
	}
	return nil
}

func TestCompositionSerializesIndependentInputsAndRestoresTuple(t *testing.T) {
	ctx := context.Background()
	controller, identity, _ := lifecycleIdentities(t)
	st := fixture.OpenState(t, t.TempDir())
	if err := st.SaveNodeID("gateway"); err != nil {
		t.Fatal(err)
	}
	projection := &composedTestProjection{}
	pipeline := apply.NewPipeline(apply.NewHolder(), st)
	cfg := Config{Identity: identity, Controllers: []string{"127.0.0.1:1"}, AllowedControllerIDs: []string{controller.svid.ID.String()}, Tenant: "default", StateDir: t.TempDir()}
	n, err := New(cfg, WithProjection(pipeline, st, projection))
	if err != nil {
		t.Fatal(err)
	}
	gateway := HostApplier(n)
	mesh := composedInput{n.composition, true}
	plan := model.Plan{TypeURL: model.PlanTypeURL, Tenant: "default", NodeID: "gateway", Workload: spiffeid.RequireFromString(identity.svid.ID.String()), InputRevision: 1, NotAfter: time.Now().Add(time.Minute), AccessSet: true, EndpointsSet: true}
	plan.Hash = plan.ContentHash()
	signedInput := func(p model.Plan) *snapshot.Snapshot {
		t.Helper()
		body, err := json.Marshal(p.Wire())
		if err != nil {
			t.Fatal(err)
		}
		payload, err := verify.PlanBodySigningPayload(model.PlanTypeURL, body)
		if err != nil {
			t.Fatal(err)
		}
		sig, err := snapshot.SignPayload(ctx, controller, payload, nil)
		if err != nil {
			t.Fatal(err)
		}
		return &snapshot.Snapshot{Tenant: "default", NodeID: "gateway", MeshPlan: &p, MeshPlanEnvelope: &snapshot.SignedMeshPlan{TypeURL: model.PlanTypeURL, Body: body, Signature: sig}}
	}
	if err := mesh.Apply(ctx, signedInput(plan)); err != nil {
		t.Fatal(err)
	}
	if gateway.Snapshot() != nil {
		t.Fatal("mesh-only state suppresses gateway cold start")
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 1; i <= 20; i++ {
			if err := gateway.Apply(ctx, &snapshot.Snapshot{Tenant: "default", NodeID: "gateway", Revision: snapshot.Revision(fmt.Sprint(i))}); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 2; i <= 21; i++ {
			p := plan
			p.InputRevision = int64(i)
			p.NotAfter = p.NotAfter.Add(time.Duration(i) * time.Second)
			p.Hash = p.ContentHash()
			if err := mesh.Apply(ctx, signedInput(p)); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	current := pipeline.Snapshot()
	serving := projection.current.Load()
	if current.Revision != "20" || current.MeshPlan.InputRevision != 21 || serving.Revision != current.Revision || serving.MeshPlan.InputRevision != current.MeshPlan.InputRevision || serving.MeshPlan.Hash != current.MeshPlan.Hash {
		t.Fatal("lost update or split tuple")
	}
	if serving.MeshPlan.NotAfter == serving.MeshPlan.NotAfter.Round(0) || !serving.MeshPlan.NotAfter.Equal(current.MeshPlan.NotAfter) {
		t.Fatal("serving tuple lost monotonic or signed deadline")
	}
	if gateway.Snapshot().MeshPlan != nil {
		t.Fatal("local plan leaked into gateway wire input")
	}
	projection.fail = true
	if err := gateway.Apply(ctx, &snapshot.Snapshot{Revision: "failed"}); err == nil {
		t.Fatal("prepare failure accepted")
	}
	if pipeline.Snapshot() != current || projection.current.Load() != serving {
		t.Fatal("failed preparation changed tuple")
	}
	projection.fail = false
	if err := gateway.Apply(ctx, gateway.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if projection.MeshPlan() != serving.MeshPlan {
		t.Fatal("credential reapplication replaced the local plan")
	}
	if err := gateway.Apply(ctx, current); err == nil {
		t.Fatal("gateway injected local mesh plan")
	}
	restored := &composedTestProjection{}
	restoredPipeline := apply.NewPipeline(apply.NewHolder(), st)
	if _, err := New(cfg, WithProjection(restoredPipeline, st, restored)); err != nil {
		t.Fatal(err)
	}
	if _, err := restoredPipeline.Restore(); err != nil {
		t.Fatal(err)
	}
	if got := restored.current.Load(); got.Revision != "20" || got.MeshPlan.InputRevision != 21 {
		t.Fatal("recovery lost composed state")
	}
	if p := restored.MeshPlan(); p.NotAfter == p.NotAfter.Round(0) || !p.NotAfter.Round(0).Equal(plan.NotAfter.Add(21*time.Second).Round(0)) {
		t.Fatal("recovery failed to anchor the persisted signed deadline")
	}
}
