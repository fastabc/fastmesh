package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/test/fixture"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/control"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"github.com/fastabc/fastmesh/store/memory"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

func historicalIdentities(t *testing.T) (lifecycleIdentity, lifecycleIdentity, lifecycleIdentity) {
	t.Helper()
	now := time.Now()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(path string, serial int64, expiry time.Time) lifecycleIdentity {
		t.Helper()
		pub, leafKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		uri, err := url.Parse("spiffe://test/" + path)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: expiry, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{uri}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, key)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err = x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		id, err := spiffe.ParseID(uri.String())
		if err != nil {
			t.Fatal(err)
		}
		return lifecycleIdentity{spiffe.SVID{ID: id, CertChain: [][]byte{der}, Bundle: [][]byte{ca.Raw}, Key: leafKey, NotAfter: leaf.NotAfter}}
	}
	return issue("controller", 2, now.Add(-time.Minute)), issue("controller", 3, ca.NotAfter), issue("orders", 4, ca.NotAfter)
}

func historicalRecord(t *testing.T, expired lifecycleIdentity, nodeID string, workload lifecycleIdentity) (*snapshot.Snapshot, *discoveryv1.DiscoveryResponse) {
	t.Helper()
	_, p := runtimePlans(controllerIDToID(workload), controllerIDToID(workload))
	p.TypeURL, p.NodeID, p.Hash = model.PlanTypeURL, nodeID, "historical-topology"
	p.NotAfter = time.Now().Add(time.Hour) // Even a future signed expiry cannot revive the expired signer.
	body, err := json.Marshal(p.Wire())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := verify.PlanBodySigningPayload(model.PlanTypeURL, body)
	if err != nil {
		t.Fatal(err)
	}
	sig := snapshot.Signature{Algorithm: "spiffe-x509-ed25519", KeyID: expired.svid.ID.String(), CertChain: expired.svid.CertChain, Value: ed25519.Sign(expired.svid.Key.(ed25519.PrivateKey), payload)}
	envelope := &snapshot.SignedMeshPlan{TypeURL: model.PlanTypeURL, Body: body, Signature: sig}
	record := &snapshot.Snapshot{Tenant: p.Tenant, NodeID: nodeID, MeshPlan: &p, MeshPlanEnvelope: envelope}
	response := &discoveryv1.DiscoveryResponse{TypeUrl: model.PlanTypeURL, VersionInfo: p.Hash, Resources: []*discoveryv1.Resource{{Name: nodeID, Version: p.Hash, Resource: &anypb.Any{TypeUrl: model.PlanTypeURL, Value: body}}}, SnapshotSignature: &discoveryv1.SnapshotSignature{Algorithm: sig.Algorithm, KeyId: sig.KeyID, Value: sig.Value, CertChain: sig.CertChain}}
	return record, response
}

func TestHistoricalSignerRecoveryRemainsExpiredAndAcceptsFreshADS(t *testing.T) {
	for _, composed := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("composed=%v/committed=%v", composed, committed), func(t *testing.T) {
				expired, current, workload := historicalIdentities(t)
				ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				backing := memory.New()
				c, err := controller.New(controller.Config{ID: "historical", Objects: backing, Leases: backing, Identity: current, TrustDomain: "test", PlanValidity: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				lis, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				cctx, cancel := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- c.Serve(cctx, lis) }()
				t.Cleanup(func() {
					cancel()
					if err := <-done; err != nil {
						t.Error(err)
					}
				})
				for !c.Active() {
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(time.Millisecond):
					}
				}
				token, err := c.CreateJoinToken(ctx, enrollment.CreateRequest{Tenant: "default"})
				if err != nil {
					t.Fatal(err)
				}
				cfg := Config{Identity: workload, Controllers: []string{lis.Addr().String()}, AllowedControllerIDs: []string{current.svid.ID.String()}, TrustDomain: "test", Tenant: "default", StateDir: t.TempDir(), ConnectTimeout: time.Second}
				st := fixture.OpenState(t, cfg.StateDir)
				var options []Option
				if composed {
					options = []Option{WithProjection(apply.NewPipeline(apply.NewHolder(), st), st, &composedTestProjection{})}
				}
				if !composed {
					if err := st.Close(); err != nil {
						t.Fatal(err)
					}
				}
				n, err := newPlanTestNode(t, cfg, options...)
				if err != nil {
					t.Fatal(err)
				}
				if !composed {
					st = n.state
				}
				opts, err := n.dialOptions(ctx)
				if err != nil {
					t.Fatal(err)
				}
				conn, err := grpc.NewClient(lis.Addr().String(), opts...)
				if err != nil {
					t.Fatal(err)
				}
				registration, err := discoveryv1.NewBootstrapClient(conn).Register(ctx, &discoveryv1.RegisterRequest{JoinToken: token.Raw, NodeName: workload.svid.ID.String(), Capabilities: []string{model.PlanCapability, model.PlanCapability}})
				_ = conn.Close()
				if err != nil {
					t.Fatal(err)
				}
				if err := st.SaveNodeID(registration.NodeId); err != nil {
					t.Fatal(err)
				}
				record, response := historicalRecord(t, expired, registration.NodeId, workload)
				n.runtime.SetRecipient(model.Session{Tenant: cfg.Tenant, NodeID: registration.NodeId, Workload: controllerIDToID(workload)})
				// The network receiver still rejects an expired signing credential.
				a, err := control.NewPlanApplier(control.PlanApplierConfig{Recipient: model.Session{Tenant: cfg.Tenant, NodeID: registration.NodeId, Workload: controllerIDToID(workload)}, Pipeline: n.pipeline, HighWater: st.InputRevision, Verifier: func(ctx context.Context) (verify.SnapshotVerifier, error) { return n.verifier(ctx) }})
				if err != nil {
					t.Fatal(err)
				}
				if err := a.Full(ctx, response); !errors.Is(err, verify.ErrVerificationFailed) {
					t.Fatalf("network accepted historical signer: %v", err)
				}
				if committed {
					if err := st.SaveApplyPending(record, nil); err != nil {
						t.Fatal(err)
					}
					if err := st.CommitApply(); err != nil {
						t.Fatal(err)
					}
				} else if err := st.SaveLastGood(record, nil); err != nil {
					t.Fatal(err)
				}
				if _, err := n.pipeline.Restore(); err != nil {
					t.Fatal(err)
				}
				p := n.runtime.CurrentPlan()
				if dataplane.Fresh(p) || !p.NotAfter.Equal(expired.svid.NotAfter) {
					t.Fatalf("historical signer revived: %v", p.NotAfter)
				}
				if !n.pipeline.Snapshot().MeshPlan.NotAfter.Equal(record.MeshPlan.NotAfter) || !bytes.Equal(n.pipeline.Snapshot().MeshPlanEnvelope.Body, record.MeshPlanEnvelope.Body) {
					t.Fatal("recovery rewrote signed input")
				}
				if _, err := n.runtime.Listen(ctx, ServiceRef{Service: "payments", Port: "api"}, "127.0.0.1:0", false); !errors.Is(err, ErrDenied) {
					t.Fatalf("historical recovery admitted listener: %v", err)
				}
				if _, err := n.runtime.Dial(ctx, ServiceRef{Service: "payments", Port: "api"}); !errors.Is(err, ErrDenied) {
					t.Fatalf("historical recovery admitted connection: %v", err)
				}
				if _, err := DialFromPlan(ctx, n, ServiceRef{Service: "payments", Port: "api"}, n.pipeline.Snapshot().MeshPlan); !errors.Is(err, ErrDenied) {
					t.Fatalf("captured historical input admitted connection: %v", err)
				}
				if a.Loaded() {
					t.Fatal("historical signer reported a usable loaded plan")
				}
				if !composed {
					if err := n.state.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if err := n.Start(ctx); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = n.Close() })
				if !dataplane.Fresh(n.runtime.CurrentPlan()) || n.runtime.CurrentPlan().Hash == record.MeshPlan.Hash || n.nodeID != registration.NodeId {
					t.Fatal("fresh ADS failed to replace historical recovery")
				}
				// Start owns a newly opened state/pipeline; inspect that pipeline.
				a, err = control.NewPlanApplier(control.PlanApplierConfig{Recipient: n.runtime.Recipient(), Pipeline: n.pipeline, HighWater: n.state.InputRevision, Verifier: n.verifier})
				if err != nil || !a.Loaded() {
					t.Fatalf("fresh ADS did not report a usable loaded plan: %v", err)
				}
			})
		}
	}
}
