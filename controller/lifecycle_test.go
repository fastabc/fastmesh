package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/protocol/planwire"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type lifecycleIdentity struct{ svid spiffe.SVID }

func (s lifecycleIdentity) GetSVID(context.Context) (spiffe.SVID, error) { return s.svid, nil }

func lifecycleIdentities(t *testing.T) (lifecycleIdentity, lifecycleIdentity, *x509.CertPool) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(path string, serial int64) lifecycleIdentity {
		pub, leafKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		uri, err := url.Parse("spiffe://test/" + path)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{uri}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, key)
		if err != nil {
			t.Fatal(err)
		}
		id, err := spiffe.ParseID(uri.String())
		if err != nil {
			t.Fatal(err)
		}
		return lifecycleIdentity{spiffe.SVID{ID: id, CertChain: [][]byte{der}, Bundle: [][]byte{ca.Raw}, Key: leafKey, NotAfter: leaf.NotAfter}}
	}
	return issue("controller", 2), issue("payments", 3), roots
}

func TestStandaloneControllerLifecycle(t *testing.T) {
	controllerID, workloadID, roots := lifecycleIdentities(t)
	backing := memory.New()

	c, err := New(Config{ID: "controller-a", Objects: backing, Leases: backing, Identity: controllerID, TrustDomain: "test", EndpointTTL: 150 * time.Millisecond, ReconcileInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lis := bufconn.Listen(1 << 20)
	done := make(chan error, 1)
	go func() { done <- c.Serve(ctx, lis) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("controller did not stop")
		}
		if c.Active() {
			t.Error("controller retained leadership after shutdown")
		}
	})
	for !c.Active() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	token, err := c.CreateJoinToken(ctx, enrollment.CreateRequest{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig, err := spiffe.ClientTLSConfig(ctx, workloadID, []string{controllerID.svid.ID.String()}, "test")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///controller", grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	register, err := discoveryv1.NewBootstrapClient(conn).Register(ctx, &discoveryv1.RegisterRequest{JoinToken: token.Raw, NodeName: "payments", Capabilities: []string{model.PlanCapability, model.PlanCapability}})
	if err != nil {
		t.Fatal(err)
	}
	owner := spiffeid.RequireFromString(workloadID.svid.ID.String())
	if err := c.PutService(ctx, model.Service{Tenant: "acme", Name: "payments", Ports: []model.ServicePort{{Name: "api", Number: 8443, Protocol: "tcp"}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutBindGrant(ctx, "payments", model.BindGrant{Tenant: "acme", Service: "payments", Workload: model.Principal{ID: owner}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutServiceAccess(ctx, "self", model.ServiceAccess{Tenant: "acme", Service: "payments", Source: model.Principal{ID: owner}}); err != nil {
		t.Fatal(err)
	}
	catalogClient := catalogv1.NewCatalogClient(conn)
	endpoint := &catalogv1.RegisterEndpointRequest{NodeId: register.NodeId, Service: "payments", Port: "api", InstanceId: "payments-1", Generation: 1, Address: "127.0.0.1:8443", Ready: true}
	lease, err := catalogClient.RegisterEndpoint(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Owner != owner.String() {
		t.Fatalf("owner=%q", lease.Owner)
	}
	discovery := discoveryv1.NewDiscoveryClient(conn)
	response, err := discovery.FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: &discoveryv1.Node{Id: register.NodeId}, TypeUrl: model.PlanTypeURL})
	if err != nil {
		t.Fatal(err)
	}
	sig := response.SnapshotSignature
	if err := verify.PlanBody(ctx, model.PlanTypeURL, response.Snapshot, snapshot.Signature{Algorithm: sig.Algorithm, KeyID: sig.KeyId, Value: sig.Value, CertChain: sig.CertChain}, verify.SnapshotVerifier{Roots: roots, TrustDomain: "test", AllowedControllerIDs: []string{controllerID.svid.ID.String()}}); err != nil {
		t.Fatal(err)
	}
	plan, err := planwire.DecodePlanBody(response.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.IsCurrent() || len(plan.Outbound) != 1 || len(plan.Outbound[0].Owners) != 1 || plan.Outbound[0].Owners[0] != owner {
		t.Fatalf("incomplete plan: %+v", plan)
	}
	endpoint.NodeId = "forged"
	if _, err := catalogClient.RegisterEndpoint(ctx, endpoint); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("forged catalog peer: %v", err)
	}
	if _, err := discovery.FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: &discoveryv1.Node{Id: register.NodeId}, Tenant: "other", TypeUrl: model.PlanTypeURL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant fetch: %v", err)
	}
	for {
		obj, err := backing.GetObject(ctx, model.EndpointKey("acme", "payments", owner, endpoint.InstanceId))
		if err != nil {
			t.Fatal(err)
		}
		var record model.EndpointRecord
		if err := json.Unmarshal(obj.Data, &record); err != nil {
			t.Fatal(err)
		}
		if !record.RemovedAt.IsZero() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("lease was not reconciled")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
