package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/protocol/bootstrap"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/etcd"
	"github.com/fastabc/fastmesh/store/memory"
	"github.com/fastabc/fastmesh/test/fixture"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestManagedEnrollment(t *testing.T) {
	t.Run("memory", func(t *testing.T) { testManagedEnrollment(t, memory.New()) })
	t.Run("etcd", func(t *testing.T) {
		endpoints := fixture.EtcdEndpoints(t)
		if endpoints == "" {
			t.Skip("set FASTMESH_ETCD_ENDPOINTS for managed enrollment on etcd")
		}
		client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		testManagedEnrollment(t, etcd.New(client, fmt.Sprintf("/managed-enrollment/%d", time.Now().UnixNano())))
	})
}

func testManagedEnrollment(t *testing.T, backing interface {
	store.ObjectStore
	store.LeaseStore
}) {
	t.Helper()
	controlID := spiffeid.RequireFromString("spiffe://test/controller")
	workloadID := spiffeid.RequireFromString("spiffe://test/payments")
	issuer := enrollmentTestIssuer(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	c, err := New(Config{ID: "controller", Objects: backing, Leases: backing, TrustDomain: "test",
		Identity: enrollmentTestIdentity(t, issuer, controlID)},
		WithEnrollment(EnrollmentConfig{Issuer: issuer}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- c.Serve(ctx, listener) }()
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

	handler := c.EnrollmentHandler()
	var lost atomic.Bool
	commits := make(chan bootstrap.Response, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if lost.CompareAndSwap(false, true) {
			result := httptest.NewRecorder()
			handler.ServeHTTP(result, r)
			if result.Code != http.StatusOK {
				t.Errorf("first server response: %d", result.Code)
			}
			var committed bootstrap.Response
			if err := json.Unmarshal(result.Body.Bytes(), &committed); err != nil {
				t.Error(err)
			}
			commits <- committed
			http.Error(w, "response lost", http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	server.TLS = &tls.Config{ClientAuth: tls.RequestClientCert}
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	opts := node.EnrollmentOptions{URL: server.URL, StateDir: t.TempDir(), ServerRoots: roots,
		Labels: map[string]string{"platform": "vm"}, Capabilities: []string{"application/v1"}}

	request := enrollment.ManagedTokenRequest{Tenant: "acme", NodeName: "edge", WorkloadID: workloadID.String()}
	if _, err := c.CreateEnrollmentToken(ctx, enrollment.ManagedTokenRequest{NodeName: "edge", WorkloadID: controlID.String()}); err == nil {
		t.Fatal("issued token for Controller identity")
	}
	token, err := c.CreateEnrollmentToken(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	opts.Token = token.Raw
	untrusted := opts
	untrusted.StateDir, untrusted.ServerRoots = t.TempDir(), x509.NewCertPool()
	if _, err := node.Join(ctx, untrusted); err == nil || lost.Load() {
		t.Fatal("sent the token without trusting the HTTPS server")
	}
	if _, err := node.Join(ctx, opts); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("response loss: %v", err)
	}
	committed := <-commits
	cfg, err := node.Join(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := cfg.Identity.GetSVID(ctx)
	if err != nil || initial.ID.String() != workloadID.String() || !bytes.Equal(initial.CertChain[0], committed.Chain[0]) {
		t.Fatalf("retry returned a different identity: %v", err)
	}
	if info, err := os.Stat(filepath.Join(opts.StateDir, "enrollment-identity", "credentials.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("credentials file permissions: %v, %v", info, err)
	}
	other := opts
	other.StateDir = t.TempDir()
	if _, err := node.Join(ctx, other); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("different key reused token: %v", err)
	}
	objects, err := backing.ListObjects(ctx, model.SystemTenant, model.NodeKind)
	if err != nil || len(objects) != 1 || objects[0].Key.Name != committed.NodeID {
		t.Fatalf("registration was duplicated or changed: %v, %v", len(objects), err)
	}

	var registered model.Node
	if err := json.Unmarshal(objects[0].Data, &registered); err != nil {
		t.Fatal(err)
	}
	if registered.Name != request.NodeName || registered.Tenant != request.Tenant ||
		registered.Status.ReportedLabels["platform"] != "vm" ||
		!slices.Contains(registered.Status.ReportedCapabilities, "application/v1") ||
		!slices.Contains(registered.Status.ReportedCapabilities, model.PlanCapability) {
		t.Fatal("registration lost the assigned identity or reported metadata")
	}

	// A valid certificate with the same workload ID still needs this node's key.
	otherID := enrollmentTestIdentity(t, issuer, workloadID)
	tlsConfig, err := spiffe.ClientTLSConfig(ctx, otherID, []string{controlID.String()}, "test")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	legacy := discoveryv1.NewBootstrapClient(conn)
	rewritten := "fgjt" + strings.TrimPrefix(token.Raw, "fmjoin")
	if _, err := legacy.Register(ctx, &discoveryv1.RegisterRequest{JoinToken: rewritten}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("managed token accepted by legacy enrollment: %v", err)
	}
	otherPeer := discoveryv1.NewDiscoveryClient(conn)
	if _, err := otherPeer.FetchSnapshot(ctx, &discoveryv1.FetchSnapshotRequest{Node: &discoveryv1.Node{Id: committed.NodeID}, TypeUrl: model.PlanTypeURL}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("alternate key was admitted: %v", err)
	}

	running, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = running.Close() })
	if err := running.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := running.Status(); got.NodeID != committed.NodeID || got.Workload != workloadID || got.InputRevision == 0 {
		t.Fatalf("node has no verified plan: %+v", got)
	}

	if err := c.RevokeEnrollmentToken(ctx, token.Token.ID); err != nil {
		t.Fatal(err)
	}
	opts.Token = ""
	if err := node.RenewEnrollment(ctx, opts); err != nil {
		t.Fatal(err)
	}
	renewed, err := cfg.Identity.GetSVID(ctx)
	if err != nil || renewed.ID != initial.ID || bytes.Equal(renewed.CertChain[0], initial.CertChain[0]) {
		t.Fatalf("running source did not observe renewal: %v", err)
	}
	if err := running.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := node.Join(ctx, node.EnrollmentOptions{StateDir: opts.StateDir})
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := reopened.Identity.GetSVID(ctx)
	if err != nil || !bytes.Equal(resumed.CertChain[0], renewed.CertChain[0]) {
		t.Fatalf("restart lost the renewed certificate: %v", err)
	}
	if err := c.DisableNode(ctx, committed.NodeID); err != nil {
		t.Fatal(err)
	}
	if err := node.RenewEnrollment(ctx, opts); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("disabled node renewed its identity: %v", err)
	}
}

func enrollmentTestIssuer(t *testing.T) spiffe.X509Issuer {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return spiffe.X509Issuer{Chain: []*x509.Certificate{ca}, Key: key, TTL: 30 * time.Minute}
}

func enrollmentTestIdentity(t *testing.T, issuer spiffe.X509Issuer, id spiffeid.ID) lifecycleIdentity {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := issuer.IssueSVID(context.Background(), id, pub)
	if err != nil {
		t.Fatal(err)
	}
	bundle := [][]byte{issuer.Chain[0].Raw}
	leaf, _, err := bootstrap.VerifyChain(chain, bundle, pub, "test")
	if err != nil {
		t.Fatal(err)
	}
	workload, err := spiffe.ParseID(id.String())
	if err != nil {
		t.Fatal(err)
	}
	return lifecycleIdentity{spiffe.SVID{ID: workload, CertChain: chain, Key: key, Bundle: bundle, NotAfter: leaf.NotAfter}}
}
