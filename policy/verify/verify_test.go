package verify

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

func signed(snap *snapshot.Snapshot) *snapshot.Snapshot {
	snap.Revision = snapshot.CanonicalHash(snap)
	return snap
}

func sigFor(snap *snapshot.Snapshot) []byte {
	b, _ := hex.DecodeString(strings.TrimPrefix(string(snapshot.CanonicalHash(snap)), "sha256:"))
	return b
}

func TestContentAcceptsMatchingHash(t *testing.T) {
	snap := signed(&snapshot.Snapshot{
		Tenant: "default",
		NodeID: "node-1",
		Resources: []snapshot.Resource{
			{Ref: snapshot.ResourceRef{Tenant: "default", Type: snapshot.ResourceType("route"), Name: "home"}, Data: []byte(`{"path":"/"}`)},
		},
	})
	if err := Content(snap); err != nil {
		t.Fatalf("Verify() = %v, want nil", err)
	}
}

func TestContentRejectsTamperedSnapshot(t *testing.T) {
	snap := signed(&snapshot.Snapshot{
		Tenant:    "default",
		NodeID:    "node-1",
		Resources: []snapshot.Resource{{Ref: snapshot.ResourceRef{Tenant: "default", Type: snapshot.ResourceType("route"), Name: "home"}, Data: []byte(`{"path":"/"}`)}},
	})
	// Tamper after the revision was fixed.
	snap.Resources[0].Data = []byte(`{"path":"/hijacked"}`)
	if err := Content(snap); err == nil {
		t.Fatal("Verify() = nil, want error for tampered content")
	}
}

func TestContentRejectsNil(t *testing.T) {
	if err := Content(nil); err == nil {
		t.Fatal("Verify(nil) = nil, want error")
	}
}

func TestSnapshotVerifierAcceptsSVIDSignature(t *testing.T) {
	snap := signed(&snapshot.Snapshot{Tenant: "default", NodeID: "node-1", GeneratedAt: time.Now().UTC()})
	root, leaf, key := testSVIDChain(t, "spiffe://example.org/example/controller/a", time.Now().UTC())
	payload, err := snapshot.SigningPayload(snap)
	if err != nil {
		t.Fatalf("SigningPayload: %v", err)
	}
	sig := snapshot.Signature{
		Algorithm: "spiffe-x509-ed25519",
		KeyID:     "spiffe://example.org/example/controller/a",
		Value:     ed25519.Sign(key, payload),
		CertChain: [][]byte{leaf.Raw},
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	verifier := SnapshotVerifier{
		TrustDomain:          spiffe.TrustDomain("example.org"),
		Roots:                roots,
		AllowedControllerIDs: []string{"spiffe://example.org/example/controller/a"},
	}
	if err := verifier.Verify(context.Background(), snap, sig); err != nil {
		t.Fatalf("Verify signed snapshot: %v", err)
	}
}

func TestSnapshotVerifierRejectsWrongRecipient(t *testing.T) {
	for _, recipient := range []struct{ node, tenant string }{{"other", "default"}, {"node-1", "other"}} {
		snap := signed(&snapshot.Snapshot{Tenant: "default", NodeID: "node-1"})
		v := SnapshotVerifier{NodeID: recipient.node, Tenant: recipient.tenant}
		if err := v.Verify(context.Background(), snap, snapshot.Signature{Algorithm: "sha256-content", Value: sigFor(snap)}); err == nil {
			t.Fatal("accepted replay to wrong recipient")
		}
	}
}

func TestSnapshotVerifierRejectsTamperedSVIDSnapshot(t *testing.T) {
	snap := signed(&snapshot.Snapshot{
		Tenant:    "default",
		NodeID:    "node-1",
		Resources: []snapshot.Resource{{Ref: snapshot.ResourceRef{Tenant: "default", Type: snapshot.ResourceType("route"), Name: "home"}, Data: []byte("home")}},
	})
	root, leaf, key := testSVIDChain(t, "spiffe://example.org/example/controller/a", time.Now().UTC())
	payload, err := snapshot.SigningPayload(snap)
	if err != nil {
		t.Fatalf("SigningPayload: %v", err)
	}
	sig := snapshot.Signature{Algorithm: "spiffe-x509-ed25519", KeyID: "spiffe://example.org/example/controller/a", Value: ed25519.Sign(key, payload), CertChain: [][]byte{leaf.Raw}}
	snap.Resources[0].Data = []byte("tampered")

	roots := x509.NewCertPool()
	roots.AddCert(root)
	err = (SnapshotVerifier{TrustDomain: "example.org", Roots: roots}).Verify(context.Background(), snap, sig)
	if !strings.Contains(errString(err), ErrVerificationFailed.Error()) {
		t.Fatalf("Verify tampered err=%v want ErrVerificationFailed", err)
	}
}

func TestSnapshotVerifierRejectsUntrustedController(t *testing.T) {
	snap := signed(&snapshot.Snapshot{Tenant: "default", NodeID: "node-1"})
	root, leaf, key := testSVIDChain(t, "spiffe://example.org/example/controller/a", time.Now().UTC())
	payload, err := snapshot.SigningPayload(snap)
	if err != nil {
		t.Fatalf("SigningPayload: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	sig := snapshot.Signature{Algorithm: "spiffe-x509-ed25519", KeyID: "spiffe://example.org/example/controller/a", Value: ed25519.Sign(key, payload), CertChain: [][]byte{leaf.Raw}}
	err = (SnapshotVerifier{
		TrustDomain:          "example.org",
		Roots:                roots,
		AllowedControllerIDs: []string{"spiffe://example.org/example/controller/b"},
	}).Verify(context.Background(), snap, sig)
	if !strings.Contains(errString(err), "not allowed") {
		t.Fatalf("Verify untrusted err=%v want not allowed", err)
	}
}

func testSVIDChain(t testing.TB, spiffeID string, now time.Time) (*x509.Certificate, *x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	rootPub, rootKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey root: %v", err)
	}
	rootTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "root"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, rootPub, rootKey)
	if err != nil {
		t.Fatalf("CreateCertificate root: %v", err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatalf("ParseCertificate root: %v", err)
	}

	leafPub, leafKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey leaf: %v", err)
	}
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("Parse URI: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "controller"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		URIs:         []*url.URL{uri},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, root, leafPub, rootKey)
	if err != nil {
		t.Fatalf("CreateCertificate leaf: %v", err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatalf("ParseCertificate leaf: %v", err)
	}
	return root, leaf, leafKey
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestSnapshotVerifierRejectsDigestSignatures(t *testing.T) {
	snap := signed(&snapshot.Snapshot{Tenant: "default", NodeID: "node-1"})
	for _, algorithm := range []string{"", "sha256-content", "unknown"} {
		if err := (SnapshotVerifier{}).Verify(context.Background(), snap, snapshot.Signature{Algorithm: algorithm, Value: sigFor(snap)}); err == nil {
			t.Fatalf("accepted %q", algorithm)
		}
	}
}
