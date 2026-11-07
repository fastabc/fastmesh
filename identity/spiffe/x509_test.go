package spiffe

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"testing"
	"time"
)

func TestX509SVIDFromCertificates(t *testing.T) {
	t.Parallel()

	rootCert, rootKey := mustRoot(t)
	leafCert, leafKey := mustLeaf(t, rootCert, rootKey, "spiffe://example.org/example/controller/a")
	roots := x509.NewCertPool()
	roots.AddCert(rootCert)

	svid, err := X509SVIDFromCertificates([]*x509.Certificate{leafCert}, leafKey, VerifyOptions{
		TrustDomain: "example.org",
		Roots:       roots,
		CurrentTime: time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("X509SVIDFromCertificates returned error: %v", err)
	}
	if got, want := svid.ID.String(), "spiffe://example.org/example/controller/a"; got != want {
		t.Fatalf("SVID ID=%q want %q", got, want)
	}
	if len(svid.CertChain) != 1 {
		t.Fatalf("CertChain len=%d want 1", len(svid.CertChain))
	}
}

func TestX509SVIDFromPEM(t *testing.T) {
	t.Parallel()

	rootCert, rootKey := mustRoot(t)
	leafCert, leafKey := mustLeaf(t, rootCert, rootKey, "spiffe://example.org/example/agent/node-a")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafCert.Raw})

	svid, err := X509SVIDFromPEM(pemBytes, leafKey, VerifyOptions{})
	if err != nil {
		t.Fatalf("X509SVIDFromPEM returned error: %v", err)
	}
	if got, want := svid.ID.String(), "spiffe://example.org/example/agent/node-a"; got != want {
		t.Fatalf("SVID ID=%q want %q", got, want)
	}
}

func TestX509SVIDRejectsTrustDomainMismatch(t *testing.T) {
	t.Parallel()

	rootCert, rootKey := mustRoot(t)
	leafCert, leafKey := mustLeaf(t, rootCert, rootKey, "spiffe://example.org/example/controller/a")
	_, err := X509SVIDFromCertificates([]*x509.Certificate{leafCert}, leafKey, VerifyOptions{TrustDomain: "other.org"})
	if !errors.Is(err, ErrTrustDomainMismatch) {
		t.Fatalf("err=%v want ErrTrustDomainMismatch", err)
	}
}

func TestIDFromURIsRejectsMissingOrMultipleSPIFFE(t *testing.T) {
	t.Parallel()

	if _, err := IDFromURIs(nil); !errors.Is(err, ErrInvalidSVID) {
		t.Fatalf("missing URI err=%v want ErrInvalidSVID", err)
	}
	one, _ := url.Parse("spiffe://example.org/example/controller/a")
	two, _ := url.Parse("spiffe://example.org/example/controller/b")
	if _, err := IDFromURIs([]*url.URL{one, two}); !errors.Is(err, ErrInvalidSVID) {
		t.Fatalf("multiple URI err=%v want ErrInvalidSVID", err)
	}
}

func mustRoot(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey root: %v", err)
	}
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-root"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate root: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate root: %v", err)
	}
	return cert, key
}

func mustLeaf(t *testing.T, root *x509.Certificate, rootKey *rsa.PrivateKey, spiffeID string) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey leaf: %v", err)
	}
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("Parse URI: %v", err)
	}
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "ignored"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, root, &key.PublicKey, rootKey)
	if err != nil {
		t.Fatalf("CreateCertificate leaf: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate leaf: %v", err)
	}
	return cert, key
}
