package workloadapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	gospiffex509svid "github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

type fakeFetcher struct {
	material X509SVIDMaterial
	err      error
}

func (f fakeFetcher) FetchX509SVID(context.Context) (X509SVIDMaterial, error) {
	return f.material, f.err
}

func TestSourceGetSVID(t *testing.T) {
	t.Parallel()

	material := mustMaterial(t, "spiffe://example.org/example/controller/a")
	src := Source{
		Fetcher:     fakeFetcher{material: material},
		TrustDomain: "example.org",
	}
	svid, err := src.GetSVID(context.Background())
	if err != nil {
		t.Fatalf("GetSVID returned error: %v", err)
	}
	if got, want := svid.ID.String(), "spiffe://example.org/example/controller/a"; got != want {
		t.Fatalf("SVID ID=%q want %q", got, want)
	}
}

func TestSourceRequiresFetcher(t *testing.T) {
	t.Parallel()

	if _, err := (Source{}).GetSVID(context.Background()); !errors.Is(err, ErrMissingFetcher) {
		t.Fatalf("GetSVID err=%v want ErrMissingFetcher", err)
	}
}

func TestFileSourceGetSVID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	material := mustMaterial(t, "spiffe://example.org/example/agent/node-a")
	certPath := filepath.Join(dir, "svid.pem")
	keyPath := filepath.Join(dir, "key.pem")
	bundlePath := filepath.Join(dir, "bundle.pem")
	if err := os.WriteFile(certPath, material.CertChainPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, material.KeyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.WriteFile(bundlePath, material.BundlePEM, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}

	svid, err := (FileSource{
		CertPath:    certPath,
		KeyPath:     keyPath,
		BundlePath:  bundlePath,
		TrustDomain: "example.org",
	}).GetSVID(context.Background())
	if err != nil {
		t.Fatalf("GetSVID returned error: %v", err)
	}
	if got, want := svid.ID.String(), "spiffe://example.org/example/agent/node-a"; got != want {
		t.Fatalf("SVID ID=%q want %q", got, want)
	}
}

func TestFileSourceObservesTwoRotations(t *testing.T) {
	dir := t.TempDir()
	source := FileSource{CertPath: filepath.Join(dir, "cert"), KeyPath: filepath.Join(dir, "key"), BundlePath: filepath.Join(dir, "bundle"), TrustDomain: "example.org"}
	native := spiffe.X509Source{Context: context.Background(), Source: source}
	var previous string
	for i := 0; i < 3; i++ {
		material := mustMaterial(t, "spiffe://example.org/example/agent/a")
		for path, data := range map[string][]byte{source.CertPath: material.CertChainPEM, source.KeyPath: material.KeyPEM, source.BundlePath: material.BundlePEM} {
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		svid, err := native.GetX509SVID()
		if err != nil {
			t.Fatal(err)
		}
		current := string(svid.Certificates[0].Raw)
		if current == previous {
			t.Fatal("source pinned old SVID")
		}
		previous = current
		bundle, err := native.GetX509BundleForTrustDomain(svid.ID.TrustDomain())
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := gospiffex509svid.Verify(svid.Certificates, bundle); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSVIDFromMaterialRejectsTrustDomainMismatch(t *testing.T) {
	t.Parallel()

	_, err := SVIDFromMaterial(mustMaterial(t, "spiffe://example.org/example/controller/a"), "other.org")
	if !errors.Is(err, spiffe.ErrTrustDomainMismatch) {
		t.Fatalf("err=%v want ErrTrustDomainMismatch", err)
	}
}

func TestSVIDFromGoSpiffe(t *testing.T) {
	t.Parallel()

	material := mustMaterial(t, "spiffe://example.org/example/controller/a")
	gosvid, err := gospiffex509svid.Parse(material.CertChainPEM, material.KeyPEM)
	if err != nil {
		t.Fatalf("go-spiffe Parse returned error: %v", err)
	}
	svid, err := SVIDFromGoSpiffe(gosvid, "example.org")
	if err != nil {
		t.Fatalf("SVIDFromGoSpiffe returned error: %v", err)
	}
	if got, want := svid.ID.String(), "spiffe://example.org/example/controller/a"; got != want {
		t.Fatalf("SVID ID=%q want %q", got, want)
	}
	if svid.Key == nil {
		t.Fatal("SVID Key is nil")
	}
}

func mustMaterial(t *testing.T, spiffeID string) X509SVIDMaterial {
	t.Helper()

	rootCert, rootKey := mustRoot(t)
	leafCert, leafKey := mustLeaf(t, rootCert, rootKey, spiffeID)
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	return X509SVIDMaterial{
		CertChainPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafCert.Raw}),
		KeyPEM:       pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		BundlePEM:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootCert.Raw}),
	}
}

func mustRoot(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey root: %v", err)
	}
	now := time.Now().UTC()
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
	now := time.Now().UTC()
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
