// Package fixture contains shared test inputs, never production dependencies.
package fixture

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"math/big"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
)

func Issuer(t testing.TB) (func(string, int64) spiffe.SVID, *x509.CertPool) {
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
	issue := func(path string, serial int64) spiffe.SVID {
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
		return spiffe.SVID{ID: id, CertChain: [][]byte{der}, Bundle: [][]byte{ca.Raw}, Key: leafKey, NotAfter: leaf.NotAfter}
	}
	return issue, roots
}

func EtcdEndpoints(t testing.TB) string {
	t.Helper()
	return os.Getenv("FASTMESH_ETCD_ENDPOINTS")
}
