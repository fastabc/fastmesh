package verify

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

func TestStoredPlanVerificationOnlyRecoversExpiredLeaf(t *testing.T) {
	for _, kind := range []string{"expired-leaf", "not-yet-valid-leaf", "expired-root", "expired-intermediate", "not-yet-valid-intermediate", "wrong-roots", "wrong-controller", "wrong-domain", "bad-signature", "bad-body", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Now()
			pub, rootKey, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			root := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
			if kind == "expired-root" {
				root.NotAfter = now.Add(-30 * time.Second)
			}
			der, err := x509.CreateCertificate(rand.Reader, root, root, pub, rootKey)
			if err != nil {
				t.Fatal(err)
			}
			root, err = x509.ParseCertificate(der)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AddCert(root)
			parent, parentKey := root, rootKey
			var intermediates [][]byte
			if kind == "expired-intermediate" || kind == "not-yet-valid-intermediate" {
				pub, key, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				cert := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: root.NotBefore, NotAfter: now.Add(-30 * time.Second), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
				if kind == "not-yet-valid-intermediate" {
					cert.NotBefore, cert.NotAfter = now.Add(time.Minute), root.NotAfter
				}
				der, err := x509.CreateCertificate(rand.Reader, cert, root, pub, rootKey)
				if err != nil {
					t.Fatal(err)
				}
				parent, err = x509.ParseCertificate(der)
				if err != nil {
					t.Fatal(err)
				}
				parentKey, intermediates = key, [][]byte{der}
			}
			pub, key, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			uri, err := url.Parse("spiffe://test/controller")
			if err != nil {
				t.Fatal(err)
			}
			leaf := &x509.Certificate{SerialNumber: big.NewInt(3), NotBefore: root.NotBefore, NotAfter: now.Add(-time.Minute), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{uri}}
			if kind == "not-yet-valid-leaf" {
				leaf.NotBefore, leaf.NotAfter = now.Add(time.Minute), now.Add(time.Hour)
			}
			der, err = x509.CreateCertificate(rand.Reader, leaf, parent, pub, parentKey)
			if err != nil {
				t.Fatal(err)
			}
			leaf, err = x509.ParseCertificate(der)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte("exact stored body")
			payload, err := PlanBodySigningPayload(model.PlanTypeURL, body)
			if err != nil {
				t.Fatal(err)
			}
			sig := snapshot.Signature{Algorithm: "spiffe-x509-ed25519", KeyID: uri.String(), CertChain: append([][]byte{der}, intermediates...), Value: ed25519.Sign(key, payload)}
			v := SnapshotVerifier{Roots: roots, TrustDomain: "test", AllowedControllerIDs: []string{uri.String()}}
			ctx := context.Background()
			switch kind {
			case "wrong-roots":
				v.Roots = x509.NewCertPool()
			case "wrong-controller":
				v.AllowedControllerIDs = []string{"spiffe://test/other"}
			case "wrong-domain":
				v.TrustDomain = "other"
			case "bad-signature":
				sig.Value[0] ^= 1
			case "bad-body":
				body[0] ^= 1
			case "canceled":
				canceled, stop := context.WithCancel(ctx)
				stop()
				ctx = canceled
			}
			deadline, err := StoredPlanBody(ctx, model.PlanTypeURL, body, sig, v)
			if kind == "expired-leaf" {
				if err != nil || !deadline.Equal(leaf.NotAfter) {
					t.Fatalf("historical verification deadline=%v err=%v", deadline, err)
				}
				if err := PlanBody(ctx, model.PlanTypeURL, body, sig, v); !errors.Is(err, ErrVerificationFailed) {
					t.Fatalf("network verification accepted historical input: %v", err)
				}
			} else if err == nil {
				t.Fatalf("historical verification bypassed %s: deadline=%v", kind, deadline)
			}
		})
	}
}
