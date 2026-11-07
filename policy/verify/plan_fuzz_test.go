package verify

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

func FuzzVerifyPlanBody(f *testing.F) {
	root, leaf, key := testSVIDChain(f, "spiffe://example.org/controller", time.Now())
	roots := x509.NewCertPool()
	roots.AddCert(root)
	verifier := SnapshotVerifier{Roots: roots, TrustDomain: "example.org", AllowedControllerIDs: []string{"spiffe://example.org/controller"}}
	body := []byte(`{"header":{"future":true},"topology":{}}`)
	payload, err := PlanBodySigningPayload(model.PlanTypeURL, body)
	if err != nil {
		f.Fatal(err)
	}
	sig := snapshot.Signature{Algorithm: "spiffe-x509-ed25519", KeyID: "spiffe://example.org/controller", CertChain: [][]byte{leaf.Raw}, Value: ed25519.Sign(key, payload)}
	if err := PlanBody(context.Background(), model.PlanTypeURL, body, sig, verifier); err != nil {
		f.Fatal(err)
	}
	f.Add(model.PlanTypeURL, body)
	f.Add(model.PlanTypeURL, append(bytes.Clone(body), ' '))
	f.Add(model.PlanTypeURL, []byte(nil))
	f.Add("fastmesh.plan.v2", body)
	f.Add("example.snapshot.v1", body)
	// Every byte position is exercised even by ordinary go test seed runs.
	for i := range body {
		changed := bytes.Clone(body)
		changed[i] ^= 1
		f.Add(model.PlanTypeURL, changed)
	}
	f.Fuzz(func(t *testing.T, typeURL string, candidate []byte) {
		err := PlanBody(context.Background(), typeURL, candidate, sig, verifier)
		wantValid := typeURL == model.PlanTypeURL && bytes.Equal(candidate, body)
		if (err == nil) != wantValid {
			t.Fatalf("raw signature verification valid=%v, want %v: %v", err == nil, wantValid, err)
		}
	})
}
