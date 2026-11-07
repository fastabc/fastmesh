package spiffe_test

import (
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/test/fixture"
)

func TestBundlePool(t *testing.T) {
	issue, roots := fixture.Issuer(t)
	svid := issue("client", 2)
	pool, err := spiffe.BundlePool(svid)
	if err != nil || !pool.Equal(roots) {
		t.Fatalf("bundle: %v", err)
	}
	if _, err := spiffe.BundlePool(spiffe.SVID{}); !errors.Is(err, spiffe.ErrMissingTrustBundle) {
		t.Fatalf("empty: %v", err)
	}
	svid.Bundle = append(svid.Bundle, []byte("invalid"))
	if _, err := spiffe.BundlePool(svid); err == nil {
		t.Fatal("accepted partially malformed bundle")
	}
}

func TestSentinelMessagesUsePackagePrefix(t *testing.T) {
	for err, want := range map[error]string{spiffe.ErrMissingTrustBundle: "spiffe: serve identity missing trust bundle"} {
		if err.Error() != want {
			t.Errorf("%q, want %q", err, want)
		}
	}
}
