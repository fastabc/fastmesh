package spiffe

import (
	"errors"
	"testing"
)

func TestParseID(t *testing.T) {
	t.Parallel()

	id, err := ParseID("spiffe://Example.Org/example/agent/node-a")
	if err != nil {
		t.Fatalf("ParseID returned error: %v", err)
	}
	if got, want := id.String(), "spiffe://example.org/example/agent/node-a"; got != want {
		t.Fatalf("ID.String()=%q want %q", got, want)
	}
}

func TestParseIDRejectsNonSPIFFE(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"https://example.org/example/controller",
		"spiffe://example.org",
		"spiffe://example.org/",
		"spiffe://example.org/example/controller?debug=true",
		"spiffe://example.org/example/controller#frag",
	}
	for _, raw := range tests {
		if _, err := ParseID(raw); !errors.Is(err, ErrInvalidID) {
			t.Fatalf("ParseID(%q) err=%v want ErrInvalidID", raw, err)
		}
	}
}

func TestParseTrustDomain(t *testing.T) {
	t.Parallel()

	td, err := ParseTrustDomain("Example.Org")
	if err != nil {
		t.Fatalf("ParseTrustDomain returned error: %v", err)
	}
	if got, want := td.String(), "example.org"; got != want {
		t.Fatalf("TrustDomain.String()=%q want %q", got, want)
	}
}
