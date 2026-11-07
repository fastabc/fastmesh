package snapshot

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
)

var ErrMissingSVIDSource = errors.New("snapshot: missing svid source")

// SignPayload signs a domain-separated payload with current identity material.
func SignPayload(ctx context.Context, source spiffe.Source, payload []byte, clock func() time.Time) (Signature, error) {
	if source == nil {
		return Signature{}, ErrMissingSVIDSource
	}
	svid, err := source.GetSVID(ctx)
	if err != nil {
		return Signature{}, err
	}
	if svid.Key == nil {
		return Signature{}, errors.New("snapshot: svid has no signing key")
	}
	if len(svid.CertChain) == 0 {
		return Signature{}, errors.New("snapshot: svid has no certificate chain")
	}
	now := time.Now().UTC()
	if clock != nil {
		now = clock().UTC()
	}
	if !svid.NotAfter.IsZero() && now.After(svid.NotAfter) {
		return Signature{}, errors.New("snapshot: svid is expired")
	}
	algorithm, value, err := signPayload(svid.Key, payload)
	if err != nil {
		return Signature{}, err
	}
	return Signature{
		Algorithm: algorithm,
		KeyID:     svid.ID.String(),
		Value:     value,
		CertChain: cloneCertChain(svid.CertChain),
	}, nil
}

func signPayload(key crypto.Signer, payload []byte) (string, []byte, error) {
	switch key.Public().(type) {
	case ed25519.PublicKey:
		value, err := key.Sign(rand.Reader, payload, crypto.Hash(0))
		return "spiffe-x509-ed25519", value, err
	case *rsa.PublicKey:
		sum := sha256.Sum256(payload)
		value, err := key.Sign(rand.Reader, sum[:], crypto.SHA256)
		return "spiffe-x509-sha256-rsa", value, err
	case *ecdsa.PublicKey:
		sum := sha256.Sum256(payload)
		value, err := key.Sign(rand.Reader, sum[:], crypto.SHA256)
		return "spiffe-x509-sha256-ecdsa", value, err
	default:
		return "", nil, fmt.Errorf("snapshot: unsupported svid key type %T", key.Public())
	}
}

func cloneCertChain(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, cert := range in {
		out = append(out, append([]byte(nil), cert...))
	}
	return out
}
