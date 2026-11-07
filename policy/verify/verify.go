package verify

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

var ErrVerificationFailed = errors.New("verify: snapshot verification failed")

// Content checks the canonical revision; it does not authenticate a sender.
func Content(snap *snapshot.Snapshot) error {
	if snap == nil {
		return errors.New("verify: nil snapshot")
	}
	want := snapshot.CanonicalHash(snap)
	if snap.Revision != want {
		return fmt.Errorf("verify: revision %q does not match content hash %q", snap.Revision, want)
	}
	return nil
}

// SignatureVerifyFunc checks a structured detached snapshot signature.
type SignatureVerifyFunc func(ctx context.Context, snap *snapshot.Snapshot, sig snapshot.Signature) error

// SnapshotVerifier verifies structured controller snapshot signatures.
type SnapshotVerifier struct {
	NodeID               string
	Tenant               string
	TrustDomain          spiffe.TrustDomain
	Roots                *x509.CertPool
	AllowedControllerIDs []string
}

// Verify checks a snapshot's content hash and detached signature.
func (v SnapshotVerifier) Verify(ctx context.Context, snap *snapshot.Snapshot, sig snapshot.Signature) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snap == nil || (v.NodeID != "" && snap.NodeID != v.NodeID) || (v.Tenant != "" && snap.Tenant != v.Tenant) {
		return fmt.Errorf("%w: wrong snapshot recipient", ErrVerificationFailed)
	}
	if err := Content(snap); err != nil {
		return fmt.Errorf("%w: %v", ErrVerificationFailed, err)
	}
	payload, err := snapshot.SigningPayload(snap)
	if err != nil {
		return err
	}
	return v.VerifyPayload(ctx, payload, sig)
}

// VerifyPayload checks a detached asymmetric signature against trusted roots
// and allowed controller IDs. Callers supply a domain-separated payload.
func (v SnapshotVerifier) VerifyPayload(ctx context.Context, payload []byte, sig snapshot.Signature) error {
	return v.verifyPayloadAt(ctx, payload, sig, time.Time{})
}

func (v SnapshotVerifier) verifyPayloadAt(ctx context.Context, payload []byte, sig snapshot.Signature, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sig.Algorithm == "" {
		return fmt.Errorf("%w: missing signature algorithm", ErrVerificationFailed)
	}
	if len(sig.Value) == 0 {
		return fmt.Errorf("%w: missing signature value", ErrVerificationFailed)
	}
	if v.Roots == nil {
		return fmt.Errorf("%w: missing trusted roots", ErrVerificationFailed)
	}
	certs, err := parseCertChain(sig.CertChain)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVerificationFailed, err)
	}
	svid, err := spiffe.X509SVIDFromCertificates(certs, nil, spiffe.VerifyOptions{
		TrustDomain: v.TrustDomain,
		Roots:       v.Roots,
		CurrentTime: at,
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrVerificationFailed, err)
	}
	controllerID := svid.ID.String()
	if sig.KeyID != "" && sig.KeyID != controllerID {
		return fmt.Errorf("%w: key id %q does not match certificate identity %q", ErrVerificationFailed, sig.KeyID, controllerID)
	}
	if len(v.AllowedControllerIDs) > 0 && !allowedController(v.AllowedControllerIDs, controllerID) {
		return fmt.Errorf("%w: controller identity %q is not allowed", ErrVerificationFailed, controllerID)
	}
	if err := verifyDetached(sig.Algorithm, certs[0].PublicKey, payload, sig.Value); err != nil {
		return fmt.Errorf("%w: %v", ErrVerificationFailed, err)
	}
	return nil
}

func parseCertChain(raw [][]byte) ([]*x509.Certificate, error) {
	if len(raw) == 0 {
		return nil, errors.New("missing certificate chain")
	}
	certs := make([]*x509.Certificate, 0, len(raw))
	for _, der := range raw {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("parse certificate: %v", err)
		}
		certs = append(certs, cert)
	}
	return certs, nil
}

func allowedController(allowed []string, id string) bool {
	for _, candidate := range allowed {
		if strings.TrimSpace(candidate) == id {
			return true
		}
	}
	return false
}

func verifyDetached(algorithm string, publicKey any, payload, sig []byte) error {
	switch algorithm {
	case "spiffe-x509-ed25519":
		key, ok := publicKey.(ed25519.PublicKey)
		if !ok {
			return fmt.Errorf("ed25519 signature has %T key", publicKey)
		}
		if !ed25519.Verify(key, payload, sig) {
			return errors.New("signature does not verify")
		}
		return nil
	case "spiffe-x509-sha256-rsa":
		key, ok := publicKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("rsa signature has %T key", publicKey)
		}
		sum := sha256.Sum256(payload)
		if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
			return errors.New("signature does not verify")
		}
		return nil
	case "spiffe-x509-sha256-ecdsa":
		key, ok := publicKey.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("ecdsa signature has %T key", publicKey)
		}
		sum := sha256.Sum256(payload)
		if !ecdsa.VerifyASN1(key, sum[:], sig) {
			return errors.New("signature does not verify")
		}
		return nil
	default:
		return fmt.Errorf("unsupported signature algorithm %q", algorithm)
	}
}
