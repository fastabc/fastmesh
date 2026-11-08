package verify

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

// PlanBodySigningPayload implements DSSE PAE over the exact received bytes.
// It intentionally does not parse, validate, or reserialize the body.
func PlanBodySigningPayload(typeURL string, body []byte) ([]byte, error) {
	if typeURL != model.PlanTypeURL {
		return nil, fmt.Errorf("%w: unsupported raw plan type", ErrVerificationFailed)
	}
	if len(body) > model.MaxPlanBodyBytes {
		return nil, model.ErrPlanTooLarge
	}
	prefix := "DSSEv1 " + strconv.Itoa(len(typeURL)) + " " + typeURL + " " + strconv.Itoa(len(body)) + " "
	return append([]byte(prefix), body...), nil
}

// PlanBody must precede any decoding of a plan body, including recovery.
func PlanBody(ctx context.Context, typeURL string, body []byte, sig snapshot.Signature, verifier SnapshotVerifier) error {
	if len(verifier.AllowedControllerIDs) == 0 {
		return fmt.Errorf("%w: missing controller allowlist", ErrVerificationFailed)
	}
	payload, err := PlanBodySigningPayload(typeURL, body)
	if err != nil {
		return err
	}
	return verifier.VerifyPayload(ctx, payload, sig)
}

// StoredPlanBody authenticates persisted input whose leaf signer has
// expired. A nonzero deadline must bound the recovered serving copy; it never
// grants validity to network input. Every non-leaf certificate remains subject
// to current validity, including roots selected by chain verification.
func StoredPlanBody(ctx context.Context, typeURL string, body []byte, sig snapshot.Signature, verifier SnapshotVerifier) (time.Time, error) {
	err := PlanBody(ctx, typeURL, body, sig, verifier)
	if err == nil {
		return time.Time{}, nil
	}
	var invalid x509.CertificateInvalidError
	if !errors.As(err, &invalid) || invalid.Reason != x509.Expired || invalid.Cert == nil {
		return time.Time{}, err
	}
	certs, parseErr := parseCertChain(sig.CertChain)
	if parseErr != nil || !bytes.Equal(invalid.Cert.Raw, certs[0].Raw) {
		return time.Time{}, err
	}
	now := time.Now()
	leaf := certs[0]
	if !now.After(leaf.NotAfter) {
		return time.Time{}, err // Expired also describes a not-yet-valid certificate.
	}
	at, first := leaf.NotAfter.Add(-time.Nanosecond), leaf.NotBefore
	intermediates := x509.NewCertPool()
	for _, cert := range certs[1:] {
		if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
			return time.Time{}, err
		}
		if cert.NotAfter.Before(at) {
			at = cert.NotAfter.Add(-time.Nanosecond)
		}
		if cert.NotBefore.After(first) {
			first = cert.NotBefore
		}
		intermediates.AddCert(cert)
	}
	if at.Before(first) {
		return time.Time{}, err
	}
	chains, chainErr := leaf.Verify(x509.VerifyOptions{Roots: verifier.Roots, Intermediates: intermediates, CurrentTime: at, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
	if chainErr != nil {
		return time.Time{}, fmt.Errorf("%w: %w", ErrVerificationFailed, chainErr)
	}
	currentChain := false
	for _, chain := range chains {
		valid := true
		for _, cert := range chain[1:] {
			valid = valid && !now.Before(cert.NotBefore) && !now.After(cert.NotAfter)
		}
		currentChain = currentChain || valid
	}
	if !currentChain {
		return time.Time{}, err
	}
	payload, payloadErr := PlanBodySigningPayload(typeURL, body)
	if payloadErr != nil {
		return time.Time{}, payloadErr
	}
	if err := verifier.verifyPayloadAt(ctx, payload, sig, at); err != nil {
		return time.Time{}, err
	}
	return leaf.NotAfter, nil
}

// PlanRecipient validates the authenticated header after raw verification.
func PlanRecipient(p model.Plan, recipient model.Session, highWater int64, now time.Time) error {
	if !p.EnvelopeValid() || p.Hash == "" {
		return fmt.Errorf("%w: invalid plan header", ErrVerificationFailed)
	}
	if p.Tenant != recipient.Tenant || p.NodeID != recipient.NodeID || p.Workload != recipient.Workload {
		return fmt.Errorf("%w: wrong plan recipient", ErrVerificationFailed)
	}
	if p.InputRevision < highWater {
		return fmt.Errorf("%w: plan revision regressed", ErrVerificationFailed)
	}
	if !now.Before(p.NotAfter) {
		return fmt.Errorf("%w: plan expired", ErrVerificationFailed)
	}
	return nil
}
