package spiffe

import (
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/url"
	"time"

	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

var (
	ErrInvalidSVID         = errors.New("spiffe: invalid x509 svid")
	ErrTrustDomainMismatch = errors.New("spiffe: trust domain mismatch")
)

// VerifyOptions controls X.509 SVID validation.
type VerifyOptions struct {
	TrustDomain TrustDomain
	Roots       *x509.CertPool
	CurrentTime time.Time
}

// X509SVIDFromPEM parses a PEM certificate chain and validates it as an SVID.
func X509SVIDFromPEM(certPEM []byte, key crypto.Signer, opts VerifyOptions) (SVID, error) {
	var certs []*x509.Certificate
	rest := certPEM
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return SVID{}, err
		}
		certs = append(certs, cert)
	}
	return X509SVIDFromCertificates(certs, key, opts)
}

// X509SVIDFromCertificates validates a parsed X.509 chain and extracts the
// leaf SPIFFE ID from its URI SAN.
func X509SVIDFromCertificates(certs []*x509.Certificate, key crypto.Signer, opts VerifyOptions) (SVID, error) {
	if len(certs) == 0 || certs[0] == nil {
		return SVID{}, ErrInvalidSVID
	}
	id, err := IDFromURIs(certs[0].URIs)
	if err != nil {
		return SVID{}, err
	}
	if opts.TrustDomain != "" && id.TrustDomain != opts.TrustDomain {
		return SVID{}, ErrTrustDomainMismatch
	}
	if opts.Roots != nil {
		verifyOpts := x509.VerifyOptions{
			Roots:       opts.Roots,
			CurrentTime: opts.CurrentTime,
			KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}
		if verifyOpts.CurrentTime.IsZero() {
			verifyOpts.CurrentTime = time.Now().UTC()
		}
		if len(certs) > 1 {
			verifyOpts.Intermediates = x509.NewCertPool()
			for _, cert := range certs[1:] {
				verifyOpts.Intermediates.AddCert(cert)
			}
		}
		if _, err := certs[0].Verify(verifyOpts); err != nil {
			return SVID{}, err
		}
	}
	chain := make([][]byte, 0, len(certs))
	for _, cert := range certs {
		chain = append(chain, append([]byte(nil), cert.Raw...))
	}
	return SVID{
		ID:        id,
		NotAfter:  certs[0].NotAfter,
		CertChain: chain,
		Key:       key,
	}, nil
}

// IDFromURIs extracts exactly one SPIFFE URI from a certificate URI SAN list.
func IDFromURIs(uris []*url.URL) (ID, error) {
	if len(uris) != 1 || uris[0] == nil {
		return ID{}, ErrInvalidSVID
	}
	id, err := x509svid.IDFromCert(&x509.Certificate{URIs: uris})
	if err != nil {
		return ID{}, errors.Join(ErrInvalidSVID, err)
	}
	return ParseID(id.String())
}
