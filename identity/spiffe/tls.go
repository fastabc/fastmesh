package spiffe

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

// X509Source adapts the existing source contract to go-spiffe TLS callbacks.
// Each callback observes current credentials and roots, including file changes.
// The context belongs to the transport lifetime, not an individual handshake.
type X509Source struct {
	Context context.Context
	Source  Source
}

func (s X509Source) GetX509SVID() (*x509svid.SVID, error) {
	material, err := s.Source.GetSVID(s.Context)
	if err != nil {
		return nil, err
	}
	if len(material.CertChain) == 0 || material.Key == nil {
		return nil, ErrInvalidSVID
	}
	certs := make([]*x509.Certificate, 0, len(material.CertChain))
	for _, der := range material.CertChain {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}
	id, err := x509svid.IDFromCert(certs[0])
	if err != nil {
		return nil, err
	}
	if id.String() != material.ID.String() {
		return nil, ErrInvalidSVID
	}
	certKey, err := x509.MarshalPKIXPublicKey(certs[0].PublicKey)
	if err != nil {
		return nil, err
	}
	sourceKey, err := x509.MarshalPKIXPublicKey(material.Key.Public())
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(certKey, sourceKey) {
		return nil, errors.New("spiffe: SVID key does not match certificate")
	}
	return &x509svid.SVID{ID: id, Certificates: certs, PrivateKey: material.Key}, nil
}

func (s X509Source) GetX509BundleForTrustDomain(td spiffeid.TrustDomain) (*x509bundle.Bundle, error) {
	material, err := s.Source.GetSVID(s.Context)
	if err != nil {
		return nil, err
	}
	if material.ID.TrustDomain.String() != td.String() {
		return nil, ErrTrustDomainMismatch
	}
	if len(material.Bundle) == 0 {
		return nil, errors.New("spiffe: missing trust bundle")
	}
	certs := make([]*x509.Certificate, 0, len(material.Bundle))
	for _, der := range material.Bundle {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}
	return x509bundle.FromX509Authorities(td, certs), nil
}
