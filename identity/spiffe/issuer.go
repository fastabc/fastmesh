package spiffe

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"math/big"
	"net/url"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// X509Issuer issues short-lived SVIDs using a provisioned CA chain and signer.
// The first certificate must be the signer's CA certificate.
type X509Issuer struct {
	Chain []*x509.Certificate
	Key   crypto.Signer
	TTL   time.Duration
}

// IssueSVID signs a leaf carrying the assigned SPIFFE URI.
func (s X509Issuer) IssueSVID(ctx context.Context, id spiffeid.ID, key crypto.PublicKey) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id.IsZero() || len(s.Chain) == 0 || s.Key == nil || s.TTL <= 0 {
		return nil, errors.New("spiffe: SVID issuer requires a CA chain, key and positive TTL")
	}
	now := time.Now()
	expiry := now.Add(s.TTL)
	for _, ca := range s.Chain {
		if ca == nil || !ca.IsCA || ca.KeyUsage&x509.KeyUsageCertSign == 0 || now.Before(ca.NotBefore) || !now.Before(ca.NotAfter) {
			return nil, errors.New("spiffe: invalid or expired issuing CA")
		}
		if ca.NotAfter.Before(expiry) {
			expiry = ca.NotAfter
		}
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	leaf := &x509.Certificate{
		SerialNumber: serial.Add(serial, big.NewInt(1)), NotBefore: now.Add(-time.Minute), NotAfter: expiry,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:        []*url.URL{id.URL()},
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, s.Chain[0], key, s.Key)
	if err != nil {
		return nil, err
	}
	out := [][]byte{der}
	for _, ca := range s.Chain {
		out = append(out, ca.Raw)
	}
	return out, nil
}
