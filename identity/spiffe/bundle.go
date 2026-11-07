package spiffe

import "crypto/x509"

// BundlePool builds roots from the SVID's DER trust bundle and rejects partial
// or absent trust material rather than silently accepting only valid entries.
func BundlePool(svid SVID) (*x509.CertPool, error) {
	if len(svid.Bundle) == 0 {
		return nil, ErrMissingTrustBundle
	}
	roots := x509.NewCertPool()
	for _, raw := range svid.Bundle {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, err
		}
		roots.AddCert(cert)
	}
	return roots, nil
}
