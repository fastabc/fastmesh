package workloadapi

import (
	"context"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"os"
)

// FileSource reads SVID material from PEM files. It is useful for standalone
// mode and for exercising the same parser path without a live SPIRE agent.
type FileSource struct {
	CertPath    string
	KeyPath     string
	BundlePath  string
	TrustDomain spiffe.TrustDomain
}

// GetSVID satisfies spiffe.Source.
func (s FileSource) GetSVID(context.Context) (spiffe.SVID, error) {
	certPEM, err := os.ReadFile(s.CertPath)
	if err != nil {
		return spiffe.SVID{}, err
	}
	keyPEM, err := os.ReadFile(s.KeyPath)
	if err != nil {
		return spiffe.SVID{}, err
	}
	var bundlePEM []byte
	if s.BundlePath != "" {
		bundlePEM, err = os.ReadFile(s.BundlePath)
		if err != nil {
			return spiffe.SVID{}, err
		}
	}
	return SVIDFromMaterial(X509SVIDMaterial{
		CertChainPEM: certPEM,
		KeyPEM:       keyPEM,
		BundlePEM:    bundlePEM,
	}, s.TrustDomain)
}
