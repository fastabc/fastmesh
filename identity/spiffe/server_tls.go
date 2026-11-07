package spiffe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
)

var ErrMissingTrustBundle = errors.New("spiffe: serve identity missing trust bundle")

// ServerTLSConfig observes the identity source on every handshake. Standard
// chain verification is retained so HTTPS handlers receive VerifiedChains.
func ServerTLSConfig(ctx context.Context, source Source, trustDomain string, optional bool) (*tls.Config, error) {
	native := X509Source{Context: ctx, Source: source}
	build := func() (*tls.Config, error) {
		svid, err := source.GetSVID(ctx)
		if err != nil {
			return nil, err
		}
		if trustDomain != "" && svid.ID.TrustDomain.String() != trustDomain {
			return nil, ErrTrustDomainMismatch
		}
		td, err := spiffeid.TrustDomainFromString(svid.ID.TrustDomain.String())
		if err != nil {
			return nil, err
		}
		roots, err := liveRoots(svid.Bundle)
		if err != nil {
			return nil, err
		}
		cfg := tlsconfig.MTLSServerConfig(native, native, tlsconfig.AuthorizeMemberOf(td))
		cfg.NextProtos = []string{"h2", "http/1.1"}
		cfg.ClientCAs = roots
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		if optional {
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
			verify := cfg.VerifyPeerCertificate
			cfg.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
				if len(raw) == 0 {
					return nil
				}
				return verify(raw, chains)
			}
		}
		return cfg, nil
	}
	cfg, err := build()
	if err != nil {
		return nil, err
	}
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		fresh, err := build()
		if err != nil {
			return nil, err
		}
		// REST may install a public certificate source after constructing the
		// identity policy. Preserve that source while rotating client roots.
		fresh.GetCertificate = cfg.GetCertificate
		fresh.NextProtos = cfg.NextProtos
		return fresh, nil
	}
	return cfg, nil
}

func liveRoots(bundle [][]byte) (*x509.CertPool, error) { return BundlePool(SVID{Bundle: bundle}) }
