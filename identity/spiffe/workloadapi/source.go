// Package workloadapi wires SPIFFE Workload API material into spiffe.Source.
package workloadapi

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"sync"

	"github.com/fastabc/fastmesh/identity/spiffe"
	gospiffex509bundle "github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	gospiffespiffeid "github.com/spiffe/go-spiffe/v2/spiffeid"
	gospiffex509svid "github.com/spiffe/go-spiffe/v2/svid/x509svid"
	gospiffeworkloadapi "github.com/spiffe/go-spiffe/v2/workloadapi"
)

var (
	ErrMissingFetcher = errors.New("workloadapi: missing fetcher")
	ErrInvalidKeyPEM  = errors.New("workloadapi: invalid key pem")
)

// X509SVIDMaterial is the subset of Workload API material needed by fastmesh.
type X509SVIDMaterial struct {
	CertChainPEM []byte
	KeyPEM       []byte
	BundlePEM    []byte
}

// Fetcher fetches current X.509 SVID material from a Workload API transport.
//
// A future gRPC implementation should satisfy this interface. Keeping the
// transport out of Source prevents generated protobuf code from leaking through
// controller and agent assembly.
type Fetcher interface {
	FetchX509SVID(ctx context.Context) (X509SVIDMaterial, error)
}

// Source implements spiffe.Source from Workload API material.
type Source struct {
	Fetcher     Fetcher
	TrustDomain spiffe.TrustDomain
}

// GetSVID satisfies spiffe.Source.
func (s Source) GetSVID(ctx context.Context) (spiffe.SVID, error) {
	if s.Fetcher == nil {
		return spiffe.SVID{}, ErrMissingFetcher
	}
	material, err := s.Fetcher.FetchX509SVID(ctx)
	if err != nil {
		return spiffe.SVID{}, err
	}
	return SVIDFromMaterial(material, s.TrustDomain)
}

// SPIRESource fetches X.509 SVIDs from the SPIRE Agent Workload API.
type SPIRESource struct {
	mu     sync.Mutex
	source *gospiffeworkloadapi.X509Source

	Addr        string
	TrustDomain spiffe.TrustDomain
}

// Start waits for initial credentials. ctx bounds initialization only; the
// owner must Close the source after all nodes using it have stopped.
func (s *SPIRESource) Start(ctx context.Context) error {
	_, err := s.GetSVID(ctx)
	return err
}

// GetSVID satisfies spiffe.Source.
func (s *SPIRESource) GetSVID(ctx context.Context) (spiffe.SVID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return spiffe.SVID{}, err
	}
	if s.source == nil {
		opts := []gospiffeworkloadapi.ClientOption{}
		if s.Addr != "" {
			opts = append(opts, gospiffeworkloadapi.WithAddr(s.Addr))
		}
		source, err := gospiffeworkloadapi.NewX509Source(ctx, gospiffeworkloadapi.WithClientOptions(opts...))
		if err != nil {
			return spiffe.SVID{}, err
		}
		s.source = source
	}
	svid, err := s.source.GetX509SVID()
	if err != nil {
		return spiffe.SVID{}, err
	}
	bundle, err := s.source.GetX509BundleForTrustDomain(svid.ID.TrustDomain())
	if err != nil {
		return spiffe.SVID{}, err
	}
	return SVIDFromGoSpiffeWithBundle(svid, gospiffex509bundle.NewSet(bundle), s.TrustDomain)
}

// Close releases the Workload API watch owned by this source.
func (s *SPIRESource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.source == nil {
		return nil
	}
	err := s.source.Close()
	s.source = nil
	return err
}

// SVIDFromGoSpiffe converts a go-spiffe X.509 SVID into the fastmesh
// SVID shape.
func SVIDFromGoSpiffe(in *gospiffex509svid.SVID, trustDomain spiffe.TrustDomain) (spiffe.SVID, error) {
	if in == nil || len(in.Certificates) == 0 {
		return spiffe.SVID{}, spiffe.ErrInvalidSVID
	}
	id, err := spiffe.ParseID(in.ID.String())
	if err != nil {
		return spiffe.SVID{}, err
	}
	if trustDomain != "" && id.TrustDomain != trustDomain {
		return spiffe.SVID{}, spiffe.ErrTrustDomainMismatch
	}
	chain := make([][]byte, 0, len(in.Certificates))
	for _, cert := range in.Certificates {
		chain = append(chain, append([]byte(nil), cert.Raw...))
	}
	return spiffe.SVID{
		ID:        id,
		NotAfter:  in.Certificates[0].NotAfter,
		CertChain: chain,
		Key:       in.PrivateKey,
	}, nil
}

// SVIDFromGoSpiffeWithBundle converts a go-spiffe SVID and bundle set into
// fastmesh SVID representation.
func SVIDFromGoSpiffeWithBundle(in *gospiffex509svid.SVID, bundles *gospiffex509bundle.Set, trustDomain spiffe.TrustDomain) (spiffe.SVID, error) {
	svid, err := SVIDFromGoSpiffe(in, trustDomain)
	if err != nil {
		return spiffe.SVID{}, err
	}
	if bundles == nil {
		return svid, nil
	}
	td, err := gospiffespiffeid.TrustDomainFromString(svid.ID.TrustDomain.String())
	if err != nil {
		return spiffe.SVID{}, err
	}
	bundle, ok := bundles.Get(td)
	if !ok {
		return svid, nil
	}
	for _, cert := range bundle.X509Authorities() {
		svid.Bundle = append(svid.Bundle, append([]byte(nil), cert.Raw...))
	}
	return svid, nil
}

// SVIDFromMaterial validates Workload API material and returns an SVID.
func SVIDFromMaterial(material X509SVIDMaterial, trustDomain spiffe.TrustDomain) (spiffe.SVID, error) {
	key, err := ParsePrivateKeyPEM(material.KeyPEM)
	if err != nil {
		return spiffe.SVID{}, err
	}
	roots, bundle, err := certPoolFromPEM(material.BundlePEM)
	if err != nil {
		return spiffe.SVID{}, err
	}
	svid, err := spiffe.X509SVIDFromPEM(material.CertChainPEM, key, spiffe.VerifyOptions{
		TrustDomain: trustDomain,
		Roots:       roots,
	})
	if err != nil {
		return spiffe.SVID{}, err
	}
	svid.Bundle = bundle
	return svid, nil
}

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

// ParsePrivateKeyPEM parses PKCS#8, PKCS#1 RSA, or SEC1 EC private keys.
func ParsePrivateKeyPEM(keyPEM []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, ErrInvalidKeyPEM
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return signerFromKey(key)
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, ErrInvalidKeyPEM
}

func signerFromKey(key any) (crypto.Signer, error) {
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return k, nil
	case *ecdsa.PrivateKey:
		return k, nil
	case ed25519.PrivateKey:
		return k, nil
	default:
		return nil, ErrInvalidKeyPEM
	}
}

func certPoolFromPEM(bundlePEM []byte) (*x509.CertPool, [][]byte, error) {
	if len(bundlePEM) == 0 {
		return nil, nil, nil
	}
	var bundle [][]byte
	rest := bundlePEM
	for {
		block, next := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = next
		if block.Type != "CERTIFICATE" {
			continue
		}
		bundle = append(bundle, append([]byte(nil), block.Bytes...))
	}
	if len(bundle) == 0 {
		return nil, nil, spiffe.ErrInvalidSVID
	}
	roots, err := spiffe.BundlePool(spiffe.SVID{Bundle: bundle})
	if err != nil {
		return nil, nil, errors.Join(spiffe.ErrInvalidSVID, err)
	}
	return roots, bundle, nil
}
