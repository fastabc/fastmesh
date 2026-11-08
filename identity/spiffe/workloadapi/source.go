// Package workloadapi wires SPIFFE Workload API material into spiffe.Source.
package workloadapi

import (
	"context"
	"errors"
	"github.com/fastabc/fastmesh/identity/spiffe"
)

var (
	ErrMissingFetcher = errors.New("workloadapi: missing fetcher")
)

// X509SVIDMaterial is the subset of Workload API material needed by fastmesh.
type X509SVIDMaterial struct {
	CertChainPEM []byte
	KeyPEM       []byte
	BundlePEM    []byte
}

// Fetcher fetches current X.509 SVID material from a Workload API transport.
//
// Custom transports can implement Fetcher without exposing generated messages.
// SPIRESource provides the standard watched Workload API integration.
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
