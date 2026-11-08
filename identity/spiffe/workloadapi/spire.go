package workloadapi

import (
	"context"
	"github.com/fastabc/fastmesh/identity/spiffe"
	gospiffex509bundle "github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	gospiffeworkloadapi "github.com/spiffe/go-spiffe/v2/workloadapi"
	"sync"
)

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
