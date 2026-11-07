package spiffe

import (
	"context"
	"crypto/tls"
	"errors"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
)

// ClientTLSConfig pins the server identity and follows live SVID/root rotation.
func ClientTLSConfig(ctx context.Context, source Source, allowed []string, trustDomain string) (*tls.Config, error) {
	if source == nil {
		return nil, ErrInvalidSVID
	}
	svid, err := source.GetSVID(ctx)
	if err != nil {
		return nil, err
	}
	if len(allowed) == 0 {
		return nil, errors.New("spiffe: allowed controller SPIFFE IDs are required")
	}
	if trustDomain != "" && svid.ID.TrustDomain.String() != trustDomain {
		return nil, ErrTrustDomainMismatch
	}
	ids := make([]spiffeid.ID, 0, len(allowed))
	for _, raw := range allowed {
		id, err := spiffeid.FromString(raw)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	native := X509Source{Context: ctx, Source: source}
	if _, err := native.GetX509SVID(); err != nil {
		return nil, err
	}
	config := tlsconfig.MTLSClientConfig(native, native, tlsconfig.AuthorizeOneOf(ids...))
	return config, nil
}
