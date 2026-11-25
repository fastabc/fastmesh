// Package bootstrap defines the managed enrollment HTTPS exchange.
package bootstrap

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

const MaxRequestSize = 16 << 10
const MaxResponseSize = 256 << 10

type Request struct {
	Token        string            `json:"token,omitempty"`
	NodeID       string            `json:"node_id,omitempty"`
	CSR          []byte            `json:"csr"`
	Labels       map[string]string `json:"labels,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	AgentVersion string            `json:"agent_version,omitempty"`
	Runtime      string            `json:"runtime,omitempty"`
}

type Response struct {
	NodeID               string   `json:"node_id"`
	Tenant               string   `json:"tenant"`
	TrustDomain          string   `json:"trust_domain"`
	Controllers          []string `json:"controllers"`
	AllowedControllerIDs []string `json:"allowed_controller_ids"`
	Chain                [][]byte `json:"chain"`
	Bundle               [][]byte `json:"bundle"`
}

func KeyID(spki []byte) string {
	sum := sha256.Sum256(spki)
	return hex.EncodeToString(sum[:])
}

// VerifyChain binds a mesh leaf to a key and trust domain. A mesh workload
// uses its SVID as both client and server.
func VerifyChain(chain, bundle [][]byte, key crypto.PublicKey, domain string) (*x509.Certificate, string, error) {
	if len(chain) == 0 || len(chain) > 8 || len(bundle) > 32 {
		return nil, "", spiffe.ErrInvalidSVID
	}
	total := 0
	for _, certs := range [][][]byte{chain, bundle} {
		for _, der := range certs {
			total += len(der)
		}
	}
	if total > MaxResponseSize/2 {
		return nil, "", spiffe.ErrInvalidSVID
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return nil, "", err
	}
	id, err := x509svid.IDFromCert(leaf)
	if err != nil || id.TrustDomain().String() != domain {
		return nil, "", spiffe.ErrInvalidSVID
	}
	expected, err := x509.MarshalPKIXPublicKey(key)
	if err != nil || !bytes.Equal(leaf.RawSubjectPublicKeyInfo, expected) {
		return nil, "", spiffe.ErrInvalidSVID
	}
	roots, err := spiffe.BundlePool(spiffe.SVID{Bundle: bundle})
	if err != nil {
		return nil, "", err
	}
	intermediates := x509.NewCertPool()
	for _, der := range chain[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, "", err
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return nil, "", err
		}
	}
	return leaf, id.String(), nil
}
