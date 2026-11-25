package enrollment

import (
	"context"
	"crypto"
	"crypto/x509"
	"time"

	"github.com/fastabc/fastmesh/internal/protocol/bootstrap"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// ManagedTokenRequest identifies the workload authorized to enroll with a CSR.
type ManagedTokenRequest struct {
	Tenant, NodeName, WorkloadID string
	Description                  string
	TTL                          time.Duration
}

// ManagedIdentity holds the server-assigned identity and cell of a managed token.
type ManagedIdentity struct {
	NodeName   string `json:"node_name"`
	WorkloadID string `json:"workload_id"`
	Region     string `json:"region,omitempty"`
}

// CreateManaged issues an expiring token restricted to one registered public key
// and the controller cell that created it. It cannot authorize legacy registration.
func (m *Manager) CreateManaged(ctx context.Context, req ManagedTokenRequest, region string) (CreateResponse, error) {
	if req.Tenant == "" {
		req.Tenant = model.DefaultTenant
	}
	id, err := spiffeid.FromString(req.WorkloadID)
	if err != nil || id.String() != req.WorkloadID || !model.ExactName(req.Tenant) || !model.ExactName(req.NodeName) || req.TTL < 0 || req.TTL > time.Hour {
		return CreateResponse{}, model.ErrInvalid
	}
	if req.TTL == 0 {
		req.TTL = 15 * time.Minute
	}
	return m.create(ctx, CreateRequest{
		Tenant: req.Tenant, Description: req.Description, TTL: req.TTL, MaxUses: 1,
	}, &ManagedIdentity{NodeName: req.NodeName, WorkloadID: id.String(), Region: region})
}

type managedRegistration struct {
	region string
	key    crypto.PublicKey
	issue  func(context.Context, string, crypto.PublicKey) ([][]byte, error)
}

// RegisterManaged proves possession of the CSR key and commits the Node, token
// use and certificate chain in one transaction. A retry with the same key
// returns the committed chain. The issuer may be called again if commits race.
func (m *Manager) RegisterManaged(
	ctx context.Context, raw, region string, csr *x509.CertificateRequest,
	build func(Token, ManagedIdentity) (model.Node, error),
	issue func(context.Context, string, crypto.PublicKey) ([][]byte, error),
) (model.Node, [][]byte, error) {
	if csr == nil || csr.CheckSignature() != nil {
		return model.Node{}, nil, model.ErrInvalid
	}
	spki, err := x509.MarshalPKIXPublicKey(csr.PublicKey)
	if err != nil {
		return model.Node{}, nil, model.ErrInvalid
	}
	return m.register(ctx, raw, "", bootstrap.KeyID(spki), &managedRegistration{region: region, key: csr.PublicKey, issue: issue},
		func(tok storedToken) (model.Node, error) { return build(tok.Token, *tok.Managed) })
}
