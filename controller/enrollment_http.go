package controller

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/protocol/bootstrap"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// SVIDIssuer signs a leaf for the server-assigned ID and CSR public key.
// Implementations may be called concurrently and return a leaf-first DER chain.
type SVIDIssuer interface {
	IssueSVID(context.Context, spiffeid.ID, crypto.PublicKey) ([][]byte, error)
}

// EnrollmentConfig enables enrollment over a host-owned HTTPS listener.
type EnrollmentConfig struct {
	Issuer SVIDIssuer
	// Include every authorized Controller ID in HA deployments.
	// An empty list uses the serving Controller's ID.
	AllowedControllerIDs []string
}

// WithEnrollment enables managed enrollment. The embedding app owns the HTTPS
// listener and authorizes callers of the local token-creation API.
func WithEnrollment(cfg EnrollmentConfig) Option {
	cfg.AllowedControllerIDs = slices.Clone(cfg.AllowedControllerIDs)
	return func(c *Config) { c.enrollment = &cfg }
}

func validateEnrollmentConfig(cfg Config) error {
	if cfg.enrollment == nil {
		return nil
	}
	if cfg.DevInsecure || cfg.enrollment.Issuer == nil || cfg.TrustDomain == "" {
		return errors.New("mesh controller: managed enrollment requires secure mode, trust domain and SVID issuer")
	}
	if td, err := spiffeid.TrustDomainFromString(cfg.TrustDomain); err != nil || td.String() != cfg.TrustDomain {
		return errors.New("mesh controller: invalid enrollment trust domain")
	}
	for _, addr := range cfg.ControllerAddrs {
		if strings.TrimSpace(addr) == "" {
			return errors.New("mesh controller: empty enrollment controller address")
		}
	}
	for _, raw := range cfg.enrollment.AllowedControllerIDs {
		id, err := spiffeid.FromString(raw)
		if err != nil || id.String() != raw || id.TrustDomain().String() != cfg.TrustDomain {
			return errors.New("mesh controller: invalid enrollment controller identity")
		}
	}
	return nil
}

// CreateEnrollmentToken issues a short-lived, single-use managed join token.
// Embedding applications must authorize the requested workload ID and tenant.
func (c *Controller) CreateEnrollmentToken(ctx context.Context, req enrollment.ManagedTokenRequest) (enrollment.CreateResponse, error) {
	if c.cfg.enrollment == nil {
		return enrollment.CreateResponse{}, errors.New("mesh controller: managed enrollment is disabled")
	}
	if err := c.requireLeader(); err != nil {
		return enrollment.CreateResponse{}, err
	}
	svid, ids, err := c.enrollmentIdentity(ctx)
	if err != nil {
		return enrollment.CreateResponse{}, err
	}
	if _, err := enrollmentWorkload(req.WorkloadID, svid, ids); err != nil {
		return enrollment.CreateResponse{}, err
	}
	return c.tokens.CreateManaged(ctx, req, c.cfg.Region)
}

// RevokeEnrollmentToken invalidates a token and prevents retrieval of a prior
// join response. Use RevokeNode to remove an already enrolled workload.
func (c *Controller) RevokeEnrollmentToken(ctx context.Context, id string) error {
	if c.cfg.enrollment == nil {
		return errors.New("mesh controller: managed enrollment is disabled")
	}
	if err := c.requireLeader(); err != nil {
		return err
	}
	return c.tokens.RevokeManaged(ctx, id)
}

func (c *Controller) enrollmentIdentity(ctx context.Context) (spiffe.SVID, []string, error) {
	svid, err := c.cfg.Identity.GetSVID(ctx)
	if err != nil {
		return spiffe.SVID{}, nil, err
	}
	if svid.ID.TrustDomain.String() != c.cfg.TrustDomain || len(svid.Bundle) == 0 {
		return spiffe.SVID{}, nil, spiffe.ErrInvalidSVID
	}
	ids := c.cfg.enrollment.AllowedControllerIDs
	if len(ids) == 0 {
		ids = []string{svid.ID.String()}
	} else if !slices.Contains(ids, svid.ID.String()) {
		return spiffe.SVID{}, nil, errors.New("mesh controller: serving identity missing from enrollment allowlist")
	}
	return svid, ids, nil
}

func enrollmentWorkload(raw string, svid spiffe.SVID, ids []string) (spiffeid.ID, error) {
	id, err := spiffeid.FromString(raw)
	if err != nil || id.String() != raw || id.TrustDomain().String() != svid.ID.TrustDomain.String() || slices.Contains(ids, raw) {
		return spiffeid.ID{}, model.ErrDenied
	}
	return id, nil
}

// EnrollmentHandler handles POST requests on a server-authenticated HTTPS
// listener. To support renewal, configure TLS to request client certificates
// (tls.RequestClientCert). This handler verifies the renewal peer against mesh
// roots itself, so it must receive the actual TLS handshake.
func (c *Controller) EnrollmentHandler() http.Handler {
	if c.cfg.enrollment == nil {
		return http.NotFoundHandler()
	}
	return http.HandlerFunc(c.serveEnrollment)
}

func (c *Controller) serveEnrollment(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.TLS == nil {
		http.Error(w, "TLS required", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	addrs := c.controllerAddresses()
	if !c.Active() || len(addrs) == 0 {
		http.Error(w, "enrollment unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, bootstrap.MaxRequestSize)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req bootstrap.Request
	if err := dec.Decode(&req); err != nil || dec.Decode(new(any)) != io.EOF || (req.Token == "") == (req.NodeID == "") {
		http.Error(w, "invalid enrollment request", http.StatusBadRequest)
		return
	}
	csr, err := x509.ParseCertificateRequest(req.CSR)
	if err != nil {
		http.Error(w, "invalid certificate request", http.StatusBadRequest)
		return
	}
	key, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || key.Curve != elliptic.P256() {
		http.Error(w, "invalid certificate request key", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	svid, ids, err := c.enrollmentIdentity(ctx)
	if err != nil {
		http.Error(w, "enrollment unavailable", http.StatusServiceUnavailable)
		return
	}
	issue := func(ctx context.Context, raw string, key crypto.PublicKey) ([][]byte, error) {
		id, err := enrollmentWorkload(raw, svid, ids)
		if err != nil {
			return nil, err
		}
		chain, err := c.cfg.enrollment.Issuer.IssueSVID(ctx, id, key)
		if err != nil {
			return nil, err
		}
		_, issuedID, err := bootstrap.VerifyChain(chain, svid.Bundle, key, c.cfg.TrustDomain)
		if err != nil || issuedID != raw {
			return nil, spiffe.ErrInvalidSVID
		}
		return chain, nil
	}
	var node model.Node
	var chain [][]byte
	if req.Token != "" {
		node, chain, err = c.registerEnrollment(ctx, req, csr, issue)
	} else {
		node, chain, err = c.renewEnrollment(r, req.NodeID, csr, svid.Bundle, issue)
	}
	if err != nil {
		http.Error(w, "enrollment failed", enrollmentHTTPStatus(err))
		return
	}
	// A committed result remains bound to its original workload and cell.
	if _, err := enrollmentWorkload(node.SpiffeID, svid, ids); err != nil || node.ManagedLabels[c.enrollmentCellLabel()] != c.cfg.Region {
		http.Error(w, "enrollment failed", http.StatusForbidden)
		return
	}
	if !c.Active() {
		http.Error(w, "enrollment unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(bootstrap.Response{
		NodeID: node.ID, Tenant: node.Tenant, TrustDomain: c.cfg.TrustDomain,
		Controllers: addrs, AllowedControllerIDs: ids,
		Chain: chain, Bundle: svid.Bundle,
	})
}

func (c *Controller) registerEnrollment(ctx context.Context, req bootstrap.Request, csr *x509.CertificateRequest,
	issue func(context.Context, string, crypto.PublicKey) ([][]byte, error),
) (model.Node, [][]byte, error) {
	return c.tokens.RegisterManaged(ctx, req.Token, c.cfg.Region, csr, func(tok enrollment.Token, identity enrollment.ManagedIdentity) (model.Node, error) {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return model.Node{}, err
		}
		now := time.Now().UTC()
		capabilities := req.Capabilities
		if !slices.Contains(capabilities, model.PlanCapability) {
			capabilities = append(capabilities, model.PlanCapability)
		}
		node := model.Node{
			ID: "node-" + hex.EncodeToString(id[:]), Name: identity.NodeName, Tenant: tok.Tenant,
			SpiffeID: identity.WorkloadID, Intent: model.IntentActive,
			Status: model.NodeStatus{JoinedAt: now, LastContact: now, ReportedCapabilities: capabilities,
				ReportedLabels: req.Labels, AgentVersion: req.AgentVersion, Runtime: req.Runtime},
		}
		if c.cfg.Region != "" {
			node.ManagedLabels = map[string]string{c.enrollmentCellLabel(): c.cfg.Region}
		}
		return node, nil
	}, issue)
}

func (c *Controller) renewEnrollment(r *http.Request, nodeID string, csr *x509.CertificateRequest, bundle [][]byte,
	issue func(context.Context, string, crypto.PublicKey) ([][]byte, error),
) (model.Node, [][]byte, error) {
	peers := r.TLS.PeerCertificates
	if !model.ExactName(nodeID) || len(peers) == 0 || csr.CheckSignature() != nil {
		return model.Node{}, nil, model.ErrDenied
	}
	chain := make([][]byte, len(peers))
	for i, peer := range peers {
		chain[i] = peer.Raw
	}
	leaf, id, err := bootstrap.VerifyChain(chain, bundle, csr.PublicKey, c.cfg.TrustDomain)
	if err != nil {
		return model.Node{}, nil, model.ErrDenied
	}
	ctx := r.Context()
	obj, err := c.objects.GetObject(ctx, model.NodeKey(nodeID))
	if err != nil {
		return model.Node{}, nil, err
	}
	var node model.Node
	if err := json.Unmarshal(obj.Data, &node); err != nil {
		return model.Node{}, nil, err
	}
	if node.ID != nodeID || node.Intent != model.IntentActive || node.SpiffeID != id ||
		node.EnrollmentKey != bootstrap.KeyID(leaf.RawSubjectPublicKeyInfo) ||
		node.ManagedLabels[c.enrollmentCellLabel()] != c.cfg.Region {
		return model.Node{}, nil, model.ErrDenied
	}
	chain, err = issue(ctx, node.SpiffeID, csr.PublicKey)
	if err != nil {
		return model.Node{}, nil, err
	}
	latest, err := c.objects.GetObject(ctx, obj.Key)
	if err != nil {
		return model.Node{}, nil, err
	}
	var current model.Node
	if err := json.Unmarshal(latest.Data, &current); err != nil {
		return model.Node{}, nil, err
	}
	if current.ID != node.ID || current.Intent != model.IntentActive || current.SpiffeID != node.SpiffeID ||
		current.Tenant != node.Tenant || current.EnrollmentKey != node.EnrollmentKey ||
		current.ManagedLabels[c.enrollmentCellLabel()] != c.cfg.Region {
		return model.Node{}, nil, model.ErrDenied
	}
	return current, chain, nil
}

func (c *Controller) enrollmentCellLabel() string {
	if c.cfg.CellLabel == "" {
		return model.CellLabel
	}
	return c.cfg.CellLabel
}

func enrollmentHTTPStatus(err error) int {
	switch {
	case errors.Is(err, model.ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, model.ErrDenied), errors.Is(err, store.ErrNotFound),
		errors.Is(err, enrollment.ErrInvalidToken), errors.Is(err, enrollment.ErrRevokedToken),
		errors.Is(err, enrollment.ErrExpiredToken), errors.Is(err, enrollment.ErrExhaustedToken):
		return http.StatusForbidden
	default:
		return http.StatusServiceUnavailable
	}
}
