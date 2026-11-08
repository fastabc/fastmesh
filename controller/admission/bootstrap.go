package admission

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Bootstrap struct {
	discoveryv1.UnimplementedBootstrapServer
	// Deprecated: Register uses the atomic store owned by Tokens.
	Objects             store.ObjectStore
	Tokens              *enrollment.Manager
	DevInsecure         bool
	Region, TrustDomain string
	CellLabel           string
	ControllerAddrs     []string
	Now                 func() time.Time
	AuthorizeWrite      func() error
}

// Register implements the Bootstrap service. It validates the presented join
// token, records a Node in the system tenant, and returns the assigned node id
// plus the trust domain and controller addresses the agent should use for the
// steady-state ADS stream.
func (s *Bootstrap) Register(ctx context.Context, req *discoveryv1.RegisterRequest) (*discoveryv1.RegisterResponse, error) {
	if s.AuthorizeWrite != nil {
		if err := s.AuthorizeWrite(); err != nil {
			return nil, err
		}
	}
	if s.Tokens == nil {
		return nil, status.Error(codes.Unavailable, "bootstrap not configured")
	}
	var err error
	var peerID string
	if !s.DevInsecure {
		peerID, err = peerWorkload(ctx)
		if err != nil {
			return nil, err
		}
	}
	id, err := newNodeID()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	label := s.CellLabel
	if label == "" {
		label = model.CellLabel
	}
	node, err := s.Tokens.Register(ctx, req.GetJoinToken(), peerID, req.GetRequestId(), func(tok enrollment.Token) (model.Node, error) {
		node := model.Node{Ephemeral: tok.Ephemeral,
			ID:       id,
			SpiffeID: peerID,
			Name:     req.GetNodeName(),
			Tenant:   tok.Tenant,
			Intent:   model.IntentActive,
		}
		// A node belongs to the cell that enrolled it. Nothing else here knows
		// where it is - the labels it reported are its own claim about itself -
		// but the address it dialed to get here is a control plane in one region,
		// and that is the answer. An unregioned control plane claims nothing.
		if s.Region != "" {
			node.ManagedLabels = map[string]string{label: s.Region}
		}
		node.Status = model.NodeStatus{
			ReportedLabels: req.GetLabels(), ReportedCapabilities: req.GetCapabilities(),
			AgentVersion: req.GetAgentVersion(), Runtime: req.GetRuntime(), JoinedAt: s.now(), LastContact: s.now(),
		}
		return node, nil
	})
	if err != nil {
		code := codes.Internal
		switch {
		case errors.Is(err, enrollment.ErrInvalidToken), errors.Is(err, enrollment.ErrRevokedToken), errors.Is(err, enrollment.ErrExpiredToken), errors.Is(err, enrollment.ErrExhaustedToken), errors.Is(err, model.ErrDenied), errors.Is(err, store.ErrNotFound):
			code = codes.PermissionDenied
		case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrCommitUncertain):
			code = codes.Unavailable
		}
		return nil, status.Error(code, err.Error())
	}
	// A retry on another cell must not redirect the already-enrolled Node to
	// addresses that cannot serve its committed region binding.
	if node.ManagedLabels[label] != s.Region {
		return nil, status.Error(codes.FailedPrecondition, "node is not enrolled in this controller region")
	}
	return &discoveryv1.RegisterResponse{
		NodeId:              node.ID,
		TrustDomain:         s.TrustDomain,
		ControllerAddresses: s.ControllerAddrs,
	}, nil
}

// newNodeID returns a random, collision-resistant node identifier.
func newNodeID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "node-" + hex.EncodeToString(b[:]), nil
}

func (s *Bootstrap) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}
