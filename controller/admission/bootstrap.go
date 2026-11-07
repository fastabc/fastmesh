package admission

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	tok, err := s.Tokens.Validate(ctx, req.GetJoinToken())
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
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
	tok, err = s.Tokens.Consume(ctx, req.GetJoinToken())
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
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
		label := s.CellLabel
		if label == "" {
			label = model.CellLabel
		}
		node.ManagedLabels = map[string]string{label: s.Region}
	}
	data, err := json.Marshal(node)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if _, err := s.Objects.PutObject(ctx, store.Object{Key: model.NodeKey(id), Data: data}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// What the agent reported about itself is status, and status is stripped
	// from any write that does not carry a status owner. Writing it as part of
	// the object above loses it silently, which leaves the node with no
	// capabilities required for policy delivery.
	reported := model.NodeStatus{
		ReportedLabels: req.GetLabels(), ReportedCapabilities: req.GetCapabilities(),
		AgentVersion: req.GetAgentVersion(), Runtime: req.GetRuntime(), JoinedAt: s.now(), LastContact: s.now(),
	}
	if _, err := store.PutStatus(ctx, s.Objects, model.NodeStatusOwner, model.NodeKey(id), reported); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &discoveryv1.RegisterResponse{
		NodeId:              id,
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
