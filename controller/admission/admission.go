package admission

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func peerWorkload(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing mTLS peer")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing peer SVID")
	}
	id, err := x509svid.IDFromCert(info.State.PeerCertificates[0])
	if err != nil {
		return "", status.Error(codes.Unauthenticated, "invalid peer SVID")
	}
	return id.String(), nil
}

type Admission struct {
	Objects         store.ObjectStore
	ContactObjects  store.ObjectStore
	ContactInterval time.Duration
	DevInsecure     bool
	Region          string
	CellLabel       string
}

// AuthorizeSession binds discovery to the enrolled peer, never to a claimed
// Node.Id or tenant. All three discovery entry points use this gate.
func (s *Admission) AuthorizeSession(ctx context.Context, nodeID, requestedTenant string) (model.Node, error) {
	return s.authorizeSession(ctx, nodeID, requestedTenant, false)
}

// AuthorizeReadSession retains identity, tenant and region checks but cannot
// claim an unassigned node. Followers serve only the leader's enrolled region.
func (s *Admission) AuthorizeReadSession(ctx context.Context, nodeID, requestedTenant string) (model.Node, error) {
	return s.authorizeSession(ctx, nodeID, requestedTenant, true)
}

func (s *Admission) authorizeSession(ctx context.Context, nodeID, requestedTenant string, readOnly bool) (model.Node, error) {
	if nodeID == "" {
		return model.Node{}, status.Error(codes.InvalidArgument, "missing node id")
	}
	var node model.Node
	if s.DevInsecure {
		var err error
		node, err = s.readNode(ctx, nodeID)
		if err != nil {
			return node, err
		}
	} else {
		id, err := peerWorkload(ctx)
		if err != nil {
			return node, err
		}
		obj, err := s.Objects.GetObject(ctx, model.NodeKey(nodeID))
		if err != nil {
			return node, status.Error(codes.PermissionDenied, "node is not enrolled")
		}
		if err := json.Unmarshal(obj.Data, &node); err != nil {
			return node, status.Error(codes.Internal, "invalid node registration")
		}
		// SpiffeID is the bootstrap-owned binding, not reported status.
		if node.ID != nodeID || node.SpiffeID == "" || node.SpiffeID != id {
			return node, status.Error(codes.PermissionDenied, "node does not belong to peer")
		}
	}
	if node.Intent == model.IntentDisabled {
		return node, status.Error(codes.PermissionDenied, "node is disabled")
	}
	if node.Tenant == "" {
		node.Tenant = model.DefaultTenant
	}
	if requestedTenant != "" && requestedTenant != node.Tenant {
		return node, status.Error(codes.PermissionDenied, "tenant does not match node registration")
	}
	if readOnly && node.ManagedLabels[s.cellLabel()] != s.Region {
		return node, status.Error(codes.FailedPrecondition, "node is not enrolled in this controller region")
	}
	if !readOnly {
		if err := s.admitRegion(ctx, node); err != nil {
			return node, err
		}
	}
	if node.Ephemeral && (s.ContactObjects != nil || !readOnly) {
		objects := s.ContactObjects
		if objects == nil {
			objects = s.Objects
		}
		interval := s.ContactInterval
		if interval <= 0 {
			interval = time.Second
		}
		now := time.Now().UTC()
		if now.Sub(node.Status.LastContact) >= interval {
			for range 8 {
				obj, err := objects.GetObject(ctx, model.NodeKey(nodeID))
				if err != nil {
					return node, err
				}
				var current model.Node
				if err = json.Unmarshal(obj.Data, &current); err != nil {
					return node, err
				}
				if current.Intent == model.IntentDisabled {
					return node, status.Error(codes.PermissionDenied, "node is disabled")
				}
				reported := current.Status
				reported.LastContact = now
				_, err = store.PutStatusIfVersion(ctx, objects, model.NodeStatusOwner, obj.Key, obj.ResourceVersion, reported)
				if errors.Is(err, store.ErrConflict) {
					continue
				}
				if err != nil {
					return node, err
				}
				break
			}
		}
	}
	return node, nil
}

func (s *Admission) readNode(ctx context.Context, nodeID string) (model.Node, error) {
	obj, err := s.Objects.GetObject(ctx, model.NodeKey(nodeID))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.Node{ID: nodeID, Intent: model.IntentActive}, nil
		}
		return model.Node{}, err
	}
	var node model.Node
	if err := json.Unmarshal(obj.Data, &node); err != nil {
		return model.Node{}, err
	}
	if node.ID == "" {
		node.ID = nodeID
	}
	if node.Intent == "" {
		node.Intent = model.IntentActive
	}
	return node, nil
}

// admitRegion decides whether this cell serves a node, and claims it if nobody
// has.
//
// Two cells serving one node would both build its
// snapshots and both hold its runtime state, and which one it obeyed would come
// down to which stream it happened to have open. The rule is symmetric and has
// no middle: a cell serves the nodes of its own region, and an unregioned
// control plane - the old single cell - serves the nodes nobody has claimed.
//
// A node with no region is adopted rather than refused, and the claim is
// written down. Otherwise splitting one control plane into cells would mean
// labelling every existing node by hand before any of them could be served,
// and an unclaimed node is exactly what a second cell must not find later.
func (s *Admission) admitRegion(ctx context.Context, node model.Node) error {
	nodeRegion := node.ManagedLabels[s.cellLabel()]
	if nodeRegion == s.Region {
		return nil
	}
	if nodeRegion != "" {
		return status.Errorf(codes.FailedPrecondition,
			"node %s belongs to region %q; this control plane is the cell for %q",
			node.ID, nodeRegion, s.Region)
	}
	if s.Region == "" {
		return nil
	}
	return s.claimNode(ctx, node)
}

// claimNode writes this cell's region onto a node that had none.
//
// The write is conditional on the version it read: two cells racing to adopt
// the same unclaimed node must not both believe they won, and the loser has to
// find out here rather than by serving a node another cell is also serving.
func (s *Admission) claimNode(ctx context.Context, node model.Node) error {
	object, err := s.Objects.GetObject(ctx, model.NodeKey(node.ID))
	if err != nil {
		// A node with no object is one the enrolment path has not written yet.
		// Serving it is what the unregioned control plane did before regions
		// existed, and refusing here would strand an agent mid-enrolment.
		return nil
	}
	var current model.Node
	if err := json.Unmarshal(object.Data, &current); err != nil {
		return nil
	}
	if claimed := current.ManagedLabels[s.cellLabel()]; claimed != "" {
		if claimed == s.Region {
			return nil
		}
		return status.Errorf(codes.FailedPrecondition,
			"node %s was claimed by region %q while this cell (%q) was adopting it",
			node.ID, claimed, s.Region)
	}
	if current.ManagedLabels == nil {
		current.ManagedLabels = make(map[string]string, 1)
	}
	current.ManagedLabels[s.cellLabel()] = s.Region
	data, err := json.Marshal(current)
	if err != nil {
		return nil
	}
	updated := store.Object{
		Key: model.NodeKey(node.ID), Data: data,
		ResourceVersion: object.ResourceVersion,
	}
	if _, err := s.Objects.PutObject(ctx, updated); err != nil {
		// Losing the race means another cell claimed it in between. Serving it
		// anyway is the one outcome this function exists to prevent.
		return status.Errorf(codes.FailedPrecondition,
			"node %s could not be claimed for region %q: %v", node.ID, s.Region, err)
	}
	return nil
}

func (s *Admission) cellLabel() string {
	if s.CellLabel != "" {
		return s.CellLabel
	}
	return model.CellLabel
}
