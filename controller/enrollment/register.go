package enrollment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

const KindRegistration = "NodeRegistration"

// Register consumes a token and creates the complete Node in one transaction.
// build is trusted controller code and may be called again after a CAS conflict.
// A request ID binds retries to the token and authenticated workload. Replays
// require a still-valid token and an existing, active Node; they never recreate
// deleted Nodes or change registration metadata. An empty ID preserves legacy
// registration without response-loss idempotency.
func (m *Manager) Register(ctx context.Context, raw, workload, requestID string, build func(Token) (model.Node, error)) (model.Node, error) {
	id, secret, err := parseRaw(raw)
	if err != nil {
		return model.Node{}, err
	}
	if requestID != "" {
		decoded, err := hex.DecodeString(requestID)
		if err != nil || len(decoded) != 16 {
			return model.Node{}, ErrInvalidToken
		}
	}
	tokenKey := store.ObjectKey{Tenant: model.SystemTenant, Kind: KindJoinToken, Name: id}
	binding, _ := json.Marshal([]string{id, workload, requestID})
	sum := sha256.Sum256(binding)
	resultKey := store.ObjectKey{Tenant: model.SystemTenant, Kind: KindRegistration, Name: hex.EncodeToString(sum[:])}
	for range 32 {
		if err := ctx.Err(); err != nil {
			return model.Node{}, err
		}
		obj, err := m.objects.GetObject(ctx, tokenKey)
		if err != nil {
			return model.Node{}, err
		}
		tok, err := decodeStored(obj.Data)
		if err != nil {
			return model.Node{}, err
		}
		if err := m.validate(tok, secret, false); err != nil {
			return model.Node{}, err
		}
		if requestID != "" {
			result, err := m.objects.GetObject(ctx, resultKey)
			if err == nil {
				var nodeID string
				if err := json.Unmarshal(result.Data, &nodeID); err != nil {
					return model.Node{}, err
				}
				saved, err := m.objects.GetObject(ctx, model.NodeKey(nodeID))
				if err != nil {
					return model.Node{}, err
				}
				var node model.Node
				if err := json.Unmarshal(saved.Data, &node); err != nil {
					return model.Node{}, err
				}
				if node.SpiffeID != workload || node.Tenant != tok.Tenant || node.Intent != model.IntentActive {
					return model.Node{}, model.ErrDenied
				}
				return node, nil
			}
			if !errors.Is(err, store.ErrNotFound) {
				return model.Node{}, err
			}
		}
		if err := m.validate(tok, secret, true); err != nil {
			return model.Node{}, err
		}
		node, err := build(tok.Token)
		if err != nil {
			return model.Node{}, err
		}
		if node.ID == "" || node.Tenant != tok.Tenant || node.SpiffeID != workload {
			return model.Node{}, model.ErrInvalid
		}
		nodeData, err := json.Marshal(node)
		if err != nil {
			return model.Node{}, err
		}
		tok.Uses++
		obj.Data, err = json.Marshal(tok)
		if err != nil {
			return model.Node{}, err
		}
		changes := []store.ObjectChange{{Object: obj}, {Object: store.Object{Key: model.NodeKey(node.ID), Data: nodeData}}}
		if requestID != "" {
			data, _ := json.Marshal(node.ID)
			changes = append(changes, store.ObjectChange{Object: store.Object{Key: resultKey, Data: data}})
		}
		err = store.ApplyObjects(ctx, m.objects, changes)
		var fenced *store.NotLeaderError
		if errors.Is(err, store.ErrConflict) && !errors.As(err, &fenced) {
			continue
		}
		if err != nil {
			return model.Node{}, err
		}
		return node, nil
	}
	return model.Node{}, store.ErrConflict
}
