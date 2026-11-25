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
	node, _, err := m.register(ctx, raw, workload, requestID, nil, func(tok storedToken) (model.Node, error) {
		return build(tok.Token)
	})
	return node, err
}

type managedResult struct {
	NodeID string   `json:"node_id"`
	Chain  [][]byte `json:"chain"`
}

func (m *Manager) register(ctx context.Context, raw, workload, requestID string, managed *managedRegistration, build func(storedToken) (model.Node, error)) (model.Node, [][]byte, error) {
	prefix := tokenPrefix
	if managed != nil {
		prefix = managedTokenPrefix
	}
	id, secret, err := parseRawWithPrefix(raw, prefix)
	if err != nil {
		return model.Node{}, nil, err
	}
	if managed != nil {
		secret = prefix + "." + secret
	}
	if managed == nil && requestID != "" {
		decoded, err := hex.DecodeString(requestID)
		if err != nil || len(decoded) != 16 {
			return model.Node{}, nil, ErrInvalidToken
		}
	}
	tokenKey := store.ObjectKey{Tenant: model.SystemTenant, Kind: KindJoinToken, Name: id}
	var chain [][]byte
	var issuedFor string
	for range 32 {
		if err := ctx.Err(); err != nil {
			return model.Node{}, nil, err
		}
		obj, err := m.objects.GetObject(ctx, tokenKey)
		if err != nil {
			return model.Node{}, nil, err
		}
		tok, err := decodeStored(obj.Data)
		if err != nil {
			return model.Node{}, nil, err
		}
		if err := m.validate(tok, secret, false); err != nil {
			return model.Node{}, nil, err
		}
		if (managed == nil) != (tok.Managed == nil) {
			return model.Node{}, nil, ErrInvalidToken
		}
		if managed != nil {
			if tok.Managed.Region != managed.region {
				return model.Node{}, nil, model.ErrDenied
			}
			workload = tok.Managed.WorkloadID
		}
		binding, _ := json.Marshal([]string{id, workload, requestID})
		sum := sha256.Sum256(binding)
		resultKey := store.ObjectKey{Tenant: model.SystemTenant, Kind: KindRegistration, Name: hex.EncodeToString(sum[:])}
		if requestID != "" {
			result, err := m.objects.GetObject(ctx, resultKey)
			if err == nil {
				var committed managedResult
				var target any = &committed.NodeID
				if managed != nil {
					target = &committed
				}
				if err := json.Unmarshal(result.Data, target); err != nil {
					return model.Node{}, nil, err
				}
				saved, err := m.objects.GetObject(ctx, model.NodeKey(committed.NodeID))
				if err != nil {
					return model.Node{}, nil, err
				}
				var node model.Node
				if err := json.Unmarshal(saved.Data, &node); err != nil {
					return model.Node{}, nil, err
				}
				if node.SpiffeID != workload || node.Tenant != tok.Tenant || node.Intent != model.IntentActive ||
					(managed != nil && node.EnrollmentKey != requestID) {
					return model.Node{}, nil, model.ErrDenied
				}
				return node, committed.Chain, nil
			}
			if !errors.Is(err, store.ErrNotFound) {
				return model.Node{}, nil, err
			}
		}
		if err := m.validate(tok, secret, true); err != nil {
			return model.Node{}, nil, err
		}
		node, err := build(tok)
		if err != nil {
			return model.Node{}, nil, err
		}
		if node.ID == "" || node.Tenant != tok.Tenant || node.SpiffeID != workload {
			return model.Node{}, nil, model.ErrInvalid
		}
		if managed != nil {
			node.EnrollmentKey = requestID
		}
		nodeData, err := json.Marshal(node)
		if err != nil {
			return model.Node{}, nil, err
		}
		tok.Uses++
		obj.Data, err = json.Marshal(tok)
		if err != nil {
			return model.Node{}, nil, err
		}
		changes := []store.ObjectChange{{Object: obj}, {Object: store.Object{Key: model.NodeKey(node.ID), Data: nodeData}}}
		if requestID != "" {
			var result any = node.ID
			if managed != nil {
				if issuedFor != workload {
					chain, err = managed.issue(ctx, workload, managed.key)
					if err != nil {
						return model.Node{}, nil, err
					}
					issuedFor = workload
				}
				// Issuing can outlast the token's validity.
				if err := m.validate(tok, secret, false); err != nil {
					return model.Node{}, nil, err
				}
				result = managedResult{NodeID: node.ID, Chain: chain}
			}
			data, _ := json.Marshal(result)
			changes = append(changes, store.ObjectChange{Object: store.Object{Key: resultKey, Data: data}})
		}
		err = store.ApplyObjects(ctx, m.objects, changes)
		var fenced *store.NotLeaderError
		if errors.Is(err, store.ErrConflict) && !errors.As(err, &fenced) {
			continue
		}
		if err != nil {
			return model.Node{}, nil, err
		}
		return node, chain, nil
	}
	return model.Node{}, nil, store.ErrConflict
}
