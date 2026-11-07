package enrollment

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
)

const (
	KindJoinToken = "JoinToken"
	tokenPrefix   = "fgjt"
)

var (
	ErrInvalidToken   = errors.New("join token: invalid")
	ErrRevokedToken   = errors.New("join token: revoked")
	ErrExhaustedToken = errors.New("join token: exhausted")
	ErrExpiredToken   = errors.New("join token: expired")
)

// Manager persists token metadata and validates presented tokens.
type Manager struct {
	objects store.ObjectStore
	rand    io.Reader
	clock   func() time.Time
}

// Option customizes Manager.
type Option func(*Manager)

// WithClock sets the time source.
func WithClock(clock func() time.Time) Option {
	return func(m *Manager) {
		if clock != nil {
			m.clock = clock
		}
	}
}

// New returns a token manager.
func New(objects store.ObjectStore, opts ...Option) *Manager {
	m := &Manager{
		objects: objects,
		rand:    rand.Reader,
		clock:   func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Token is token metadata. Secret material is never stored here.
type Token struct {
	MaxUses     uint64            `json:"max_uses,omitempty"`
	Uses        uint64            `json:"uses,omitempty"`
	Ephemeral   bool              `json:"ephemeral,omitempty"`
	ID          string            `json:"id"`
	Tenant      string            `json:"tenant"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	ExpiresAt   time.Time         `json:"expires_at,omitempty"`
	RevokedAt   time.Time         `json:"revoked_at,omitempty"`
}

type storedToken struct {
	Token
	SecretHash string `json:"secret_hash"`
}

// CreateRequest describes one token creation.
type CreateRequest struct {
	Tenant      string
	Description string
	Labels      map[string]string
	TTL         time.Duration
	MaxUses     uint64
	Ephemeral   bool
}

// CreateResponse returns metadata and the one-time raw token.
type CreateResponse struct {
	Token Token
	Raw   string
}

// Create creates and stores a new join token.
func (m *Manager) Create(ctx context.Context, req CreateRequest) (CreateResponse, error) {
	now := m.clock().UTC()
	id, err := randomPart(m.rand, 16)
	if err != nil {
		return CreateResponse{}, err
	}
	secret, err := randomPart(m.rand, 32)
	if err != nil {
		return CreateResponse{}, err
	}
	t := Token{
		ID:      id,
		MaxUses: req.MaxUses, Ephemeral: req.Ephemeral,
		Tenant:      req.Tenant,
		Description: req.Description,
		Labels:      cloneMap(req.Labels),
		CreatedAt:   now,
	}
	if t.Tenant == "" {
		t.Tenant = model.DefaultTenant
	}
	if req.TTL > 0 {
		t.ExpiresAt = now.Add(req.TTL)
	}
	stored := storedToken{Token: t, SecretHash: hashSecret(secret)}
	data, err := json.Marshal(stored)
	if err != nil {
		return CreateResponse{}, err
	}
	if _, err := m.objects.PutObject(ctx, store.Object{
		Key: store.ObjectKey{
			Tenant: model.SystemTenant,
			Kind:   KindJoinToken,
			Name:   id,
		},
		Data: data,
	}); err != nil {
		return CreateResponse{}, err
	}
	return CreateResponse{Token: t, Raw: tokenPrefix + "." + id + "." + secret}, nil
}

// Get returns token metadata by ID.
func (m *Manager) Get(ctx context.Context, id string) (Token, error) {
	stored, err := m.getStored(ctx, id)
	if err != nil {
		return Token{}, err
	}
	return stored.Token, nil
}

// List returns all token metadata sorted by creation time then ID.
func (m *Manager) List(ctx context.Context) ([]Token, error) {
	objects, err := m.objects.ListObjects(ctx, model.SystemTenant, KindJoinToken)
	if err != nil {
		return nil, err
	}
	out := make([]Token, 0, len(objects))
	for _, obj := range objects {
		stored, err := decodeStored(obj.Data)
		if err != nil {
			return nil, err
		}
		out = append(out, stored.Token)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (m *Manager) Revoke(ctx context.Context, id string) error {
	unchanged := errors.New("token already revoked")
	_, err := store.UpdateJSON(ctx, m.objects, store.ObjectKey{Tenant: model.SystemTenant, Kind: KindJoinToken, Name: id}, 32, func(stored *storedToken, exists bool) error {
		if !exists {
			return store.ErrNotFound
		}
		if !stored.RevokedAt.IsZero() {
			return unchanged
		}
		stored.RevokedAt = m.clock().UTC()
		return nil
	})
	if errors.Is(err, unchanged) {
		return nil
	}
	return err
}

// Validate verifies a raw join token and returns its metadata.
func (m *Manager) Validate(ctx context.Context, raw string) (Token, error) {
	id, secret, err := parseRaw(raw)
	if err != nil {
		return Token{}, err
	}
	stored, err := m.getStored(ctx, id)
	if err != nil {
		return Token{}, err
	}
	if stored.SecretHash != hashSecret(secret) {
		return Token{}, ErrInvalidToken
	}
	if !stored.RevokedAt.IsZero() {
		return Token{}, ErrRevokedToken
	}
	if !stored.ExpiresAt.IsZero() && !m.clock().Before(stored.ExpiresAt) {
		return Token{}, ErrExpiredToken
	}
	if stored.MaxUses > 0 && stored.Uses >= stored.MaxUses {
		return Token{}, ErrExhaustedToken
	}
	return stored.Token, nil
}

func (m *Manager) getStored(ctx context.Context, id string) (storedToken, error) {
	obj, err := m.objects.GetObject(ctx, store.ObjectKey{
		Tenant: model.SystemTenant,
		Kind:   KindJoinToken,
		Name:   id,
	})
	if err != nil {
		return storedToken{}, err
	}
	return decodeStored(obj.Data)
}

func decodeStored(data []byte) (storedToken, error) {
	var stored storedToken
	if err := json.Unmarshal(data, &stored); err != nil {
		return storedToken{}, err
	}
	return stored, nil
}

func parseRaw(raw string) (id, secret string, err error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || parts[0] != tokenPrefix || parts[1] == "" || parts[2] == "" {
		return "", "", ErrInvalidToken
	}
	return parts[1], parts[2], nil
}

func randomPart(r io.Reader, n int) (string, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func hashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (m *Manager) Consume(ctx context.Context, raw string) (Token, error) {
	id, secret, err := parseRaw(raw)
	if err != nil {
		return Token{}, err
	}
	var consumed Token
	_, err = store.UpdateJSON(ctx, m.objects, store.ObjectKey{Tenant: model.SystemTenant, Kind: KindJoinToken, Name: id}, 32, func(stored *storedToken, exists bool) error {
		if !exists {
			return store.ErrNotFound
		}
		if stored.SecretHash != hashSecret(secret) {
			return ErrInvalidToken
		}
		if !stored.RevokedAt.IsZero() {
			return ErrRevokedToken
		}
		if !stored.ExpiresAt.IsZero() && !m.clock().Before(stored.ExpiresAt) {
			return ErrExpiredToken
		}
		if stored.MaxUses > 0 && stored.Uses >= stored.MaxUses {
			return ErrExhaustedToken
		}
		stored.Uses++
		consumed = stored.Token
		return nil
	})
	if err != nil {
		return Token{}, err
	}
	return consumed, nil
}
