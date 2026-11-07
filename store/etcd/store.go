package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/fastabc/fastmesh/store"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultPrefix = "/fastmesh/v1"

// Config controls etcd store construction.
type Config struct {
	Endpoints   []string
	Prefix      string
	DialTimeout time.Duration
	CAFile      string
	CertFile    string
	KeyFile     string
	Username    string
	Password    string
}

// Store persists control-plane objects and leases in etcd.
type Store struct {
	client *clientv3.Client
	prefix string
}

type fencedObjectStore struct {
	*Store
	source store.FenceTokenSource
}

var (
	_ store.ObjectStore              = (*Store)(nil)
	_ store.AtomicObjectCreator      = (*Store)(nil)
	_ store.ConditionalObjectDeleter = (*Store)(nil)
	_ store.BulkObjectDeleter        = (*Store)(nil)
	_ store.WatchableStore           = (*Store)(nil)
	_ store.LeaseStore               = (*Store)(nil)
)

type storedObject struct {
	UpdatedAt time.Time         `json:"updatedAt"`
	Labels    map[string]string `json:"labels,omitempty"`
	Data      []byte            `json:"data,omitempty"`
}

type storedLease struct {
	Name       string    `json:"name"`
	Holder     string    `json:"holder"`
	LeaderHint string    `json:"leaderHint,omitempty"`
	Token      int64     `json:"token"`
	ExpiresAt  time.Time `json:"expiresAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// Open creates an etcd client-backed store. Call Close when the process owns
// the returned client lifetime.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("etcd store: missing endpoints")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	tlsConfig, err := loadTLSConfig(cfg.CAFile, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
		TLS:         tlsConfig,
		Username:    cfg.Username,
		Password:    cfg.Password,
	})
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		_ = client.Close()
		return nil, ctx.Err()
	default:
	}
	st := New(client, cfg.Prefix)
	return st, nil
}

func loadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	if caFile == "" {
		return nil, errors.New("etcd store: CA file is required when client TLS is configured")
	}
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("etcd store: client certificate and key must be set together")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("etcd store: read CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("etcd store: CA file contains no certificates")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("etcd store: load client key pair: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

// New wraps an existing etcd client. The caller owns the client lifetime.
func New(client *clientv3.Client, prefix string) *Store {
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Store{client: client, prefix: strings.TrimRight(prefix, "/")}
}

// Client exposes the borrowed connection for additional host-owned key spaces.
// The caller that opened the client retains responsibility for closing it.
func (s *Store) Client() *clientv3.Client { return s.client }

// Prefix is where this store keeps its objects. The accounting plane derives
// its own prefix from it so a deployment that moved its objects does not leave
// its balances behind under the default path.
func (s *Store) Prefix() string { return s.prefix }

// Close closes the underlying etcd client.
func (s *Store) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

// FencedObjectStore returns an etcd-native fenced writer. Put/Delete compare
// the controller lease id and object CAS in the same etcd transaction.
func (s *Store) FencedObjectStore(_ store.LeaseStore, source store.FenceTokenSource) store.ObjectStore {
	return fencedObjectStore{Store: s, source: source}
}

// txnOpLimit is how many deletes go into one transaction. etcd's server-side
// --max-txn-ops defaults to 128 and rejects the whole transaction above it, so
// a prune large enough to matter has to be split.
const txnOpLimit = 128

func (s *Store) getLeaseWithRevision(ctx context.Context, key string) (store.Lease, int64, error) {
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return store.Lease{}, 0, err
	}
	if len(resp.Kvs) == 0 {
		return store.Lease{}, 0, store.ErrNotFound
	}
	lease, err := decodeLeaseValue(resp.Kvs[0].Value)
	if err != nil {
		return store.Lease{}, 0, err
	}
	return lease, resp.Kvs[0].ModRevision, nil
}

func (s fencedObjectStore) fenceToken(ctx context.Context) (store.FenceToken, error) {
	if s.source == nil {
		return store.FenceToken{}, store.ErrConflict
	}
	token := s.source()
	if token.Name == "" || token.Holder == "" || token.Token == 0 {
		if token.Name != "" {
			if lease, err := s.GetLease(ctx, token.Name); err == nil {
				return store.FenceToken{}, &store.NotLeaderError{Hint: lease.LeaderHint}
			}
		}
		return store.FenceToken{}, store.ErrConflict
	}
	return token, nil
}

func (s fencedObjectStore) conflictError(ctx context.Context, token store.FenceToken) error {
	lease, err := s.GetLease(ctx, token.Name)
	if err == nil && (lease.Holder != token.Holder || lease.Token != token.Token) {
		return &store.NotLeaderError{Hint: lease.LeaderHint}
	}
	return store.ErrConflict
}

func decodeLeaseValue(value []byte) (store.Lease, error) {
	var stored storedLease
	if err := json.Unmarshal(value, &stored); err != nil {
		return store.Lease{}, err
	}
	return store.Lease(stored), nil
}

func decodeObjectKV(kv *mvccpb.KeyValue) (store.Object, error) {
	key, err := parseObjectKey(string(kv.Key))
	if err != nil {
		return store.Object{}, err
	}
	var stored storedObject
	if err := json.Unmarshal(kv.Value, &stored); err != nil {
		return store.Object{}, err
	}
	return store.Object{
		Key:             key,
		ResourceVersion: kv.ModRevision,
		UpdatedAt:       stored.UpdatedAt,
		Data:            append([]byte(nil), stored.Data...),
		Labels:          cloneMap(stored.Labels),
	}, nil
}

func mapWatchEvent(ev *clientv3.Event) (store.Event, error) {
	key, err := parseObjectKey(string(ev.Kv.Key))
	if err != nil {
		return store.Event{}, err
	}
	switch ev.Type {
	case mvccpb.PUT:
		obj, err := decodeObjectKV(ev.Kv)
		if err != nil {
			return store.Event{}, err
		}
		return store.Event{Type: store.EventPut, Object: obj}, nil
	case mvccpb.DELETE:
		return store.Event{
			Type: store.EventDelete,
			Object: store.Object{
				Key:             key,
				ResourceVersion: ev.Kv.ModRevision,
				UpdatedAt:       time.Now().UTC(),
			},
		}, nil
	default:
		return store.Event{}, fmt.Errorf("etcd store: unknown watch event type %v", ev.Type)
	}
}

func (s *Store) objectKey(key store.ObjectKey) string {
	return path.Join(s.prefix, "objects", escape(key.Tenant), escape(key.Kind), escape(key.Name))
}

func (s *Store) objectListPrefix(tenantName, kind string) string {
	tenantName = defaultTenantFilter(tenantName)
	switch {
	case tenantName != "" && kind != "":
		return path.Join(s.prefix, "objects", escape(tenantName), escape(kind)) + "/"
	case tenantName != "":
		return path.Join(s.prefix, "objects", escape(tenantName)) + "/"
	default:
		return path.Join(s.prefix, "objects") + "/"
	}
}

func (s *Store) leaseKey(name string) string {
	return path.Join(s.prefix, "leases", escape(name))
}

func parseObjectKey(raw string) (store.ObjectKey, error) {
	parts := strings.Split(strings.Trim(raw, "/"), "/")
	if len(parts) < 5 {
		return store.ObjectKey{}, fmt.Errorf("etcd store: invalid object key %q", raw)
	}
	return store.ObjectKey{
		Tenant: unescape(parts[len(parts)-3]),
		Kind:   unescape(parts[len(parts)-2]),
		Name:   unescape(parts[len(parts)-1]),
	}, nil
}

func objectMatches(obj store.Object, tenantName string, kinds store.KindFilter) bool {
	if tenantName != "" && obj.Key.Tenant != tenantName {
		return false
	}
	return kinds.Matches(obj.Key.Kind)
}

func normalizeObjectKey(key store.ObjectKey) store.ObjectKey {
	key.Tenant = defaultTenant(key.Tenant)
	return key
}

func defaultTenant(name string) string {
	if name == "" {
		return store.DefaultTenant
	}
	return name
}

func defaultTenantFilter(name string) string {
	if name == "" {
		return ""
	}
	return defaultTenant(name)
}

func ttlSeconds(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 1
	}
	return int64(math.Ceil(ttl.Seconds()))
}

func escape(value string) string {
	return url.PathEscape(value)
}

func unescape(value string) string {
	out, err := url.PathUnescape(value)
	if err != nil {
		return value
	}
	return out
}

func cloneObject(obj store.Object) store.Object {
	obj.Data = append([]byte(nil), obj.Data...)
	obj.Labels = cloneMap(obj.Labels)
	return obj
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
