package node

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/state"
	"google.golang.org/grpc"
)

type Config struct {
	Identity                                 spiffe.Source
	Controllers, AllowedControllerIDs        []string
	TrustDomain, Tenant, JoinToken, StateDir string
	// ListenAddress is the local bind IP and port; port zero requests an
	// ephemeral port. Wildcard binds require a concrete AdvertiseAddress.
	// AdvertiseAddress port zero reuses the actual listener port.
	ListenAddress    string
	AdvertiseAddress string
	ConnectTimeout   time.Duration
	// DrainTimeout bounds listener/connection draining. Close also waits for
	// control and apply work; a blocking hook can outlive this timeout.
	DrainTimeout time.Duration
	// DialTimeout bounds the whole Dial; ConnectTimeout bounds one attempt.
	DialTimeout time.Duration
}

// Node embeds control-plane synchronization and local service communication.
// One state directory belongs to one workload/node process at a time.
type Node struct {
	cfg               Config
	state             *state.Store
	runtime           *dataplane.Runtime
	pipeline          *apply.Pipeline
	composition       *composition
	mu                sync.Mutex
	started, closed   bool
	ctx               context.Context
	cancel            context.CancelFunc
	initDone, runDone chan struct{}
	runErr            error
	nodeID            string
	lastContact       time.Time
	controlState      string
	lastApplyFailure  string
	conns             []*grpc.ClientConn
	bindings          map[ServiceRef]*Listener
	closeOnce         sync.Once
	closeErr          error
}

func New(cfg Config, options ...Option) (*Node, error) {
	if cfg.Identity == nil || len(cfg.Controllers) == 0 || len(cfg.AllowedControllerIDs) == 0 || cfg.StateDir == "" || !model.ExactName(cfg.Tenant) {
		return nil, errors.New("mesh node: identity, controllers, controller allowlist, state directory and tenant required")
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = cfg.ConnectTimeout
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 5 * time.Second
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:0"
	}
	listen, err := netip.ParseAddrPort(cfg.ListenAddress)
	if err != nil {
		return nil, errors.New("mesh node: listen address must be an IP and port")
	}
	if cfg.AdvertiseAddress == "" && (listen.Addr().IsUnspecified() || listen.Addr().IsMulticast()) {
		return nil, errors.New("mesh node: wildcard listener requires a concrete advertise address")
	}
	if cfg.AdvertiseAddress != "" {
		a, err := netip.ParseAddrPort(cfg.AdvertiseAddress)
		if err != nil || a.Addr().IsUnspecified() || a.Addr().IsMulticast() {
			return nil, errors.New("mesh node: advertise address must be a concrete IP and port")
		}
	}
	r := dataplane.New(cfg.Identity, cfg.ConnectTimeout)
	n := &Node{cfg: cfg, runtime: r, initDone: make(chan struct{}), runDone: make(chan struct{}), bindings: map[ServiceRef]*Listener{}}
	r.SetStoredPlanVerifier(n.verifyStoredPlan)
	for _, option := range options {
		if err := option(n); err != nil {
			return nil, err
		}
	}
	return n, nil
}

type Option func(*Node) error

type ServiceRef = model.ServiceRef
