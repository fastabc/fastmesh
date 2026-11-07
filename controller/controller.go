package controller

import (
	"context"
	"errors"
	"time"

	"github.com/fastabc/fastmesh/controller/admission"
	"github.com/fastabc/fastmesh/controller/discovery"
	"github.com/fastabc/fastmesh/controller/enrollment"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/internal/controller/index"
	"github.com/fastabc/fastmesh/internal/controller/plancache"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/election"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Observer augments ADS metrics with compilation, signing and authenticated
// ACK watermarks. Callbacks must be concurrency safe and return promptly.
type Observer interface {
	DiscoveryStreamOpened() func()
	RecordDiscoveryPush(mode string, err error, elapsed time.Duration)
	RecordDiscoveryAck(ok bool)
	RecordPlanCompile(time.Duration, error)
	RecordPlanSignature(time.Duration, error)
	RecordPlanApplied(nodeID string, inputRevision int64)
}

type Config struct {
	Cache                                                  CacheConfig
	CellLabel                                              string
	Observer                                               Observer
	ID, Region, TrustDomain                                string
	EphemeralTTL                                           time.Duration
	Objects                                                store.ObjectStore
	Leases                                                 store.LeaseStore
	Identity                                               spiffe.Source
	DevInsecure                                            bool
	ControllerAddrs                                        []string
	LeaseTTL, EndpointTTL, PlanValidity, ReconcileInterval time.Duration
	discovery                                              discovery.Config
	// An injected elector belongs to the embedding product's lifecycle.
	Elector        *election.Elector
	grpcRegistrars []func(*grpc.Server)
}

type Option func(*Config)

type compileObserver interface{ RecordPlanCompile(time.Duration, error) }
type signatureObserver interface{ RecordPlanSignature(time.Duration, error) }

type Controller struct {
	compileObserver   compileObserver
	signatureObserver signatureObserver
	cfg               Config
	objects           store.ObjectStore
	elector           *election.Elector
	ownsElection      bool
	admission         *admission.Admission
	tokens            *enrollment.Manager
	endpoints         *endpoint.Manager
	index             *index.Index
	plans             plancache.Cache
}

func New(cfg Config, opts ...Option) (*Controller, error) {
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Observer != nil {
		cfg.discovery.Metrics = cfg.Observer
	}
	if cfg.Objects == nil || cfg.Leases == nil {
		return nil, errors.New("mesh controller: object and lease stores required")
	}
	viewer, ok := cfg.Objects.(store.Viewer)
	if !ok {
		return nil, store.ErrViewUnsupported
	}
	watchable, ok := cfg.Objects.(store.RevisionWatcher)
	if !ok {
		return nil, store.ErrWatchUnsupported
	}
	if !cfg.DevInsecure && cfg.Identity == nil {
		return nil, errors.New("mesh controller: identity required")
	}
	if cfg.EphemeralTTL <= 0 {
		cfg.EphemeralTTL = 5 * time.Minute
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 15 * time.Second
	}
	if cfg.EndpointTTL <= 0 {
		cfg.EndpointTTL = 30 * time.Second
	}
	if cfg.PlanValidity <= 0 {
		cfg.PlanValidity = time.Minute
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = time.Second
	}
	if cfg.LeaseTTL/3 <= 0 || cfg.EndpointTTL/3 <= 0 || cfg.EphemeralTTL/3 <= 0 || cfg.PlanValidity/3 <= 0 {
		return nil, errors.New("mesh controller: lease, endpoint, ephemeral and plan durations must be at least 3ns")
	}
	if cfg.discovery.HeartbeatInterval <= 0 || cfg.discovery.HeartbeatInterval > min(cfg.PlanValidity/3, cfg.EndpointTTL/3, cfg.EphemeralTTL/3) {
		cfg.discovery.HeartbeatInterval = min(cfg.PlanValidity/3, cfg.EndpointTTL/3, cfg.EphemeralTTL/3)
	}
	if cfg.discovery.DefaultTypeURL == "" {
		cfg.discovery.DefaultTypeURL = model.PlanTypeURL
	}
	if cfg.Cache.MaxEntries < 0 || cfg.Cache.MaxPending < 0 || cfg.Cache.Parallelism < 0 {
		return nil, errors.New("mesh controller: cache limits must be nonnegative")
	}
	c := &Controller{cfg: cfg, elector: cfg.Elector, ownsElection: cfg.Elector == nil}
	c.plans.MaxEntries, c.plans.MaxPending, c.plans.Parallelism = cfg.Cache.MaxEntries, cfg.Cache.MaxPending, cfg.Cache.Parallelism
	c.compileObserver, _ = cfg.discovery.Metrics.(compileObserver)
	c.signatureObserver, _ = cfg.discovery.Metrics.(signatureObserver)
	if c.elector == nil {
		if cfg.ID == "" {
			return nil, errors.New("mesh controller: controller ID required")
		}
		c.elector = election.New(cfg.Leases, election.Config{Holder: cfg.ID, LeaseName: election.LeaseNameFor(cfg.Region), TTL: cfg.LeaseTTL})
	}
	c.objects = store.FencedFunc(cfg.Objects, cfg.Leases, func() store.FenceToken {
		return store.FenceToken{Name: c.elector.LeaseName(), Holder: c.elector.Holder(), Token: c.elector.Lease().Token}
	})
	c.admission = &admission.Admission{Objects: c.objects, DevInsecure: cfg.DevInsecure, Region: cfg.Region, CellLabel: cfg.CellLabel, ContactObjects: cfg.Objects, ContactInterval: min(time.Second, cfg.EphemeralTTL/3)}
	c.tokens = enrollment.New(c.objects)
	c.index = index.New(watchable, viewer)
	c.index.Expiry.NodeTTL = cfg.EphemeralTTL
	c.endpoints = &endpoint.Manager{Objects: c.objects, Viewer: viewer, TTL: cfg.EndpointTTL, CanBind: func(ctx context.Context, tenant, service, workload string) (bool, error) {
		s, err := c.index.Snapshot(ctx, tenant)
		if err != nil {
			return false, err
		}
		return s.CanBind(service, workload), nil
	}}
	return c, nil
}

func (c *Controller) Active() bool { return c.elector.Active() }

func (c *Controller) requireLeader() error {
	if !c.Active() {
		message := "controller is not leader"
		if hint := c.elector.Lease().LeaderHint; hint != "" {
			message += ": leader is " + hint
		}
		return status.Error(codes.Unavailable, message)
	}
	return nil
}
