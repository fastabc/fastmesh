package election

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/fastabc/fastmesh/store"
)

// DefaultLeaseName is the lease a controller takes leadership on when nothing
// names one. Offline tooling reads it to tell whether a control plane is still
// running against a store it is about to modify.
const DefaultLeaseName = "controller"

// LeaseNameFor is the lease a controller in region takes leadership on.
//
// Each region owns a separate controller lease:
// two regions contending for one lease would elect a single leader across
// them, rather than one independent leader per region.
//
// An empty region keeps DefaultLeaseName, and that is not cosmetic. A
// deployment upgrading into this must not look leaderless to the offline
// tooling that reads the lease, nor have two processes each believe they are
// active while the changeover is half done.
func LeaseNameFor(region string) string {
	region = strings.TrimSpace(region)
	if region == "" {
		return DefaultLeaseName
	}
	return DefaultLeaseName + "/" + region
}

const defaultTTL = 15 * time.Second

// Config controls one elector.
type Config struct {
	LeaseName string
	Holder    string
	Advertise string
	TTL       time.Duration
	Clock     func() time.Time
}

// Elector coordinates active-passive controller leadership.
type Elector struct {
	mu     sync.RWMutex
	leases store.LeaseStore
	cfg    Config
	lease  store.Lease
	active bool
}

// New creates an Elector.
func New(leases store.LeaseStore, cfg Config) *Elector {
	if cfg.LeaseName == "" {
		cfg.LeaseName = DefaultLeaseName
	}
	if cfg.TTL <= 0 {
		cfg.TTL = defaultTTL
	}
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return time.Now().UTC() }
	}
	return &Elector{leases: leases, cfg: cfg}
}

// TryAcquire attempts to become active.
func (e *Elector) TryAcquire(ctx context.Context) (bool, error) {
	lease, ok, err := e.leases.AcquireLease(ctx, store.LeaseRequest{
		Name:       e.cfg.LeaseName,
		Holder:     e.cfg.Holder,
		LeaderHint: e.cfg.Advertise,
		TTL:        e.cfg.TTL,
		Now:        e.cfg.Clock(),
	})
	if err != nil {
		return false, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lease = lease
	e.active = ok
	return ok, nil
}

// Renew extends leadership if this elector still owns the lease token.
func (e *Elector) Renew(ctx context.Context) (bool, error) {
	e.mu.RLock()
	active := e.active
	token := e.lease.Token
	e.mu.RUnlock()
	if !active {
		return false, nil
	}
	lease, ok, err := e.leases.RenewLease(ctx, store.LeaseRequest{
		Name:       e.cfg.LeaseName,
		Holder:     e.cfg.Holder,
		LeaderHint: e.cfg.Advertise,
		Token:      token,
		TTL:        e.cfg.TTL,
		Now:        e.cfg.Clock(),
	})
	if err != nil {
		e.mu.Lock()
		e.active = false
		e.mu.Unlock()
		return false, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lease = lease
	e.active = ok
	return ok, nil
}

// Release gives up leadership when this elector owns the current token.
func (e *Elector) Release(ctx context.Context) error {
	e.mu.RLock()
	active := e.active
	token := e.lease.Token
	e.mu.RUnlock()
	if !active {
		return nil
	}
	err := e.leases.ReleaseLease(ctx, store.LeaseRequest{
		Name:   e.cfg.LeaseName,
		Holder: e.cfg.Holder,
		Token:  token,
	})
	e.mu.Lock()
	e.active = false
	e.mu.Unlock()
	return err
}

// Active reports whether the last acquire or renew succeeded.
func (e *Elector) Active() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.active
}

// Lease returns the last lease observed by this elector.
func (e *Elector) Lease() store.Lease {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lease
}

// Holder returns this elector's configured holder identity.
func (e *Elector) Holder() string { return e.cfg.Holder }

// LeaseName returns this elector's configured lease name.
func (e *Elector) LeaseName() string { return e.cfg.LeaseName }

// SetAdvertise updates the address written on the next acquire or renewal.
func (e *Elector) SetAdvertise(endpoint string) {
	e.mu.Lock()
	e.cfg.Advertise = endpoint
	e.mu.Unlock()
}
