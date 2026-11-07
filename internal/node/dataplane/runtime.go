package dataplane

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/breaker"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

// ServiceRef names one port in the node's tenant-scoped service catalog.
type ServiceRef = model.ServiceRef

type Runtime struct {
	recipient        model.Session
	observer         Observer
	observedHash     string
	identity         spiffe.Source
	timeout          time.Duration
	plan             atomic.Pointer[model.Plan]
	planSource       func() *model.Plan
	verifyStoredPlan func(context.Context, *snapshot.Snapshot) (*model.Plan, error)
	mu               sync.Mutex
	inputPlan        *model.Plan // Last committed signed input; protected by mu.
	closed           bool
	listeners        map[*Listener]struct{}
	active           map[*Conn]struct{}
	circuits         map[endpointKey]*breaker.Breaker
	connections      map[endpointKey]int
	zero             chan struct{}
	changed          chan struct{}
}

func New(identity spiffe.Source, timeout time.Duration) *Runtime {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	zero := make(chan struct{})
	close(zero)
	return &Runtime{identity: identity, timeout: timeout, listeners: map[*Listener]struct{}{}, active: map[*Conn]struct{}{}, circuits: map[endpointKey]*breaker.Breaker{}, connections: map[endpointKey]int{}, zero: zero, changed: make(chan struct{})}
}

func (r *Runtime) addrs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for l := range r.listeners {
		out = append(out, l.Addr().String())
	}
	return out
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	r.signalAvailability()
	listeners := make([]*Listener, 0, len(r.listeners))
	for l := range r.listeners {
		listeners = append(listeners, l)
	}
	zero := r.zero
	r.mu.Unlock()
	for _, l := range listeners {
		_ = l.Close()
	}
	select {
	case <-zero:
		return nil
	case <-ctx.Done():
		r.mu.Lock()
		connections := make([]*Conn, 0, len(r.active))
		for c := range r.active {
			connections = append(connections, c)
		}
		r.mu.Unlock()
		for _, c := range connections {
			_ = c.Close()
		}
		return ctx.Err()
	}
}

// Plan source and verification callbacks are configured before the runtime is
// started. Published plans and connection state remain owned by the runtime.
func (r *Runtime) SetPlanSource(source func() *model.Plan) { r.planSource = source }

func (r *Runtime) SetStoredPlanVerifier(verify func(context.Context, *snapshot.Snapshot) (*model.Plan, error)) {
	r.verifyStoredPlan = verify
}

func (r *Runtime) SetRecipient(recipient model.Session) {
	r.mu.Lock()
	r.recipient = recipient
	r.mu.Unlock()
}

func (r *Runtime) Recipient() model.Session { r.mu.Lock(); defer r.mu.Unlock(); return r.recipient }

func (r *Runtime) SetObserver(observer Observer) { r.mu.Lock(); r.observer = observer; r.mu.Unlock() }

func (r *Runtime) Observer() Observer { r.mu.Lock(); defer r.mu.Unlock(); return r.observer }

func (r *Runtime) CommitInputPlan(p *model.Plan) { r.mu.Lock(); r.inputPlan = p; r.mu.Unlock() }

func (r *Runtime) ActiveConnections() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.active) }
