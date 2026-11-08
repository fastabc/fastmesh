package node

import (
	"context"
	"errors"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/node/control"
	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/state"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
)

// Start waits for a verified usable plan, including an unexpired last-good
// plan. Controllers coordinate policy; they never relay application traffic.
func (n *Node) Start(ctx context.Context) (err error) {
	n.mu.Lock()
	if n.closed || n.started {
		n.mu.Unlock()
		return errors.New("mesh node: already started or closed")
	}
	if n.composition == nil {
		st, openErr := state.Open(n.cfg.StateDir)
		if openErr != nil {
			n.mu.Unlock()
			return openErr
		}
		n.state = st
		n.pipeline = apply.NewPipeline(apply.NewHolder(), st)
		n.pipeline.AddCommitHook(n.runtime)
	}
	n.started = true
	n.controlState = "connecting"
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.mu.Unlock()
	launched := false
	defer func() {
		if !launched {
			close(n.runDone)
		}
		close(n.initDone)
		if err != nil {
			_ = n.Close()
		}
	}()
	recipient, err := n.prepareSession()
	if err != nil {
		return err
	}
	ready := make(chan struct{}, 1)
	cfg, err := n.newControlConfig(recipient, ready)
	if err != nil {
		return err
	}
	launched = true
	go n.runControl(cfg)
	go func() { <-n.ctx.Done(); _ = n.Close() }()
	return n.awaitUsablePlan(ready)
}

// Run synchronizes until cancellation or a terminal control-stream failure.
func (n *Node) Run(ctx context.Context) error {
	if err := n.Start(ctx); err != nil {
		return err
	}
	<-n.runDone
	n.mu.Lock()
	err := n.runErr
	n.mu.Unlock()
	_ = n.Close()
	return err
}

// Close stops control work, drains traffic, and waits for apply before releasing
// owned state. DrainTimeout is not a deadline for this entire operation.
// The caller owns the identity Source and any injected composition state.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closed = true
		started, cancel := n.started, n.cancel
		n.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if started {
			<-n.initDone
			<-n.runDone
		}
		n.mu.Lock()
		bindings := make([]*Listener, 0, len(n.bindings))
		for _, b := range n.bindings {
			if b != nil {
				bindings = append(bindings, b)
			}
		}
		n.mu.Unlock()
		ctx, stop := context.WithTimeout(context.Background(), n.cfg.DrainTimeout)
		defer stop()
		for _, b := range bindings {
			_ = b.close(ctx)
		}
		if n.pipeline != nil {
			n.pipeline.Wait()
		}
		n.closeErr = n.runtime.Shutdown(ctx)
		for _, conn := range n.conns {
			_ = conn.Close()
		}
		if n.composition == nil && n.state != nil {
			n.closeErr = errors.Join(n.closeErr, n.state.Close())
		}
	})
	return n.closeErr
}

func (n *Node) openControllers(addrs []string, opts []grpc.DialOption) ([]*grpc.ClientConn, error) {
	conns := make([]*grpc.ClientConn, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			for _, opened := range conns {
				_ = opened.Close()
			}
			return nil, err
		}
		conns = append(conns, conn)
	}
	return conns, nil
}
func (n *Node) enroll(ctx context.Context, workload spiffeid.ID, opts []grpc.DialOption) error {
	var err error
	if n.cfg.JoinToken == "" {
		return errors.New("mesh node: join token required for first enrollment")
	}
	requestID, err := n.state.EnrollmentRequestID(n.cfg.JoinToken, workload.String())
	if err != nil {
		return err
	}
	for _, conn := range n.conns {
		callCtx, cancel := context.WithTimeout(ctx, n.cfg.ConnectTimeout)
		var registration *discoveryv1.RegisterResponse
		registration, err = discoveryv1.NewBootstrapClient(conn).Register(callCtx, &discoveryv1.RegisterRequest{RequestId: requestID, JoinToken: n.cfg.JoinToken, NodeName: workload.String(), Capabilities: []string{model.PlanCapability}})
		cancel()
		if err == nil {
			n.mu.Lock()
			n.nodeID = registration.NodeId
			n.mu.Unlock()
			if len(registration.ControllerAddresses) > 0 {
				discovered, dialErr := n.openControllers(registration.ControllerAddresses, opts)
				if dialErr != nil {
					return dialErr
				}
				for _, old := range n.conns {
					_ = old.Close()
				}
				n.conns = discovered
				n.cfg.Controllers = append([]string(nil), registration.ControllerAddresses...)
			}
			break
		}
	}
	if err != nil {
		return err
	}
	if err = n.state.SaveNodeID(n.nodeID); err != nil {
		return err
	}
	return nil
}
func (n *Node) newControlConfig(recipient model.Session, ready chan<- struct{}) (control.Config, error) {
	var input apply.Applier = n.pipeline
	if n.composition != nil {
		input = composedInput{n.composition, true}
	}
	applier, err := control.NewPlanApplier(control.PlanApplierConfig{Recipient: recipient, Pipeline: input, Verifier: n.verifier, HighWater: n.state.InputRevision})
	if err != nil {
		return control.Config{}, err
	}
	adsConfig := control.Config{ControlContact: func(addr string, err error) {
		n.mu.Lock()
		if err == nil {
			n.lastContact = time.Now()
			n.controlState = "connected"
		} else {
			n.controlState = "disconnected"
		}
		n.mu.Unlock()
		if n.runtime.Observer() != nil {
			n.runtime.Observer().ControlContact(addr, err)
		}
	}, TypeURL: model.PlanTypeURL, Appliers: map[string]control.Applier{model.PlanTypeURL: applier.DiscoveryApplier()}, Node: func() *discoveryv1.Node {
		return &discoveryv1.Node{Id: n.nodeID, Capabilities: []string{model.PlanCapability}}
	}, DialOptions: n.dialOptions, AfterApply: func(_ context.Context, _ string, _ *discoveryv1.Node, err error) error {
		n.mu.Lock()
		n.lastApplyFailure = Reason(err)
		n.mu.Unlock()
		if err == nil {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		return err
	}}
	return adsConfig, nil
}
func (n *Node) runControl(cfg control.Config) {
	err := control.New(cfg).Run(n.ctx, n.cfg.Controllers)
	n.mu.Lock()
	n.runErr = err
	if errors.Is(err, apply.ErrRecoveryRequired) {
		n.lastApplyFailure = Reason(err)
	}
	n.mu.Unlock()
	close(n.runDone)
	n.cancel()
}
func (n *Node) awaitUsablePlan(ready <-chan struct{}) error {
	for !dataplane.Fresh(n.runtime.CurrentPlan()) {
		select {
		case <-ready:
		case <-n.runDone:
			n.mu.Lock()
			err := n.runErr
			n.mu.Unlock()
			return err
		case <-n.ctx.Done():
			return n.ctx.Err()
		}
	}
	return nil
}

func (n *Node) prepareSession() (model.Session, error) {
	svid, err := n.cfg.Identity.GetSVID(n.ctx)
	if err != nil {
		return model.Session{}, err
	}
	workload, err := spiffeid.FromString(svid.ID.String())
	if err != nil {
		return model.Session{}, err
	}
	opts, err := n.dialOptions(n.ctx)
	if err != nil {
		return model.Session{}, err
	}
	conns, err := n.openControllers(n.cfg.Controllers, opts)
	if err != nil {
		return model.Session{}, err
	}
	n.conns = conns
	loadedID, loadErr := n.state.LoadNodeID()
	n.mu.Lock()
	n.nodeID = loadedID
	n.mu.Unlock()
	if errors.Is(loadErr, state.ErrNotFound) {
		if err = n.enroll(n.ctx, workload, opts); err != nil {
			return model.Session{}, err
		}
	} else if loadErr != nil {
		return model.Session{}, loadErr
	}
	recipient := model.Session{Tenant: n.cfg.Tenant, NodeID: n.nodeID, Workload: workload}
	n.runtime.SetRecipient(recipient)
	if n.composition == nil {
		if _, err := n.pipeline.Restore(); err != nil {
			return model.Session{}, err
		}
	}

	return recipient, nil
}
