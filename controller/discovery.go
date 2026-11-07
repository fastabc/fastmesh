package controller

import (
	"context"
	"errors"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/discovery"
	"github.com/fastabc/fastmesh/internal/controller/plancache"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
	"github.com/fastabc/fastmesh/policy/verify"
	"github.com/fastabc/fastmesh/store"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Controller) authorize(ctx context.Context, id, tenant string) (discovery.Session, error) {
	if err := c.requireLeader(); err != nil {
		return discovery.Session{}, err
	}
	return c.authorizeSession(ctx, id, tenant, false)
}

func (c *Controller) authorizeDiscovery(ctx context.Context, id, tenant string) (discovery.Session, error) {
	return c.authorizeSession(ctx, id, tenant, !c.Active())
}

func (c *Controller) authorizeSession(ctx context.Context, id, tenant string, readOnly bool) (discovery.Session, error) {
	if c.cfg.discovery.Authorize != nil {
		return c.cfg.discovery.Authorize(ctx, id, tenant)
	}
	var n model.Node
	var err error
	if readOnly {
		n, err = c.admission.AuthorizeReadSession(ctx, id, tenant)
	} else {
		n, err = c.admission.AuthorizeSession(ctx, id, tenant)
	}
	if err != nil {
		return discovery.Session{}, err
	}
	var workload spiffeid.ID
	if n.SpiffeID != "" {
		workload, err = spiffeid.FromString(n.SpiffeID)
		if err != nil {
			return discovery.Session{}, err
		}
	}
	observed := n.Status
	capabilities := observed.ReportedCapabilities
	return discovery.Session{Principal: model.Session{Tenant: n.Tenant, NodeID: n.ID, Workload: workload}, Node: &discoveryv1.Node{Id: n.ID, SpiffeId: n.SpiffeID, Capabilities: capabilities, Intent: n.Intent, Generation: n.Generation, ObservedIntent: observed.ObservedIntent, ObservedGeneration: observed.ObservedGeneration}}, nil
}

func (c *Controller) build(ctx context.Context, r discovery.BuildRequest) (discovery.Artifact, error) {
	if r.TypeURL != model.PlanTypeURL {
		if c.cfg.discovery.Builder == nil {
			return discovery.Artifact{}, status.Error(codes.Unimplemented, "unsupported discovery type")
		}
		return c.cfg.discovery.Builder.Build(ctx, r)
	}
	captured, err := c.index.Snapshot(ctx, r.Session.Principal.Tenant)
	if err != nil {
		return discovery.Artifact{}, err
	}
	var recorder func(time.Duration, error)
	if c.compileObserver != nil {
		recorder = c.compileObserver.RecordPlanCompile
	}
	resource, p, err := c.plans.Resource(ctx, captured, r.Session.Principal, r.TypeURL, time.Now(), c.cfg.PlanValidity, recorder)
	if errors.Is(err, plancache.ErrCapacity) {
		return discovery.Artifact{}, status.Error(codes.ResourceExhausted, err.Error())
	}
	if err != nil {
		return discovery.Artifact{}, err
	}
	return discovery.Artifact{InputRevision: p.InputRevision, Revision: p.Hash, Resources: []*discoveryv1.Resource{resource}, FetchPayload: resource.Resource.Value, Sign: func(ctx context.Context) (*discoveryv1.SnapshotSignature, error) {
		payload, err := verify.PlanBodySigningPayload(r.TypeURL, resource.Resource.Value)
		if err != nil {
			return nil, err
		}
		started := time.Now()
		sig, err := snapshot.SignPayload(ctx, c.cfg.Identity, payload, nil)
		if c.signatureObserver != nil {
			c.signatureObserver.RecordPlanSignature(time.Since(started), err)
		}
		if err != nil {
			return nil, err
		}
		return &discoveryv1.SnapshotSignature{Algorithm: sig.Algorithm, KeyId: sig.KeyID, Value: sig.Value, CertChain: sig.CertChain}, nil
	}}, nil
}

func (c *Controller) watch(ctx context.Context, session discovery.Session, typeURL string) (<-chan struct{}, error) {
	if typeURL != model.PlanTypeURL {
		if c.cfg.discovery.Watch == nil {
			return nil, store.ErrWatchUnsupported
		}
		return c.cfg.discovery.Watch(ctx, session, typeURL)
	}
	return c.index.Subscribe(ctx, session.Principal.Tenant)
}
