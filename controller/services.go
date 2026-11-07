package controller

import (
	"context"

	"github.com/fastabc/fastmesh/internal/controller/catalogrpc"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/controller/admission"
	"github.com/fastabc/fastmesh/controller/discovery"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/grpc"
)

// RegisterServices registers the Discovery, Bootstrap and Catalog services on one gRPC server.
func (c *Controller) RegisterServices(gs *grpc.Server) {
	dcfg := c.cfg.discovery
	dcfg.Authorize = c.authorizeDiscovery
	dcfg.Builder = discovery.BuilderFunc(c.build)
	dcfg.Watch = c.watch
	discoveryv1.RegisterDiscoveryServer(gs, discovery.New(dcfg))
	discoveryv1.RegisterBootstrapServer(gs, &admission.Bootstrap{Objects: c.objects, Tokens: c.tokens, DevInsecure: c.cfg.DevInsecure, Region: c.cfg.Region, TrustDomain: c.cfg.TrustDomain, ControllerAddrs: c.cfg.ControllerAddrs, AuthorizeWrite: c.requireLeader, CellLabel: c.cfg.CellLabel})
	catalogv1.RegisterCatalogServer(gs, &catalogrpc.Server{Manager: c.endpoints, AuthorizeSession: func(ctx context.Context, id string) (model.Session, error) {
		session, err := c.authorize(ctx, id, "")
		return session.Principal, err
	}})
	for _, register := range c.cfg.grpcRegistrars {
		register(gs)
	}
}
