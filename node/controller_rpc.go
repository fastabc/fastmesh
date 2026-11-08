package node

import (
	"context"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func (n *Node) dialOptions(ctx context.Context) ([]grpc.DialOption, error) {
	cfg, err := spiffe.ClientTLSConfig(ctx, n.cfg.Identity, n.cfg.AllowedControllerIDs, n.cfg.TrustDomain)
	if err != nil {
		return nil, err
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(cfg)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(model.MaxPlanEnvelopeBytes))}, nil
}

func (n *Node) lease(ctx context.Context, call func(context.Context, catalogv1.CatalogClient) (*catalogv1.EndpointLease, error)) (*catalogv1.EndpointLease, error) {
	var err error = ErrDenied
	for _, conn := range n.conns {
		callCtx, cancel := context.WithTimeout(ctx, n.cfg.ConnectTimeout)
		var lease *catalogv1.EndpointLease
		lease, err = call(callCtx, catalogv1.NewCatalogClient(conn))
		cancel()
		if err == nil {
			return lease, nil
		}
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.PermissionDenied, codes.InvalidArgument, codes.NotFound:
			// The leader answered the write. A later follower's Unavailable
			// must not hide a lost generation or an authorization rejection.
			return nil, err
		}
	}
	return nil, err
}
