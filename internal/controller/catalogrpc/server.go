package catalogrpc

import (
	"context"
	"errors"
	"net/netip"

	"github.com/fastabc/fastmesh/internal/controller/endpoint"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	catalogv1.UnimplementedCatalogServer
	Manager          *endpoint.Manager
	AuthorizeSession func(context.Context, string) (model.Session, error)
}

func (s *Server) session(ctx context.Context, nodeID string) (model.Session, error) {
	if s.AuthorizeSession == nil {
		return model.Session{}, status.Error(codes.Unauthenticated, "catalog admission unavailable")
	}
	return s.AuthorizeSession(ctx, nodeID)
}

func (s *Server) RegisterEndpoint(ctx context.Context, req *catalogv1.RegisterEndpointRequest) (*catalogv1.EndpointLease, error) {
	session, err := s.session(ctx, req.GetNodeId())
	if err != nil {
		return nil, err
	}
	addr, err := netip.ParseAddrPort(req.GetAddress())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid endpoint address")
	}
	endpoint, err := s.Manager.Register(ctx, session, endpoint.Registration{Service: req.GetService(), Port: req.GetPort(), InstanceID: req.GetInstanceId(), Generation: req.GetGeneration(), Address: addr, Ready: req.GetReady(), Serving: req.GetServing(), Terminating: req.GetTerminating()})
	if err != nil {
		return nil, rpcError(err)
	}
	return lease(endpoint), nil
}

func (s *Server) RenewEndpoint(ctx context.Context, req *catalogv1.EndpointRequest) (*catalogv1.EndpointLease, error) {
	session, err := s.session(ctx, req.GetNodeId())
	if err != nil {
		return nil, err
	}
	endpoint, err := s.Manager.Renew(ctx, session, req.GetService(), req.GetInstanceId(), req.GetGeneration())
	if err != nil {
		return nil, rpcError(err)
	}
	return lease(endpoint), nil
}

func (s *Server) DeregisterEndpoint(ctx context.Context, req *catalogv1.EndpointRequest) (*emptypb.Empty, error) {
	session, err := s.session(ctx, req.GetNodeId())
	if err != nil {
		return nil, err
	}
	if err = s.Manager.Deregister(ctx, session, req.GetService(), req.GetInstanceId(), req.GetGeneration()); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

func lease(e model.Endpoint) *catalogv1.EndpointLease {
	return &catalogv1.EndpointLease{Owner: e.Owner.String(), Generation: e.Generation, ExpiresAt: timestamppb.New(e.LeaseExpiresAt)}
}

func rpcError(err error) error {
	var notLeader *store.NotLeaderError
	switch {
	case errors.As(err, &notLeader):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, endpoint.ErrDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, endpoint.ErrGeneration):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, model.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *Server) SetEndpointServing(ctx context.Context, req *catalogv1.EndpointRequest) (*catalogv1.EndpointLease, error) {
	session, err := s.session(ctx, req.GetNodeId())
	if err != nil {
		return nil, err
	}
	endpoint, err := s.Manager.SetServing(ctx, session, req.GetService(), req.GetInstanceId(), req.GetGeneration(), req.GetServing())
	if err != nil {
		return nil, rpcError(err)
	}
	return lease(endpoint), nil
}
