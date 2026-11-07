package catalogrpc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	catalogv1 "github.com/fastabc/fastmesh/api/catalog/v1"
	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/internal/controller/endpoint"
	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRPCErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"generation", endpoint.ErrGeneration, codes.FailedPrecondition},
		{"conflict", store.ErrConflict, codes.Aborted},
		{"not-leader", &store.NotLeaderError{Hint: "controller:9443"}, codes.Unavailable},
		{"not-leader-without-hint", &store.NotLeaderError{}, codes.Unavailable},
		{"denied", endpoint.ErrDenied, codes.PermissionDenied},
		{"invalid", model.ErrInvalid, codes.InvalidArgument},
		{"not-found", store.ErrNotFound, codes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := rpcError(fmt.Errorf("wrapped: %w", tc.err))
			if status.Code(err) != tc.code {
				t.Fatalf("code=%v, want %v: %v", status.Code(err), tc.code, err)
			}
			if tc.name == "not-leader" && !strings.Contains(status.Convert(err).Message(), "controller:9443") {
				t.Fatalf("missing leader hint: %v", err)
			}
		})
	}
}

func TestCatalogRPCSharesDiscoveryServerAndSessionAdmission(t *testing.T) {
	backing := memory.New()
	session, req := seed(t, backing)
	rpc := &Server{Manager: &endpoint.Manager{Objects: backing, Viewer: backing}, AuthorizeSession: func(_ context.Context, node string) (model.Session, error) {
		if node != session.NodeID {
			return model.Session{}, status.Error(codes.PermissionDenied, "node not bound to peer")
		}
		return session, nil
	}}
	server := grpc.NewServer()
	discoveryv1.RegisterDiscoveryServer(server, discoveryv1.UnimplementedDiscoveryServer{})
	catalogv1.RegisterCatalogServer(server, rpc)
	if len(server.GetServiceInfo()) != 2 {
		t.Fatal("catalog did not share gRPC server")
	}
	lis := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///catalog", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := catalogv1.NewCatalogClient(conn)
	register := &catalogv1.RegisterEndpointRequest{NodeId: "forged", Service: req.Service, Port: req.Port, InstanceId: req.InstanceID, Generation: req.Generation, Address: req.Address.String(), Ready: true}
	if _, err = client.RegisterEndpoint(ctx, register); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("forged register: %v", err)
	}
	register.NodeId = session.NodeID
	lease, err := client.RegisterEndpoint(ctx, register)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Owner != session.Workload.String() {
		t.Fatalf("owner=%q", lease.Owner)
	}
	update := &catalogv1.EndpointRequest{NodeId: "forged", Service: req.Service, InstanceId: req.InstanceID, Generation: 1}
	if _, err = client.RenewEndpoint(ctx, update); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("forged renew: %v", err)
	}
	if _, err = client.DeregisterEndpoint(ctx, update); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("forged deregister: %v", err)
	}
	update.NodeId = session.NodeID
	if _, err = client.RenewEndpoint(ctx, update); err != nil {
		t.Fatal(err)
	}
	if _, err = client.DeregisterEndpoint(ctx, update); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RenewEndpoint(ctx, update); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing lease renewal: %v", err)
	}
}
