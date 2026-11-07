package discovery

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestStreamsSkipUnchangedRevisionAndRenewOnHeartbeat(t *testing.T) {
	for _, delta := range []bool{false, true} {
		for _, heartbeat := range []bool{false, true} {
			t.Run(map[bool]string{false: "full", true: "delta"}[delta]+map[bool]string{false: "/changes", true: "/heartbeat"}[heartbeat], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				changes := make(chan struct{}, 1)
				built := make(chan struct{}, 16)
				responses := make(chan string, 16)
				var signatures atomic.Int32
				var revision atomic.Value
				revision.Store("r1")
				interval := time.Hour
				if heartbeat {
					interval = 20 * time.Millisecond
				}
				s := New(Config{HeartbeatInterval: interval, CoalesceWindow: time.Millisecond,
					Authorize: func(_ context.Context, id, _ string) (Session, error) {
						return Session{Principal: model.Session{Tenant: "default", NodeID: id}, Node: &discoveryv1.Node{Id: id}}, nil
					},
					Watch: func(context.Context, Session, string) (<-chan struct{}, error) { return changes, nil },
					Builder: BuilderFunc(func(context.Context, BuildRequest) (Artifact, error) {
						built <- struct{}{}
						return Artifact{Revision: revision.Load().(string), Sign: func(context.Context) (*discoveryv1.SnapshotSignature, error) { signatures.Add(1); return nil, nil }}, nil
					}),
				})
				gs := grpc.NewServer()
				discoveryv1.RegisterDiscoveryServer(gs, s)
				lis := bufconn.Listen(1 << 16)
				go func() { _ = gs.Serve(lis) }()
				defer gs.Stop()
				conn, err := grpc.NewClient("passthrough:///revision", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				client := discoveryv1.NewDiscoveryClient(conn)
				if delta {
					stream, err := client.DeltaAggregatedResources(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if err := stream.Send(&discoveryv1.DeltaDiscoveryRequest{Node: &discoveryv1.Node{Id: "n"}}); err != nil {
						t.Fatal(err)
					}
					go func() {
						for {
							r, err := stream.Recv()
							if err != nil {
								return
							}
							if stream.Send(&discoveryv1.DeltaDiscoveryRequest{ResponseNonce: r.Nonce}) != nil {
								return
							}
							responses <- r.SystemVersionInfo
						}
					}()
				} else {
					stream, err := client.StreamAggregatedResources(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if err := stream.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: "n"}}); err != nil {
						t.Fatal(err)
					}
					go func() {
						for {
							r, err := stream.Recv()
							if err != nil {
								return
							}
							if stream.Send(&discoveryv1.DiscoveryRequest{ResponseNonce: r.Nonce, VersionInfo: r.VersionInfo}) != nil {
								return
							}
							responses <- r.VersionInfo
						}
					}()
				}
				receive := func(want string) {
					t.Helper()
					select {
					case got := <-responses:
						if got != want {
							t.Fatalf("revision=%s want=%s", got, want)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				receive("r1")
				<-built
				if heartbeat {
					receive("r1")
					receive("r1")
					return
				}
				for range 3 {
					changes <- struct{}{}
					select {
					case <-built:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					select {
					case got := <-responses:
						t.Fatalf("unchanged revision pushed again: %s", got)
					case <-time.After(25 * time.Millisecond):
					}
				}
				if got := signatures.Load(); got != 1 {
					t.Fatalf("unchanged notifications signed %d times", got)
				}
				revision.Store("r2")
				changes <- struct{}{}
				receive("r2")
				if got := signatures.Load(); got != 2 {
					t.Fatalf("changed revision signatures=%d", got)
				}
			})
		}
	}
}
