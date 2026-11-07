package discovery

import (
	"context"
	"net"
	"testing"
	"time"

	discoveryv1 "github.com/fastabc/fastmesh/api/discovery/v1"
	"github.com/fastabc/fastmesh/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"
)

func boundedServer(t *testing.T, cfg Config) (*Server, discoveryv1.DiscoveryClient) {
	t.Helper()
	cfg.DefaultTypeURL = "test"
	cfg.HeartbeatInterval = time.Hour
	cfg.Authorize = func(_ context.Context, id, _ string) (Session, error) {
		return Session{Principal: model.Session{Tenant: "default", NodeID: id}, Node: &discoveryv1.Node{Id: id}}, nil
	}
	cfg.Watch = func(context.Context, Session, string) (<-chan struct{}, error) { return nil, nil }
	if cfg.Builder == nil {
		cfg.Builder = BuilderFunc(func(context.Context, BuildRequest) (Artifact, error) { return Artifact{Revision: "r1"}, nil })
	}
	s := New(cfg)
	gs := grpc.NewServer()
	discoveryv1.RegisterDiscoveryServer(gs, s)
	lis := bufconn.Listen(4096)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///limits", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return s, discoveryv1.NewDiscoveryClient(conn)
}

func waitStreams(t *testing.T, s *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n := s.streams
		s.mu.Unlock()
		if n == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("stream count did not reach %d", want)
}

func TestDiscoveryFirstFrameTimeout(t *testing.T) {
	s, c := boundedServer(t, Config{FirstRequestTimeout: 40 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	full, err := c.StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := full.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("full: %v", err)
	}
	delta, err := c.DeltaAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delta.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("delta: %v", err)
	}
	waitStreams(t, s, 0)
}

func TestDiscoveryAckTimeoutReleasesStream(t *testing.T) {
	for _, deltaMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "delta"}[deltaMode], func(t *testing.T) {
			s, c := boundedServer(t, Config{AckTimeout: 80 * time.Millisecond})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if deltaMode {
				stream, err := c.DeltaAggregatedResources(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.Send(&discoveryv1.DeltaDiscoveryRequest{Node: &discoveryv1.Node{Id: "a"}}); err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Recv(); err != nil {
					t.Fatal(err)
				}
				if err := stream.Send(&discoveryv1.DeltaDiscoveryRequest{ResponseNonce: "stale"}); err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
					t.Fatalf("recv: %v", err)
				}
			} else {
				stream, err := c.StreamAggregatedResources(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: "a"}}); err != nil {
					t.Fatal(err)
				}
				response, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				// Even the right nonce cannot ACK another revision.
				if err := stream.Send(&discoveryv1.DiscoveryRequest{ResponseNonce: response.Nonce, VersionInfo: "stale"}); err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
					t.Fatalf("recv: %v", err)
				}
			}
			waitStreams(t, s, 0)
		})
	}
}

func TestDiscoveryPerNodeFairnessAndValidAck(t *testing.T) {
	s, c := boundedServer(t, Config{MaxStreamsPerNode: 1, AckTimeout: 80 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	open := func(id string) grpc.BidiStreamingClient[discoveryv1.DiscoveryRequest, discoveryv1.DiscoveryResponse] {
		t.Helper()
		stream, err := c.StreamAggregatedResources(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: id}}); err != nil {
			t.Fatal(err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&discoveryv1.DiscoveryRequest{ResponseNonce: response.Nonce, VersionInfo: response.VersionInfo}); err != nil {
			t.Fatal(err)
		}
		return stream
	}
	_ = open("a")
	duplicate, err := c.DeltaAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := duplicate.Send(&discoveryv1.DeltaDiscoveryRequest{Node: &discoveryv1.Node{Id: "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := duplicate.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("duplicate: %v", err)
	}
	_ = open("b")
	time.Sleep(160 * time.Millisecond)
	waitStreams(t, s, 2)
	cancel()
	waitStreams(t, s, 0)
}

func TestDiscoveryTotalLimitIncludesSilentStreams(t *testing.T) {
	s, c := boundedServer(t, Config{MaxStreams: 1, FirstRequestTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	firstCtx, closeFirst := context.WithCancel(ctx)
	if _, err := c.StreamAggregatedResources(firstCtx); err != nil {
		t.Fatal(err)
	}
	waitStreams(t, s, 1)
	second, err := c.DeltaAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("capacity: %v", err)
	}
	closeFirst()
	waitStreams(t, s, 0)
	third, err := c.StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: "c"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := third.Recv(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverySlowReaderReleasesStream(t *testing.T) {
	s, c := boundedServer(t, Config{AckTimeout: 80 * time.Millisecond, Builder: BuilderFunc(func(context.Context, BuildRequest) (Artifact, error) {
		return Artifact{Revision: "large", Resources: []*discoveryv1.Resource{{Resource: &anypb.Any{Value: make([]byte, 8<<20)}}}}, nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := c.StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&discoveryv1.DiscoveryRequest{Node: &discoveryv1.Node{Id: "slow"}}); err != nil {
		t.Fatal(err)
	}
	waitStreams(t, s, 1)
	// The client never reads. The handler must exit even while Send is blocked
	// by HTTP/2 flow control, and gRPC then cancels the transport goroutine.
	waitStreams(t, s, 0)
}
