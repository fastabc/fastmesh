package node

import (
	"context"
	"io"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/internal/node/dataplane"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// This fixture uses real mesh mTLS and runtime policy, with no controller/journal
// work in the timed section. Plans are static and valid for the whole benchmark.
func benchmarkPeers(b *testing.B, protocol string) (*Node, *Node, *dataplane.Listener, ServiceRef) {
	b.Helper()
	issue, _ := lifecycleIssuer(b)
	ci, si := issue("client", 2), issue("server", 3)
	client := &Node{runtime: dataplane.New(ci, time.Second), cfg: Config{DialTimeout: time.Second}}
	server := &Node{runtime: dataplane.New(si, time.Second)}
	cp, sp := runtimePlans(spiffeid.RequireFromString(ci.svid.ID.String()), spiffeid.RequireFromString(si.svid.ID.String()))
	cp.NotAfter = time.Now().Add(time.Hour)
	sp.NotAfter = cp.NotAfter
	sp.Inbound[0].Port.Protocol = protocol
	installPlan(b, server.runtime, sp)
	ref := ServiceRef{Service: "payments", Port: "api"}
	lis, err := server.runtime.Listen(context.Background(), ref, "127.0.0.1:0", protocol != "tcp")
	if err != nil {
		b.Fatal(err)
	}
	cp.Outbound = []model.OutboundService{{Service: ref.Service, Port: sp.Inbound[0].Port, Endpoints: []model.Endpoint{{Tenant: "default", Service: ref.Service, Port: ref.Port, InstanceID: "one", Owner: sp.Workload, Address: netip.MustParseAddrPort(lis.Addr().String()), Generation: 1, Ready: true, Serving: true, LeaseExpiresAt: cp.NotAfter}}}}
	installPlan(b, client.runtime, cp)
	b.Cleanup(func() {
		lis.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client.runtime.Shutdown(ctx)
		server.runtime.Shutdown(ctx)
	})
	return client, server, lis, ref
}

func BenchmarkMeshTransport(b *testing.B) {
	b.Run("mTLS_Dial", func(b *testing.B) {
		client, _, lis, ref := benchmarkPeers(b, "tcp")
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				c, err := lis.Accept()
				if err != nil {
					return
				}
				c.Close()
			}
		}()
		b.Cleanup(func() { lis.Close(); <-done })
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c, err := client.Dial(context.Background(), ref)
			if err != nil {
				b.Fatal(err)
			}
			c.Close()
		}
	})
	b.Run("HTTP_reused", func(b *testing.B) {
		client, server, lis, ref := benchmarkPeers(b, "http")
		srv := &http.Server{ConnContext: dataplane.PeerContext, Handler: server.runtime.HTTPHandler(ref, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))}
		go srv.Serve(lis)
		b.Cleanup(func() { srv.Close() })
		hc, err := client.HTTPClientWithProtocol(ref, "http")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(hc.CloseIdleConnections)
		call := func() {
			resp, err := hc.Get("http://payments/pay")
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 204 {
				b.Fatal(resp.StatusCode)
			}
		}
		call()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			call()
		}
	})
	b.Run("GRPC_reused", func(b *testing.B) {
		client, server, lis, ref := benchmarkPeers(b, "grpc")
		unary, stream := server.GRPCAuthorizer(ref)
		srv := grpc.NewServer(grpc.Creds(server.GRPCServerCredentials()), grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))
		healthpb.RegisterHealthServer(srv, health.NewServer())
		go srv.Serve(lis)
		b.Cleanup(srv.Stop)
		conn, err := grpc.NewClient("passthrough:///payments", client.GRPCDialOptions(ref)...)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { conn.Close() })
		hc := healthpb.NewHealthClient(conn)
		call := func() {
			if _, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
				b.Fatal(err)
			}
		}
		call()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			call()
		}
	})
}
