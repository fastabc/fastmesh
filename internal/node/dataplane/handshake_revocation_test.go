package dataplane

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/identity/spiffe"
	"github.com/fastabc/fastmesh/model"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
)

type handshakeObserver struct {
	rejected chan struct{}
}

func (o *handshakeObserver) ConnDenied(model.Direction, ServiceRef, spiffeid.ID, string) {
	close(o.rejected)
}

func TestRevocationBetweenTLSAuthorizationAndHandshakeCompletion(t *testing.T) {
	clientIdentity, serverIdentity, _ := lifecycleIdentities(t)
	clientID := spiffeid.RequireFromString(clientIdentity.svid.ID.String())
	serverID := spiffeid.RequireFromString(serverIdentity.svid.ID.String())
	_, plan := runtimePlans(clientID, serverID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime := New(serverIdentity, time.Second)
	observer := &handshakeObserver{rejected: make(chan struct{})}
	runtime.SetObserver(observer)
	installPlan(t, runtime, plan)
	lis, err := runtime.Listen(ctx, ServiceRef{Service: "payments", Port: "api"}, "127.0.0.1:0", false)
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	authorized, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	// crypto/tls calls VerifyConnection after the existing peer-certificate
	// authorization callback, but before HandshakeContext returns.
	lis.config.VerifyConnection = func(tls.ConnectionState) error {
		close(authorized)
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	accepted := make(chan net.Conn, 1)
	go func() { conn, _ := lis.Accept(); accepted <- conn }()
	defer func() {
		_ = lis.Close()
		if conn := <-accepted; conn != nil {
			_ = conn.Close()
			t.Error("connection admitted after its authorization was revoked")
		}
	}()
	native := spiffe.X509Source{Context: ctx, Source: clientIdentity}
	dialer := tls.Dialer{Config: tlsconfig.MTLSClientConfig(native, native, tlsconfig.AuthorizeID(serverID))}
	dialed := make(chan struct{})
	go func() {
		defer close(dialed)
		conn, err := dialer.DialContext(ctx, "tcp", lis.Addr().String())
		if err == nil {
			defer conn.Close()
			<-ctx.Done()
		}
	}()
	defer func() {
		release.Do(func() { close(resume) })
		cancel()
		<-dialed
	}()
	select {
	case <-authorized:
	case <-ctx.Done():
		t.Fatal("TLS authorization did not reach barrier")
	}
	plan.Inbound = nil
	installPlan(t, runtime, plan)
	release.Do(func() { close(resume) })
	select {
	case <-observer.rejected:
	case <-ctx.Done():
		t.Fatal("completed handshake was not rejected after revocation")
	}
}

func (*handshakeObserver) PlanApplied(int64, time.Time, bool)                   {}
func (*handshakeObserver) ConnRevoked(model.Direction, ServiceRef, spiffeid.ID) {}
func (*handshakeObserver) DialFailed(ServiceRef, netip.AddrPort, error)         {}
func (*handshakeObserver) ControlContact(string, error)                         {}
