package node

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestNodeCloseDrainsHTTPAndStopsSelectingTerminatingInstance(t *testing.T) {
	client, server, _, ctx := startNodePair(t, false)
	ref := ServiceRef{Service: "payments", Port: "api"}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	_, err := server.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "drained")
	}))
	if err != nil {
		t.Fatal(err)
	}
	for {
		p := client.runtime.CurrentPlan()
		if len(p.Outbound) == 1 && len(p.Outbound[0].Endpoints) == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	httpClient := client.HTTPClient(ref)
	defer httpClient.CloseIdleConnections()
	response := make(chan error, 1)
	go func() {
		r, err := httpClient.Get("https://payments/pay")
		if err == nil {
			var data []byte
			data, err = io.ReadAll(r.Body)
			_ = r.Body.Close()
			if err == nil && string(data) != "drained" {
				err = io.ErrUnexpectedEOF
			}
		}
		response <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closed := make(chan error, 1)
	closeStart := time.Now()
	go func() { closed <- server.Close() }()
	// Wait for the signed terminating publication while the handler is blocked.
	for {
		p := client.runtime.CurrentPlan()
		if len(p.Outbound) == 1 && len(p.Outbound[0].Endpoints) == 1 && p.Outbound[0].Endpoints[0].Terminating {
			break
		}
		select {
		case err := <-closed:
			t.Fatalf("Close returned before request drained: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if conn, err := client.Dial(ctx, ref); err == nil {
		_ = conn.Close()
		t.Fatal("new connection selected terminating instance")
	}
	once.Do(func() { close(release) })
	select {
	case err := <-response:
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(closeStart) >= server.cfg.DrainTimeout {
			t.Fatal("request completed beyond DrainTimeout")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestExistingV1EnrollmentNegotiatesV1WithoutChangingNodeID(t *testing.T) {
	client, server, _, ctx := startNodePair(t, true)
	ref := ServiceRef{Service: "payments", Port: "api"}
	_, err := server.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "v1") }))
	if err != nil {
		t.Fatal(err)
	}
	httpClient := client.HTTPClient(ref)
	defer httpClient.CloseIdleConnections()
	for {
		r, err := httpClient.Get("https://payments/")
		if err == nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	persisted, err := client.state.LoadNodeID()
	if err != nil || persisted != client.nodeID {
		t.Fatalf("persistent node ID changed: %s %v", persisted, err)
	}

}

func TestNodeBindRevocationClosesRealConnectionWithinOneSecond(t *testing.T) {
	client, server, controller, ctx := startNodePair(t, false)
	ref := ServiceRef{Service: "payments", Port: "api"}
	lis, err := server.Listen(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); _ = conn.Close() }()
		}
	}()
	var conn net.Conn
	for {
		conn, err = client.Dial(ctx, ref)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := io.ReadFull(conn, b[:]); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := controller.DeleteBindGrant(ctx, "default", "payments"); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(start.Add(time.Second))
	if _, err := conn.Read(b[:]); err == nil || time.Since(start) >= time.Second {
		t.Fatalf("revocation close latency=%v err=%v", time.Since(start), err)
	}
}

func TestNodeCloseForcesHTTPConnectionClosedAtDrainTimeout(t *testing.T) {
	client, server, _, ctx := startNodePair(t, false)
	server.cfg.DrainTimeout = 100 * time.Millisecond
	ref := ServiceRef{Service: "payments", Port: "api"}
	started, release, handled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		close(release)
		select {
		case <-handled:
		case <-time.After(time.Second):
		}
	})
	_, err := server.HTTPHandler(ctx, ref, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		defer close(handled)
		close(started)
		<-release
		_, _ = io.WriteString(w, "late")
	}))
	if err != nil {
		t.Fatal(err)
	}
	for len(client.runtime.CurrentPlan().Outbound[0].Endpoints) == 0 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	httpClient := client.HTTPClient(ref)
	defer httpClient.CloseIdleConnections()
	response := make(chan error, 1)
	go func() {
		r, err := httpClient.Get("https://payments/pay")
		if err == nil {
			_ = r.Body.Close()
		}
		response <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	start := time.Now()
	if err := server.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close=%v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Close exceeded drain budget")
	}
	select {
	case err := <-response:
		if err == nil {
			t.Fatal("connection survived drain timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out HTTP connection stayed open")
	}
}

func TestEnrollmentRejectsForeignPersistentNodeID(t *testing.T) {
	client, server, _, _ := startNodePair(t, true)
	cfg := server.cfg
	cfg.StateDir = t.TempDir()
	n, err := newPlanTestNode(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.state.SaveNodeID(client.nodeID); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop()
	if err := n.Start(ctx); err == nil {
		t.Fatal("foreign enrollment accepted")
	}
	if n.runtime.CurrentPlan() != nil {
		t.Fatal("identity rejection published a plan")
	}
}
