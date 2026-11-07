package dataplane

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func assertEcho(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := io.ReadFull(c, b[:]); err != nil || b[0] != 'x' {
		t.Fatalf("echo=%q err=%v", b, err)
	}
}

func TestSaturatedHandshakeCompletionCannotBeEvicted(t *testing.T) {
	peerID := spiffeid.RequireFromString("spiffe://test/client")
	_, plan := runtimePlans(peerID, spiffeid.RequireFromString("spiffe://test/server"))
	r := New(nil, time.Second)
	installPlan(t, r, plan)
	defer r.Shutdown(context.Background())
	for range 100 {
		l := &Listener{runtime: r}
		local, remote := net.Pipe()
		c := &Conn{conn: local, runtime: r, ref: ServiceRef{Service: "payments", Port: "api"}}
		if err := r.admit(c); err != nil {
			t.Fatal(err)
		}
		l.addPending(c)
		for range MaxPendingHandshakes - 1 {
			l.addPending(&Conn{})
		}
		start := make(chan struct{})
		finished := make(chan bool, 1)
		evicted := make(chan *Conn, 1)
		go func() { <-start; finished <- l.finishHandshake(c, peerID) }()
		go func() { <-start; evicted <- l.addPending(&Conn{}) }()
		close(start)
		ready, victim := <-finished, <-evicted
		if ready {
			if victim == c {
				t.Fatal("completed handshake selected for eviction")
			}
			// Once ready, later saturation must keep this application connection.
			for range MaxPendingHandshakes + 1 {
				if l.addPending(&Conn{}) == c {
					t.Fatal("ready connection remained pending")
				}
			}
			go func() { _, _ = io.Copy(remote, remote) }()
			assertEcho(t, c)
		} else {
			if victim != c || c.ready {
				t.Fatal("evicted handshake became ready")
			}
			// The eviction owner may close after the completion attempt; this window
			// must not allow the selected victim to be handed to the application.
			if l.finishHandshake(c, peerID) {
				t.Fatal("evicted handshake reclaimed delivery")
			}
		}
		_ = c.Close()
		_ = remote.Close()
	}
}
