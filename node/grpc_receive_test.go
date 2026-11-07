package node

import (
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockedReceive struct {
	grpc.ServerStream
	entered, release chan struct{}
}

func (s blockedReceive) RecvMsg(any) error { close(s.entered); <-s.release; return nil }
func TestGRPCRevocationWhileReceiveBlocked(t *testing.T) {
	var revoked atomic.Bool
	underlying := blockedReceive{entered: make(chan struct{}), release: make(chan struct{})}
	stream := authorizedStream{ServerStream: underlying, check: func() error {
		if revoked.Load() {
			return status.Error(codes.PermissionDenied, "revoked")
		}
		return nil
	}}
	done := make(chan error, 1)
	go func() { done <- stream.RecvMsg(new(int)) }()
	<-underlying.entered
	revoked.Store(true)
	close(underlying.release)
	select {
	case err := <-done:
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("message crossed revocation boundary: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receive did not exit")
	}
}
