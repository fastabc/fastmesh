package enrollment

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fastabc/fastmesh/store/memory"
)

func TestMaxUsesConcurrentConsumption(t *testing.T) {
	s := memory.New()
	a, b := New(s), New(s)
	token, err := a.Create(context.Background(), CreateRequest{MaxUses: 3, Ephemeral: true})
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := a
			if i%2 == 1 {
				m = b
			}
			tok, err := m.Consume(context.Background(), token.Raw)
			if err == nil {
				accepted.Add(1)
				if !tok.Ephemeral {
					t.Error("lost ephemeral bit")
				}
			} else if !errors.Is(err, ErrExhaustedToken) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 3 {
		t.Fatalf("accepted=%d", accepted.Load())
	}
	if _, err := a.Validate(context.Background(), token.Raw); !errors.Is(err, ErrExhaustedToken) {
		t.Fatalf("exhausted token validates: %v", err)
	}
	if err := a.Revoke(context.Background(), token.Token.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Consume(context.Background(), token.Raw); !errors.Is(err, ErrRevokedToken) {
		t.Fatalf("revoked token consumed: %v", err)
	}
}
