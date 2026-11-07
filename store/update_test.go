package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
)

type updateStore struct {
	*memory.Store
	calls    int
	failures int
	failure  error
}

func (s *updateStore) PutObject(ctx context.Context, o store.Object) (store.Object, error) {
	s.calls++
	if s.failures > 0 {
		s.failures--
		return store.Object{}, s.failure
	}
	return s.Store.PutObject(ctx, o)
}
func (s *updateStore) CreateObject(ctx context.Context, o store.Object) (store.Object, error) {
	s.calls++
	if s.failures > 0 {
		s.failures--
		return store.Object{}, s.failure
	}
	return s.Store.CreateObject(ctx, o)
}

func TestUpdateJSONRetriesConflictsButNotFencing(t *testing.T) {
	type value struct {
		N int `json:"n"`
	}
	key := store.ObjectKey{Tenant: "acme", Kind: "test", Name: "one"}
	for _, mode := range []string{"create", "retry", "fencing", "callback", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			s := &updateStore{Store: memory.New(), failure: store.ErrConflict}
			if mode == "retry" {
				s.failures = 1
			}
			if mode == "fencing" {
				s.failures = 1
				s.failure = &store.NotLeaderError{Hint: "new-leader"}
			}
			if mode == "exhausted" {
				s.failures = 10
			}
			callbackErr := errors.New("mutation rejected")
			calls := 0
			_, err := store.UpdateJSON(context.Background(), s, key, 3, func(v *value, exists bool) error {
				calls++
				if exists {
					t.Error("missing object reported as existing")
				}
				if mode == "callback" {
					return callbackErr
				}
				v.N = 1
				return nil
			})
			switch mode {
			case "create":
				if err != nil || calls != 1 || s.calls != 1 {
					t.Fatalf("create: %v calls=%d/%d", err, calls, s.calls)
				}
			case "retry":
				if err != nil || calls != 2 {
					t.Fatalf("retry: %v calls=%d", err, calls)
				}
			case "fencing":
				var fenced *store.NotLeaderError
				if !errors.As(err, &fenced) || calls != 1 {
					t.Fatalf("fencing retried: %v calls=%d", err, calls)
				}
			case "callback":
				if !errors.Is(err, callbackErr) || s.calls != 0 {
					t.Fatalf("callback wrote: %v calls=%d", err, s.calls)
				}
			case "exhausted":
				if !errors.Is(err, store.ErrConflict) || calls != 3 {
					t.Fatalf("exhaustion: %v calls=%d", err, calls)
				}
			}
		})
	}
}
