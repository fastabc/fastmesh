package storetest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/fastabc/fastmesh/store"
)

type ExtensionBackend interface {
	store.ObjectStore
	Extension(string) (json.RawMessage, error)
	UpdateExtension(context.Context, string, func(json.RawMessage) (json.RawMessage, error)) error
}

// RunExtensionConformance exercises serialized updates beside ordinary writes.
func RunExtensionConformance(t *testing.T, open func(*testing.T) ExtensionBackend) {
	t.Helper()
	s := open(t)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			if err := s.UpdateExtension(context.Background(), "counter", func(raw json.RawMessage) (json.RawMessage, error) {
				var n int
				if len(raw) > 0 {
					if err := json.Unmarshal(raw, &n); err != nil {
						return nil, err
					}
				}
				return json.Marshal(n + 1)
			}); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() {
			if _, err := s.PutObject(context.Background(), store.Object{Key: store.ObjectKey{Tenant: "unknown", Kind: "test", Name: string(rune('a' + i))}, Data: []byte(`{}`)}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	raw, err := s.Extension("counter")
	if err != nil || string(raw) != "20" {
		t.Fatalf("lost extension update: %s %v", raw, err)
	}
	raw[0] = 'x'
	copy, _ := s.Extension("counter")
	if string(copy) != "20" {
		t.Fatal("extension read aliases stored bytes")
	}
	for _, name := range []string{"version", "Objects", "LEASES"} {
		if err := s.UpdateExtension(context.Background(), name, func(json.RawMessage) (json.RawMessage, error) { return []byte(`[]`), nil }); err == nil {
			t.Errorf("accepted reserved section %s", name)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.UpdateExtension(ctx, "counter", func(json.RawMessage) (json.RawMessage, error) { t.Error("canceled callback ran"); return nil, nil }); err == nil {
		t.Fatal("canceled update succeeded")
	}
}
