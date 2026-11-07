package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/memory"
)

func TestPutStatusEnforcesRegisteredOwnerAndPreservesSpec(t *testing.T) {
	objects := memory.New()
	key := store.ObjectKey{Tenant: "default", Kind: "Node", Name: "web"}
	if _, err := objects.PutObject(context.Background(), store.Object{Key: key, Data: []byte(`{
		"apiVersion":"example/v1","kind":"Node",
		"metadata":{"tenant":"default","name":"web"},
		"spec":{"value":"retained"}
	}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutStatus(context.Background(), objects, "other-controller", key, map[string]any{"state": "Ready"}); !errors.Is(err, store.ErrStatusOwner) {
		t.Fatalf("wrong owner err=%v", err)
	}
	if _, err := store.PutStatus(context.Background(), objects, "ads-controller", key, map[string]any{"state": "Ready"}); err != nil {
		t.Fatal(err)
	}
	got, err := objects.GetObject(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Spec   map[string]any `json:"spec"`
		Status map[string]any `json:"status"`
	}
	if err := json.Unmarshal(got.Data, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Spec["value"] != "retained" || envelope.Status["state"] != "Ready" {
		t.Fatalf("envelope=%+v", envelope)
	}
}

func TestStatusOwnerRegistryMatchesCurrentWriters(t *testing.T) {
	want := map[string]string{
		"Node":         "ads-controller",
		"mesh.Service": "mesh-catalog",
	}
	if len(store.StatusOwners) != len(want) {
		t.Fatalf("owners=%v want=%v", store.StatusOwners, want)
	}
	for kind, owner := range want {
		if store.StatusOwners[kind] != owner {
			t.Fatalf("owner[%q]=%q want %q", kind, store.StatusOwners[kind], owner)
		}
	}
}

func TestUnregisteredStatusOwnerFailsClosed(t *testing.T) {
	key := store.ObjectKey{Tenant: "default", Kind: "application.Widget", Name: "web"}
	for _, owner := range []string{"", "application-controller"} {
		if _, err := store.PutStatus(context.Background(), memory.New(), owner, key, nil); !errors.Is(err, store.ErrStatusOwner) {
			t.Fatalf("owner %q: %v", owner, err)
		}
	}
}

func TestSentinelMessagesUsePackagePrefix(t *testing.T) {
	for err, want := range map[error]string{store.ErrNotFound: "store: not found", store.ErrConflict: "store: conflict", store.ErrConditionalDeleteUnsupported: "store: conditional delete unsupported", store.ErrAtomicCreateUnsupported: "store: atomic create unsupported", store.ErrStatusOwner: "store: status owner mismatch", (&store.NotLeaderError{Hint: "a"}): "store: conflict: leader is a"} {
		if err.Error() != want {
			t.Errorf("%q, want %q", err, want)
		}
	}
}

func TestStatusRejectsInvalidEnvelopes(t *testing.T) {
	for _, body := range []string{"spec: value", "null", "[]", "{", "42"} {
		t.Run(body, func(t *testing.T) {
			objects := memory.New()
			key := store.ObjectKey{Tenant: "default", Kind: "Node", Name: "n"}
			original, err := objects.PutObject(context.Background(), store.Object{Key: key, Data: []byte(body)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.PutStatus(context.Background(), objects, "ads-controller", key, map[string]string{"state": "Ready"}); err == nil {
				t.Fatal("accepted invalid envelope")
			}
			current, err := objects.GetObject(context.Background(), key)
			if err != nil || current.ResourceVersion != original.ResourceVersion {
				t.Fatal("rejected write changed object")
			}
		})
	}
}
