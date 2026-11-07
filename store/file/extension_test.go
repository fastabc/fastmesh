package file

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/fastabc/fastmesh/store"
)

func TestUnknownSectionsSurviveObjectWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	initial := []byte(`{"version":3,"objects":[],"tenants":[{"name":"acme","quota":{"max_routes":2},"future":"<tenant>"}],"roles":[{"tenant":"acme","name":"viewer","future":{"x":1},"rules":[{"resources":["route"],"verbs":["get"]}]}]}`)
	if err := os.WriteFile(path, initial, 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.PutObject(context.Background(), store.Object{Key: store.ObjectKey{Tenant: "acme", Kind: "mesh.Service", Name: "x"}, Data: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var before, after map[string]json.RawMessage
	if err := json.Unmarshal(initial, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &after); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tenants", "roles"} {
		var a, b bytes.Buffer
		if err := json.Compact(&a, before[key]); err != nil {
			t.Fatal(err)
		}
		if err := json.Compact(&b, after[key]); err != nil {
			t.Fatal(err)
		}
		if a.String() != b.String() {
			t.Fatalf("%s changed: %s != %s", key, b.String(), a.String())
		}
	}
}

func TestExtensionUpdateOwnershipAndFailures(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	put := func(value string) error {
		return s.UpdateExtension(ctx, "product", func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(value), nil })
	}
	if err := put(`{"value":1}`); err != nil {
		t.Fatal(err)
	}
	got, err := s.Extension("product")
	if err != nil {
		t.Fatal(err)
	}
	got[0] = 'x'
	got, _ = s.Extension("product")
	if string(got) != `{"value":1}` {
		t.Fatal("returned bytes alias store")
	}
	if err := put(`{invalid`); err == nil {
		t.Fatal("accepted malformed JSON")
	}
	if err := s.UpdateExtension(ctx, "objects", func(json.RawMessage) (json.RawMessage, error) { return []byte(`[]`), nil }); err == nil {
		t.Fatal("overwrote core section")
	}
	s.syncFile = func(*os.File) error { return errors.New("sync") }
	if err := put(`{"value":2}`); err == nil {
		t.Fatal("missing failure")
	}
	got, _ = s.Extension("product")
	if string(got) != `{"value":1}` {
		t.Fatal("failed update was published")
	}
	s.syncFile = nil
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := put(`{}`); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed write: %v", err)
	}
}
