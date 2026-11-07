package file

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func TestWatchConformance(t *testing.T) {
	storetest.RunWatchConformance(t, func(t *testing.T) store.WatchableStore {
		t.Helper()
		s, err := Open(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatalf("Open returned error: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestObjectVersionConformance(t *testing.T) {
	storetest.RunObjectVersionConformance(t, func(t *testing.T) store.ObjectStore {
		t.Helper()
		s, err := Open(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestStorePersistsObjectsAcrossOpen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	key := store.ObjectKey{Tenant: "default", Kind: "Route", Name: "checkout"}
	written, err := s.PutObject(context.Background(), store.Object{
		Key:    key,
		Data:   []byte(`{"path":"/checkout"}`),
		Labels: map[string]string{"app": "shop"},
	})
	if err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}
	if written.ResourceVersion == 0 {
		t.Fatal("ResourceVersion=0 want non-zero")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.GetObject(context.Background(), key)
	if err != nil {
		t.Fatalf("GetObject returned error: %v", err)
	}
	if string(got.Data) != `{"path":"/checkout"}` || got.Labels["app"] != "shop" {
		t.Fatalf("unexpected object: %+v", got)
	}
}

// The whole state is re-encoded and fsynced on every write, so what it costs to
// encode is paid per write. Indentation formats a file nothing reads for a
// human, and formatting it is not free: it is bytes to write and time to
// produce them, on the hot path of every store write.
func TestSavedStateIsCompactJSON(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.PutObject(context.Background(), store.Object{
		Key:  store.ObjectKey{Tenant: "default", Kind: "Route", Name: "checkout"},
		Data: []byte(`{"path":"/checkout"}`),
	}); err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}

	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// One JSON value, one trailing newline from the encoder.
	if lines := bytes.Count(saved, []byte("\n")); lines != 1 {
		t.Errorf("saved state spans %d lines, want one compact value", lines)
	}
	if !json.Valid(saved) {
		t.Error("saved state is not valid JSON")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
}

func TestOpenDoesNotMigrateNamespaces(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	raw, err := json.Marshal(map[string]any{
		"version": 1,
		"objects": []store.Object{{
			Key:  store.ObjectKey{Kind: "route", Name: "home"},
			Data: []byte(`{"path":"/"}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	objs, err := s.ListObjects(context.Background(), store.DefaultTenant, "route")
	if err != nil {
		t.Fatalf("ListObjects returned error: %v", err)
	}
	if len(objs) != 0 {
		t.Fatalf("objects=%+v, unexpected namespace migration", objs)
	}
}

func TestStorePersistsLeaseAcrossOpen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	lease, ok, err := s.AcquireLease(context.Background(), store.LeaseRequest{
		Name:   "controller",
		Holder: "a",
		TTL:    time.Minute,
		Now:    now,
	})
	if err != nil || !ok {
		t.Fatalf("AcquireLease ok=%v err=%v, want acquired", ok, err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.GetLease(context.Background(), "controller")
	if err != nil {
		t.Fatalf("GetLease returned error: %v", err)
	}
	if got.Token != lease.Token || got.Holder != "a" {
		t.Fatalf("lease=%+v want token=%d holder=a", got, lease.Token)
	}
}

func TestOpenRejectsMissingPath(t *testing.T) {
	t.Parallel()

	if _, err := Open(""); !errors.Is(err, ErrMissingPath) {
		t.Fatalf("Open empty err=%v want ErrMissingPath", err)
	}
}

func TestExtensionConformance(t *testing.T) {
	storetest.RunExtensionConformance(t, func(t *testing.T) storetest.ExtensionBackend {
		s, err := Open(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
