package enrollment

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	filestore "github.com/fastabc/fastmesh/store/file"
	"github.com/fastabc/fastmesh/store/memory"
)

func TestManagerCreateValidateRevokeWithMemoryStore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	m := New(memory.New(), WithClock(func() time.Time { return now }))

	created, err := m.Create(context.Background(), CreateRequest{Tenant: "default", TTL: time.Hour})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if !strings.HasPrefix(created.Raw, "fgjt.") {
		t.Fatalf("raw token %q missing prefix", created.Raw)
	}
	validated, err := m.Validate(context.Background(), created.Raw)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if validated.ID != created.Token.ID {
		t.Fatalf("validated id=%q want %q", validated.ID, created.Token.ID)
	}
	if err := m.Revoke(context.Background(), created.Token.ID); err != nil {
		t.Fatalf("Revoke returned error: %v", err)
	}
	if _, err := m.Validate(context.Background(), created.Raw); !errors.Is(err, ErrRevokedToken) {
		t.Fatalf("Validate revoked err=%v want ErrRevokedToken", err)
	}
}

func TestManagerRejectsExpiredToken(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	m := New(memory.New(), WithClock(func() time.Time { return now }))
	created, err := m.Create(context.Background(), CreateRequest{TTL: time.Second})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	now = now.Add(2 * time.Second)
	if _, err := m.Validate(context.Background(), created.Raw); !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("Validate expired err=%v want ErrExpiredToken", err)
	}
}

func TestManagerDoesNotStoreRawSecret(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	backend, err := filestore.Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	m := New(backend)
	created, err := m.Create(context.Background(), CreateRequest{})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	parts := strings.Split(created.Raw, ".")
	if len(parts) != 3 {
		t.Fatalf("raw token has unexpected format: %q", created.Raw)
	}
	if bytes.Contains(onDisk, []byte(parts[2])) {
		t.Fatal("file store contains raw token secret")
	}
}

func TestManagerPersistsTokensWithFileStore(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	backend, err := filestore.Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	created, err := New(backend).Create(context.Background(), CreateRequest{Description: "agent join"})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := filestore.Open(path)
	if err != nil {
		t.Fatalf("reopen returned error: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := New(reopened).Get(context.Background(), created.Token.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Description != "agent join" {
		t.Fatalf("description=%q want agent join", got.Description)
	}
}
