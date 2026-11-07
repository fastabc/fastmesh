package file

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
)

func TestStoreOwnershipAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if other, err := Open(path); !errors.Is(err, ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("second owner: %v", err)
	}
	ctx := context.Background()
	initial, err := s.WatchObjects(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	revisions, err := s.WatchRevisions(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-initial:
		if ok {
			t.Fatal("initial watch open")
		}
	default:
		t.Fatal("initial watch not closed")
	}
	select {
	case _, ok := <-revisions:
		if ok {
			t.Fatal("revision watch open")
		}
	default:
		t.Fatal("revision watch not closed")
	}
	if _, err := s.WatchObjects(ctx, ""); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed watch: %v", err)
	}
	if _, err := s.WatchRevisions(ctx, ""); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed revision watch: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := s.PutObject(ctx, store.Object{Key: store.ObjectKey{Kind: "test", Name: "ghost"}}); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed object write: %v", err)
	}
	if _, err := reopened.GetObject(ctx, store.ObjectKey{Tenant: "default", Kind: "test", Name: "ghost"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("closed owner mutated successor: %v", err)
	}
}

func TestStoreOwnershipProcess(t *testing.T) {
	if mode := os.Getenv("FASTMESH_FILE_LOCK_TEST_MODE"); mode != "" {
		s, err := Open(os.Getenv("FASTMESH_FILE_LOCK_TEST_PATH"))
		if mode == "busy" {
			if s != nil {
				_ = s.Close()
			}
			if !errors.Is(err, ErrLocked) {
				t.Fatalf("expected lock rejection: %v", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		fmt.Println("locked")
		// The parent kills this process to exercise OS-owned lock release.
		for {
			time.Sleep(time.Hour)
		}
	}
	path := filepath.Join(t.TempDir(), "state.json")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	child := func(mode string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStoreOwnershipProcess$")
		cmd.Env = append(os.Environ(), "FASTMESH_FILE_LOCK_TEST_MODE="+mode, "FASTMESH_FILE_LOCK_TEST_PATH="+path)
		return cmd
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if out, err := child("busy").CombinedOutput(); err != nil {
		t.Fatalf("second process: %v\n%s", err, out)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := child("hold")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	scanner := bufio.NewScanner(out)
	if !scanner.Scan() || scanner.Text() != "locked" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("child did not acquire ownership")
	}
	if other, err := Open(path); !errors.Is(err, ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("child lock: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("lock survived process exit: %v", err)
	}
	_ = reopened.Close()
}
