package state

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestOpenIsExclusive(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if other, err := Open(dir); !errors.Is(err, ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("second Open: %v", err)
	}
	seen := map[uint64]bool{}
	for range 200 {
		g, err := a.NextMeshGeneration()
		if err != nil || seen[g] {
			t.Fatalf("generation=%d err=%v", g, err)
		}
		seen[g] = true
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.SaveNodeID("closed"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed write: %v", err)
	}
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if g, err := b.NextMeshGeneration(); err != nil || g != 201 {
		t.Fatalf("reopen generation=%d err=%v", g, err)
	}
}

func TestOpenRecoversAfterOwnerProcessDies(t *testing.T) {
	if dir := os.Getenv("FASTMESH_STATE_LOCK_TEST_DIR"); dir != "" {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		fmt.Println("locked")
		for {
			time.Sleep(time.Hour)
		}
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOpenRecoversAfterOwnerProcessDies$")
	cmd.Env = append(os.Environ(), "FASTMESH_STATE_LOCK_TEST_DIR="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(out)
	if !scanner.Scan() || scanner.Text() != "locked" {
		t.Fatal("child did not acquire lock")
	}
	if other, err := Open(dir); !errors.Is(err, ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("child ownership: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("lock survived process death: %v", err)
	}
	_ = s.Close()
}
