package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/policy/apply"
	"github.com/fastabc/fastmesh/policy/state"
)

func TestSecondNodeOnSameStateDirFailsToStart(t *testing.T) {
	client, _, _, ctx := startNodePair(t, false)
	cfg := client.cfg
	second, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Start(ctx); !errors.Is(err, state.ErrLocked) {
		t.Fatalf("second Start: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(ctx); err != nil {
		t.Fatalf("Start after owner closes: %v", err)
	}
}

func TestFailedStartReleasesStateOwnership(t *testing.T) {
	controller, identity, _ := lifecycleIdentities(t)
	cfg := Config{Identity: identity, Controllers: []string{"127.0.0.1:1"}, AllowedControllerIDs: []string{controller.svid.ID.String()}, Tenant: "default", StateDir: t.TempDir(), ConnectTimeout: time.Millisecond}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.Start(ctx); err == nil {
		t.Fatal("Start without enrollment succeeded")
	}
	s, err := state.Open(cfg.StateDir)
	if err != nil {
		t.Fatalf("failed Start leaked lock: %v", err)
	}
	_ = s.Close()
}

func TestComposedNodeDoesNotCloseHostState(t *testing.T) {
	controller, identity, _ := lifecycleIdentities(t)
	dir := t.TempDir()
	st, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	n, err := New(Config{Identity: identity, Controllers: []string{"127.0.0.1:1"}, AllowedControllerIDs: []string{controller.svid.ID.String()}, Tenant: "default", StateDir: dir}, WithProjection(apply.NewPipeline(apply.NewHolder(), st), st, &composedTestProjection{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := state.Open(dir); !errors.Is(err, state.ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("Node released host ownership: %v", err)
	}
}
