package state

import (
	"errors"
	"testing"

	"github.com/fastabc/fastmesh/model"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

func TestInputRevisionFollowsDurableDecisionAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	st := openState(t, dir)
	snap := &snapshot.Snapshot{Tenant: "default", NodeID: "node", MeshPlan: &model.Plan{InputRevision: 100}}
	if err := st.SaveApplyPending(snap, nil); err != nil {
		t.Fatal(err)
	}
	st = reopenState(t, st, dir)
	if got, err := st.InputRevision(); err != nil || got != 0 {
		t.Fatalf("pending raised watermark: %d %v", got, err)
	}
	if err := st.CommitApply(); err != nil {
		t.Fatal(err)
	}
	st = reopenState(t, st, dir)
	if got, err := st.InputRevision(); err != nil || got != 100 {
		t.Fatalf("committed decision lost: %d %v", got, err)
	}
	journal, err := st.LoadApplyJournal()
	if err != nil || journal.InputRevision != 100 {
		t.Fatalf("journal watermark: %+v %v", journal, err)
	}
	if err := st.CompleteApply(); err != nil {
		t.Fatal(err)
	}
	snap.MeshPlan = &model.Plan{InputRevision: 98}
	if err := st.SaveApplyPending(snap, nil); !errors.Is(err, ErrRevisionRegression) {
		t.Fatalf("rollback accepted: %v", err)
	}
	// A gateway-only checkpoint cannot reset the independent mesh watermark.
	snap.MeshPlan = nil
	if err := st.SaveLastGood(snap, nil); err != nil {
		t.Fatal(err)
	}
	st = reopenState(t, st, dir)
	if got, err := st.InputRevision(); err != nil || got != 100 {
		t.Fatalf("gateway erased watermark: %d %v", got, err)
	}
}
