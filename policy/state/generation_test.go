package state

import "testing"

func TestMeshGenerationSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	for want := uint64(1); want <= 3; want++ {
		st := openState(t, dir)
		got, err := st.NextMeshGeneration()
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		if err != nil || got != want {
			t.Fatalf("generation=%d want=%d err=%v", got, want, err)
		}
	}
}
