package fixture

import (
	"testing"

	"github.com/fastabc/fastmesh/policy/state"
)

// OpenState acquires a test-owned state directory and releases it at cleanup.
func OpenState(t testing.TB, dir string) *state.Store {
	t.Helper()
	s, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
