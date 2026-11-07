package file

import (
	"path/filepath"
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func TestViewConformance(t *testing.T) {
	storetest.RunViewConformance(t, func(t *testing.T) store.ObjectStore {
		st, err := Open(filepath.Join(t.TempDir(), "store.json"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		return st
	})
}
