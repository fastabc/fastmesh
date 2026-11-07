package file

import (
	"path/filepath"
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func transactionBackend(t *testing.T) store.ObjectStore {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func TestTransactionConformance(t *testing.T) {
	storetest.RunTransactionConformance(t, transactionBackend)
}
func TestConcurrentTransactionView(t *testing.T) {
	storetest.RunConcurrentTransactionView(t, transactionBackend)
}
