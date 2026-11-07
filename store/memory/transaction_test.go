package memory

import (
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func transactionBackend(t *testing.T) store.ObjectStore { return New() }
func TestTransactionConformance(t *testing.T) {
	storetest.RunTransactionConformance(t, transactionBackend)
}
func TestConcurrentTransactionView(t *testing.T) {
	storetest.RunConcurrentTransactionView(t, transactionBackend)
}
