package memory

import (
	"testing"

	"github.com/fastabc/fastmesh/store"
	"github.com/fastabc/fastmesh/store/storetest"
)

func TestViewConformance(t *testing.T) {
	storetest.RunViewConformance(t, func(t *testing.T) store.ObjectStore { return New() })
}
