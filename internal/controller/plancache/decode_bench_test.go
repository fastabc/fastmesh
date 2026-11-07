package plancache

import (
	"fmt"
	"testing"

	"github.com/fastabc/fastmesh/internal/controller/catalog"
)

func BenchmarkCatalogDecode(b *testing.B) {
	for _, services := range []int{100, 1000} {
		b.Run(fmt.Sprint(services), func(b *testing.B) {
			_, snapshots := newCacheFixture(b, 1, services)
			v := snapshots[0].View()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := catalog.Decode(v, snapshots[0].Tenant); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
