package file

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fastabc/fastmesh/store"
)

// BenchmarkPutObjectAtScale characterises what one write costs as the store
// grows. Every mutation re-encodes the whole state file and fsyncs it, so the
// question this answers is which of the two dominates.
//
// Measured on an M-series laptop (SSD): 10 objects 6.8ms, 100 objects 7.8ms,
// 1000 objects 8.4ms. The fsync floor is ~6.8ms and the marginal cost of an
// object is ~1.5us, so object count is not what makes writes slow at any
// realistic single-node scale - durability is. Read that before optimising the
// serialisation: the win is not there.
//
// What does scale badly is bulk loading, since each of N objects pays its own
// fsync. A provider seeding thousands of objects at startup is the case to
// watch, and the fix for it would be a batch write, not a faster encoder.
func BenchmarkPutObjectAtScale(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("objects=%d", n), func(b *testing.B) {
			s := benchStore(b, n)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.PutObject(ctx, store.Object{
					Key:  store.ObjectKey{Tenant: "default", Kind: "Route", Name: "hot"},
					Data: []byte(`{"apiVersion":"example/v1alpha1","kind":"Route","spec":{"host":"b.example","path":"/"}}`),
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchStore(b *testing.B, objects int) *Store {
	b.Helper()
	s, err := Open(filepath.Join(b.TempDir(), "store.json"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	for i := 0; i < objects; i++ {
		if _, err := s.PutObject(ctx, store.Object{
			Key:  store.ObjectKey{Tenant: "default", Kind: "Route", Name: fmt.Sprintf("r-%04d", i)},
			Data: []byte(`{"apiVersion":"example/v1alpha1","kind":"Route","spec":{"host":"a.example","path":"/"}}`),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return s
}
