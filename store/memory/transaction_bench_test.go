package memory

import (
	"context"
	"fmt"
	"github.com/fastabc/fastmesh/store"
	"testing"
)

func BenchmarkSingleObjectTransaction(b *testing.B) {
	for _, count := range []int{10, 1000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s := New()
			ctx := context.Background()
			for i := range count {
				_, err := s.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "item", Name: fmt.Sprint(i)}, Data: []byte(`{"value":1}`)})
				if err != nil {
					b.Fatal(err)
				}
			}
			key := store.ObjectKey{Tenant: "default", Kind: "item", Name: "0"}
			obj, err := s.GetObject(ctx, key)
			if err != nil {
				b.Fatal(err)
			}
			changes := []store.ObjectChange{{Object: obj}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := s.ApplyObjects(ctx, changes); err != nil {
					b.Fatal(err)
				}
				changes[0].Object.ResourceVersion = s.version
			}
		})
	}
}
