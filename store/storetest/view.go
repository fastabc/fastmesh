package storetest

import (
	"context"
	"fmt"
	"testing"

	"github.com/fastabc/fastmesh/store"
)

// RunViewConformance tests isolation, monotonic revisions and a concurrent
// revoke-then-register sequence that must never expose new data with old access.
func RunViewConformance(t *testing.T, open func(*testing.T) store.ObjectStore) {
	t.Helper()
	st := open(t)
	v, ok := st.(store.Viewer)
	if !ok {
		t.Fatalf("%T does not implement Viewer", st)
	}
	ctx := context.Background()
	ranges := []store.Range{{Tenant: "default", Kind: "Access"}, {Tenant: "default", Kind: "Endpoint"}}
	key := store.ObjectKey{Tenant: "default", Kind: "Access", Name: "allow"}
	put := func(key store.ObjectKey, data string) {
		t.Helper()
		if _, err := st.PutObject(ctx, store.Object{Key: key, Data: []byte(data), Labels: map[string]string{"test": "original"}}); err != nil {
			t.Fatal(err)
		}
	}
	put(key, "allow")
	before, err := v.View(ctx, ranges)
	if err != nil {
		t.Fatal(err)
	}
	before.Objects[ranges[0]][0].Data[0] = 'X'
	before.Objects[ranges[0]][0].Labels["test"] = "modified"
	obj, err := st.GetObject(ctx, key)
	if err != nil || string(obj.Data) != "allow" || obj.Labels["test"] != "original" {
		t.Fatalf("view aliases store: %+v, %v", obj, err)
	}
	done := make(chan error, 1)
	go func() {
		if err := st.DeleteObject(ctx, key); err != nil {
			done <- err
			return
		}
		_, err := st.PutObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "Endpoint", Name: "new"}, Data: []byte("new")})
		done <- err
	}()
	last := before.Revision
	for i := 0; i < 100; i++ {
		view, err := v.View(ctx, ranges)
		if err != nil {
			t.Fatal(err)
		}
		if view.Revision < last {
			t.Fatal("revision regressed")
		}
		last = view.Revision
		if len(view.Objects[ranges[1]]) > 0 && len(view.Objects[ranges[0]]) > 0 {
			t.Fatal("mixed revisions: new endpoint with revoked access")
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	after, err := v.View(ctx, ranges)
	if err != nil || after.Revision <= before.Revision {
		t.Fatalf("revision did not advance: %+v, %v", after, err)
	}
	if len(after.Objects[ranges[0]]) != 0 || len(after.Objects[ranges[1]]) != 1 {
		t.Fatalf("unexpected final view: %+v", after)
	}
	for _, r := range ranges {
		if _, ok := after.Objects[r]; !ok {
			t.Fatal("empty loaded range omitted")
		}
	}
	empty, err := v.View(ctx, nil)
	if err != nil || empty.Revision < after.Revision {
		t.Fatal(fmt.Sprintf("empty view: %+v, %v", empty, err))
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := v.View(cancelled, ranges); err == nil {
		t.Fatal("cancelled view succeeded")
	}
}
