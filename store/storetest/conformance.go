package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
)

// RunWatchConformance verifies the common WatchObjects contract for stores.
func RunWatchConformance(t *testing.T, open func(t *testing.T) store.WatchableStore) {
	t.Helper()

	t.Run("put-then-event", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := st.WatchObjects(ctx, store.DefaultTenant, "Route")
		if err != nil {
			t.Fatalf("WatchObjects returned error: %v", err)
		}
		written, err := st.PutObject(context.Background(), store.Object{
			Key:  store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Route", Name: "home"},
			Data: []byte(`{"path":"/"}`),
		})
		if err != nil {
			t.Fatalf("PutObject returned error: %v", err)
		}
		ev := recvEvent(t, ch)
		if ev.Type != store.EventPut {
			t.Fatalf("event type=%v want EventPut", ev.Type)
		}
		if ev.Object.Key != written.Key {
			t.Fatalf("event key=%+v want %+v", ev.Object.Key, written.Key)
		}
		if ev.Object.ResourceVersion != written.ResourceVersion || ev.Object.ResourceVersion == 0 {
			t.Fatalf("event rv=%d written rv=%d", ev.Object.ResourceVersion, written.ResourceVersion)
		}
	})

	t.Run("delete-event", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		key := store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Route", Name: "home"}
		written, err := st.PutObject(context.Background(), store.Object{Key: key, Data: []byte(`{"path":"/"}`)})
		if err != nil {
			t.Fatalf("PutObject returned error: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := st.WatchObjects(ctx, "", "")
		if err != nil {
			t.Fatalf("WatchObjects returned error: %v", err)
		}
		if err := st.DeleteObject(context.Background(), key); err != nil {
			t.Fatalf("DeleteObject returned error: %v", err)
		}
		ev := recvEvent(t, ch)
		if ev.Type != store.EventDelete {
			t.Fatalf("event type=%v want EventDelete", ev.Type)
		}
		if ev.Object.Key != key {
			t.Fatalf("event key=%+v want %+v", ev.Object.Key, key)
		}
		if ev.Object.ResourceVersion <= written.ResourceVersion {
			t.Fatalf("delete rv=%d must be greater than put rv=%d", ev.Object.ResourceVersion, written.ResourceVersion)
		}
	})

	t.Run("filter", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := st.WatchObjects(ctx, "prod", "Route")
		if err != nil {
			t.Fatalf("WatchObjects returned error: %v", err)
		}
		if _, err := st.PutObject(context.Background(), store.Object{
			Key:  store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Route", Name: "default-home"},
			Data: []byte(`{"path":"/"}`),
		}); err != nil {
			t.Fatalf("PutObject default route returned error: %v", err)
		}
		assertNoEvent(t, ch)
		if _, err := st.PutObject(context.Background(), store.Object{
			Key:  store.ObjectKey{Tenant: "prod", Kind: "Service", Name: "api"},
			Data: []byte(`{"url":"http://127.0.0.1"}`),
		}); err != nil {
			t.Fatalf("PutObject prod service returned error: %v", err)
		}
		assertNoEvent(t, ch)
		written, err := st.PutObject(context.Background(), store.Object{
			Key:  store.ObjectKey{Tenant: "prod", Kind: "Route", Name: "home"},
			Data: []byte(`{"path":"/"}`),
		})
		if err != nil {
			t.Fatalf("PutObject prod route returned error: %v", err)
		}
		ev := recvEvent(t, ch)
		if ev.Object.Key != written.Key {
			t.Fatalf("event key=%+v want %+v", ev.Object.Key, written.Key)
		}
	})

	// A subscriber that needs several kinds must not have to open a watch per
	// kind: the ADS server holds one of these per connected agent, and the
	// snapshot it builds reads a dozen kinds plus the node registration.
	t.Run("several-kinds", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := st.WatchObjects(ctx, "", "Route", "Service")
		if err != nil {
			t.Fatalf("WatchObjects returned error: %v", err)
		}
		if _, err := st.PutObject(context.Background(), store.Object{
			Key:  store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Middleware", Name: "secure"},
			Data: []byte(`{}`),
		}); err != nil {
			t.Fatalf("PutObject middleware returned error: %v", err)
		}
		assertNoEvent(t, ch)
		for _, kind := range []string{"Route", "Service"} {
			written, err := st.PutObject(context.Background(), store.Object{
				Key:  store.ObjectKey{Tenant: store.DefaultTenant, Kind: kind, Name: "subscribed"},
				Data: []byte(`{}`),
			})
			if err != nil {
				t.Fatalf("PutObject %s returned error: %v", kind, err)
			}
			if ev := recvEvent(t, ch); ev.Object.Key != written.Key {
				t.Fatalf("event key=%+v want %+v", ev.Object.Key, written.Key)
			}
		}
	})

	// No kinds means every kind: a subscriber that has not said what it is
	// about gets what it used to get.
	t.Run("no-kinds-means-every-kind", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := st.WatchObjects(ctx, "")
		if err != nil {
			t.Fatalf("WatchObjects returned error: %v", err)
		}
		written, err := st.PutObject(context.Background(), store.Object{
			Key:  store.ObjectKey{Tenant: store.DefaultTenant, Kind: "Middleware", Name: "secure"},
			Data: []byte(`{}`),
		})
		if err != nil {
			t.Fatalf("PutObject returned error: %v", err)
		}
		if ev := recvEvent(t, ch); ev.Object.Key != written.Key {
			t.Fatalf("event key=%+v want %+v", ev.Object.Key, written.Key)
		}
	})

	t.Run("ctx-cancel", func(t *testing.T) {
		t.Parallel()
		st := open(t)
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := st.WatchObjects(ctx, "", "")
		if err != nil {
			t.Fatalf("WatchObjects returned error: %v", err)
		}
		cancel()
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatal("watch channel remained open after context cancellation")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for watch channel close")
		}
	})
}

func recvEvent(t *testing.T, ch <-chan store.Event) store.Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed before event")
		}
		return ev
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
	}
	return store.Event{}
}

func assertNoEvent(t *testing.T, ch <-chan store.Event) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected watch event: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}
