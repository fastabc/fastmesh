package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/fastabc/fastmesh/store"
)

func TestObjectSaveFailureRollsBack(t *testing.T) {
	for _, operation := range []string{"create", "update", "delete", "bulk", "transaction"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "store.json")
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			key := store.ObjectKey{Tenant: "default", Kind: "Access", Name: "allow"}
			obj, err := s.PutObject(ctx, store.Object{Key: key, Data: []byte("old")})
			if err != nil {
				t.Fatal(err)
			}
			before, err := s.View(ctx, []store.Range{{}})
			if err != nil {
				t.Fatal(err)
			}
			// A non-empty directory cannot be replaced by a file. The original
			// data remains at path and can be reopened independently.
			blocked := filepath.Join(t.TempDir(), "blocked")
			if err := os.Mkdir(blocked, 0o755); err != nil {
				t.Fatal(err)
			}
			s.path = blocked
			switch operation {
			case "create":
				_, err = s.CreateObject(ctx, store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "Access", Name: "new"}})
			case "update":
				obj.Data = []byte("new")
				_, err = s.PutObject(ctx, obj)
			case "delete":
				err = s.DeleteObject(ctx, key)
			case "transaction":
				obj.Data = []byte("new")
				err = s.ApplyObjects(ctx, []store.ObjectChange{{Object: obj}, {Object: store.Object{Key: store.ObjectKey{Tenant: "default", Kind: "Access", Name: "candidate"}}}})
			case "bulk":
				_, err = s.DeleteObjects(ctx, []store.ObjectKey{key})
			}
			if err == nil || errors.Is(err, store.ErrCommitUncertain) {
				t.Fatalf("expected pre-rename error: %v", err)
			}
			after, err := s.View(ctx, []store.Range{{}})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("failed write mutated memory: before=%+v after=%+v", before, after)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			disk, err := reopened.View(ctx, []store.Range{{}})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, disk) {
				t.Fatal("memory differs from disk")
			}
		})
	}
}

func TestPostRenameFailurePreservesVisibleRevision(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.syncDirectory = func(string) error { return errors.New("injected directory fsync failure") }
	key := store.ObjectKey{Tenant: "default", Kind: "Access", Name: "allow"}
	if _, err := s.PutObject(ctx, store.Object{Key: key}); !errors.Is(err, store.ErrCommitUncertain) {
		t.Fatalf("want uncertain: %v", err)
	}
	before, err := s.View(ctx, []store.Range{{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	after, err := reopened.View(ctx, []store.Range{{}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("renamed state rolled back in memory")
	}
	if _, err := reopened.PutObject(ctx, store.Object{Key: key}); err != nil {
		t.Fatal(err)
	}
	next, err := reopened.View(ctx, nil)
	if err != nil || next.Revision <= before.Revision {
		t.Fatalf("revision did not survive restart: %+v %v", next, err)
	}
}

func TestUncertainTransactionPreservesWholeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "store.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	baseline, err := s.PutObject(ctx, store.Object{Key: store.ObjectKey{Kind: "Profile", Name: "baseline"}})
	if err != nil {
		t.Fatal(err)
	}
	watch, err := s.WatchRevisions(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	s.syncDirectory = func(string) error { return errors.New("injected directory fsync failure") }
	candidate := store.Object{Key: store.ObjectKey{Kind: "Profile", Name: "candidate"}, Data: []byte("new")}
	err = s.ApplyObjects(ctx, []store.ObjectChange{{Object: baseline, Delete: true}, {Object: candidate}})
	if !errors.Is(err, store.ErrCommitUncertain) {
		t.Fatalf("want uncertain: %v", err)
	}
	select {
	case _, ok := <-watch:
		if ok {
			t.Fatal("uncertain transaction published")
		}
	default:
		t.Fatal("uncertain transaction kept watch open")
	}
	objects, err := s.ListObjects(ctx, "", "Profile")
	if err != nil || len(objects) != 1 || objects[0].Key.Name != "candidate" || string(objects[0].Data) != "new" {
		t.Fatalf("partial transaction: %+v %v", objects, err)
	}
	before, err := s.View(ctx, []store.Range{{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	after, err := reopened.View(ctx, []store.Range{{}})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("renamed transaction lost across restart: %v", err)
	}
}
