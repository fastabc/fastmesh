package file

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/store"
)

// Use the public mutations, including overwrite and middle-element deletion:
// a shallow slice rollback can pass create tests while corrupting old values.
func TestMetadataPersistenceFailures(t *testing.T) {
	ctx := context.Background()
	families := []struct {
		name string
		put  func(*Store, string, bool) error
		del  func(*Store, string) error
	}{}
	for _, section := range []string{"tenants", "roles", "cluster_roles", "role_bindings", "cluster_role_bindings", "cross_tenant_references"} {
		families = append(families, struct {
			name string
			put  func(*Store, string, bool) error
			del  func(*Store, string) error
		}{section,
			func(s *Store, name string, updated bool) error {
				return s.UpdateExtension(ctx, section, func(raw json.RawMessage) (json.RawMessage, error) {
					var values map[string]string
					if len(raw) > 0 {
						if err := json.Unmarshal(raw, &values); err != nil {
							return nil, err
						}
					}
					if values == nil {
						values = map[string]string{}
					}
					values[name] = map[bool]string{false: "before", true: "after"}[updated]
					return json.Marshal(values)
				})
			},
			func(s *Store, name string) error {
				return s.UpdateExtension(ctx, section, func(raw json.RawMessage) (json.RawMessage, error) {
					var values map[string]string
					if err := json.Unmarshal(raw, &values); err != nil {
						return nil, err
					}
					delete(values, name)
					return json.Marshal(values)
				})
			}})
	}
	for _, family := range families {
		for _, operation := range []string{"create", "update", "delete"} {
			for _, fault := range []string{"create-file", "file-sync", "rename", "directory-sync"} {
				t.Run(family.name+"/"+operation+"/"+fault, func(t *testing.T) {
					s, err := Open(filepath.Join(t.TempDir(), "state.json"))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = s.Close() })
					for _, name := range []string{"a", "b", "c"} {
						if err := family.put(s, name, false); err != nil {
							t.Fatal(err)
						}
					}
					before := stateJSON(t, s.state)
					restore := metadataFault(t, s, fault)
					var changeErr error
					switch operation {
					case "create":
						changeErr = family.put(s, "d", true)
					case "update":
						changeErr = family.put(s, "b", true)
					case "delete":
						changeErr = family.del(s, "b")
					}
					assertMetadataFailure(t, s, before, fault, changeErr)
					restore()
					// A subsequent successful write must not resurrect a failed mutation.
					if err := s.saveLocked(); err != nil {
						t.Fatal(err)
					}
					disk, err := os.ReadFile(s.path)
					if err != nil {
						t.Fatal(err)
					}
					var saved state
					if err := json.Unmarshal(disk, &saved); err != nil {
						t.Fatal(err)
					}
					if stateJSON(t, saved) != stateJSON(t, s.state) {
						t.Fatal("memory and disk differ")
					}
				})
			}
		}
	}
}

func TestLeasePersistenceFailures(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, operation := range []string{"acquire", "replace", "renew", "release"} {
		for _, fault := range []string{"create-file", "file-sync", "rename", "directory-sync"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				s, err := Open(filepath.Join(t.TempDir(), "state.json"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				var lease store.Lease
				for _, name := range []string{"a", "b", "c"} {
					l, ok, err := s.AcquireLease(ctx, store.LeaseRequest{Name: name, Holder: "owner", TTL: time.Minute, Now: now})
					if err != nil || !ok {
						t.Fatalf("lease: %v %v", ok, err)
					}
					if name == "b" {
						lease = l
					}
				}
				before := stateJSON(t, s.state)
				restore := metadataFault(t, s, fault)
				req := store.LeaseRequest{Name: "b", Holder: "owner", Token: lease.Token, TTL: 2 * time.Minute, Now: now}
				switch operation {
				case "acquire":
					req.Name = "d"
					_, _, err = s.AcquireLease(ctx, req)
				case "replace":
					req.Holder = "new-owner"
					req.Now = now.Add(2 * time.Minute)
					_, _, err = s.AcquireLease(ctx, req)
				case "renew":
					_, _, err = s.RenewLease(ctx, req)
				case "release":
					err = s.ReleaseLease(ctx, req)
				}
				assertMetadataFailure(t, s, before, fault, err)
				restore()
			})
		}
	}
}

func stateJSON(t *testing.T, s state) string {
	t.Helper()
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func metadataFault(t *testing.T, s *Store, fault string) func() {
	t.Helper()
	path := s.path
	switch fault {
	case "create-file":
		blocked := filepath.Join(t.TempDir(), "blocked")
		if err := os.WriteFile(blocked, []byte("blocked"), 0600); err != nil {
			t.Fatal(err)
		}
		s.path = filepath.Join(blocked, "state.json")
	case "file-sync":
		s.syncFile = func(*os.File) error { return errors.New("injected file sync failure") }
	case "rename":
		s.path = t.TempDir()
	case "directory-sync":
		s.syncDirectory = func(string) error { return errors.New("injected directory sync failure") }
	}
	return func() { s.path = path; s.syncDirectory = nil; s.syncFile = nil }
}
func assertMetadataFailure(t *testing.T, s *Store, before, fault string, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected persistence failure")
	}
	if fault == "directory-sync" {
		if !errors.Is(err, store.ErrCommitUncertain) {
			t.Fatalf("expected uncertain commit: %v", err)
		}
		data, err := os.ReadFile(s.path)
		if err != nil {
			t.Fatal(err)
		}
		var disk state
		if err := json.Unmarshal(data, &disk); err != nil {
			t.Fatal(err)
		}
		if stateJSON(t, disk) != stateJSON(t, s.state) || stateJSON(t, s.state) == before {
			t.Fatal("uncertain commit must retain renamed state")
		}
	} else if stateJSON(t, s.state) != before {
		t.Fatal("failed write changed visible state or revision")
	}
}
