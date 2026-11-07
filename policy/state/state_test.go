package state

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/fastabc/fastmesh/policy/material"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

func sampleSnapshot() *snapshot.Snapshot {
	return &snapshot.Snapshot{
		Tenant:   "default",
		NodeID:   "node-1",
		Revision: "sha256:abc",
		Resources: []snapshot.Resource{
			{Ref: snapshot.ResourceRef{Tenant: "default", Type: snapshot.ResourceType("route"), Name: "home"}, Data: []byte(`{"path":"/"}`)},
		},
	}
}

func TestSaveLoadSnapshotRoundTrip(t *testing.T) {
	s := openState(t, t.TempDir())
	want := sampleSnapshot()
	if err := s.SaveLastGood(want, nil); err != nil {
		t.Fatalf("SaveLastGood: %v", err)
	}
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got.Revision != want.Revision || got.NodeID != want.NodeID {
		t.Errorf("loaded = %+v, want revision %q node %q", got, want.Revision, want.NodeID)
	}
	if len(got.Resources) != 1 || got.Resources[0].Ref.Name != "home" {
		t.Errorf("loaded resources = %+v, want one route 'home'", got.Resources)
	}
}

func TestLoadSnapshotMissingReturnsNotFound(t *testing.T) {
	s := openState(t, t.TempDir())
	_, err := s.LoadSnapshot()
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadSnapshot on empty dir = %v, want ErrNotFound", err)
	}
}

func TestSaveLastGoodOverwrites(t *testing.T) {
	s := openState(t, t.TempDir())
	if err := s.SaveLastGood(sampleSnapshot(), nil); err != nil {
		t.Fatalf("first save: %v", err)
	}
	second := sampleSnapshot()
	second.Revision = "sha256:def"
	if err := s.SaveLastGood(second, nil); err != nil {
		t.Fatalf("second save: %v", err)
	}
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got.Revision != "sha256:def" {
		t.Errorf("revision = %q, want sha256:def", got.Revision)
	}
}

func TestApplyJournalSelectsOnlyCommittedTargetAsLastGood(t *testing.T) {
	dir := t.TempDir()
	s := openState(t, dir)
	old := sampleSnapshot()
	old.Revision = "sha256:old"
	target := sampleSnapshot()
	target.Revision = "sha256:target"
	if err := s.SaveLastGood(old, nil); err != nil {
		t.Fatalf("SaveLastGood(old): %v", err)
	}
	if err := s.SaveApplyPending(target, nil); err != nil {
		t.Fatalf("SaveApplyPending: %v", err)
	}

	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot(pending): %v", err)
	}
	if got.Revision != old.Revision {
		t.Fatalf("pending journal selected revision %q, want old last-good %q", got.Revision, old.Revision)
	}
	journal, err := s.LoadApplyJournal()
	if err != nil {
		t.Fatalf("LoadApplyJournal(pending): %v", err)
	}
	if journal.Decision != ApplyDecisionPending || journal.Snapshot.Revision != target.Revision {
		t.Fatalf("pending journal = %+v, want target %q", journal, target.Revision)
	}

	if err := s.CommitApply(); err != nil {
		t.Fatalf("CommitApply: %v", err)
	}
	got, err = s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot(committed): %v", err)
	}
	if got.Revision != target.Revision {
		t.Fatalf("committed journal selected revision %q, want target %q", got.Revision, target.Revision)
	}
	journal, err = s.LoadApplyJournal()
	if err != nil {
		t.Fatalf("LoadApplyJournal(committed): %v", err)
	}
	if journal.Decision != ApplyDecisionCommitted {
		t.Fatalf("journal decision = %q, want committed", journal.Decision)
	}
	if err := s.CompleteApply(); err != nil {
		t.Fatalf("CompleteApply: %v", err)
	}
	if _, err := s.LoadApplyJournal(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadApplyJournal after completion = %v, want ErrNotFound", err)
	}
	got, err = s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot(completed): %v", err)
	}
	if got.Revision != target.Revision {
		t.Fatalf("completed last-good revision = %q, want %q", got.Revision, target.Revision)
	}
}

func TestClearPendingApplyJournalPreservesOldLastGood(t *testing.T) {
	s := openState(t, t.TempDir())
	old := sampleSnapshot()
	old.Revision = "sha256:old"
	target := sampleSnapshot()
	target.Revision = "sha256:target"
	if err := s.SaveLastGood(old, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveApplyPending(target, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearApplyJournal(); err != nil {
		t.Fatalf("ClearApplyJournal: %v", err)
	}
	if _, err := s.LoadApplyJournal(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadApplyJournal after clear = %v, want ErrNotFound", err)
	}
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != old.Revision {
		t.Fatalf("last-good revision = %q, want %q", got.Revision, old.Revision)
	}
}

func TestSaveLoadNodeID(t *testing.T) {
	s := openState(t, t.TempDir())
	if err := s.SaveNodeID("node-xyz"); err != nil {
		t.Fatalf("SaveNodeID: %v", err)
	}
	got, err := s.LoadNodeID()
	if err != nil {
		t.Fatalf("LoadNodeID: %v", err)
	}
	if got != "node-xyz" {
		t.Errorf("node id = %q, want node-xyz", got)
	}
}

func TestSaveLoadPolicyContact(t *testing.T) {
	s := openState(t, t.TempDir())
	want := time.Date(2026, 7, 26, 1, 2, 3, 456, time.UTC)
	if err := s.SavePolicyContact(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadPolicyContact()
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("contact=%s want %s", got, want)
	}
}

func TestSaveLoadControllerEndpoints(t *testing.T) {
	s := openState(t, t.TempDir())
	if err := s.SaveControllerEndpoints([]string{" controller-a:7443 ", "controller-b:7443", "controller-a:7443"}); err != nil {
		t.Fatalf("SaveControllerEndpoints: %v", err)
	}
	got, err := s.LoadControllerEndpoints()
	if err != nil {
		t.Fatalf("LoadControllerEndpoints: %v", err)
	}
	want := []string{"controller-a:7443", "controller-b:7443"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoints=%v want %v", got, want)
	}
	if err := s.SaveControllerEndpoints(nil); err == nil {
		t.Fatal("SaveControllerEndpoints accepted an empty set")
	}
}

func TestLoadNodeIDMissingReturnsNotFound(t *testing.T) {
	s := openState(t, t.TempDir())
	_, err := s.LoadNodeID()
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadNodeID on empty dir = %v, want ErrNotFound", err)
	}
}

func TestSaveLoadLastGoodWithEncryptedMaterials(t *testing.T) {
	dir := t.TempDir()
	s := openState(t, dir)
	want := []material.ResolvedMaterial{{
		Ref:       material.SecretRef{Provider: "file", Tenant: "default", Name: "tls-key"},
		Plaintext: material.Plaintext{Value: []byte("super-secret-private-key"), ContentType: "application/x-pem-file"},
	}}
	if err := s.SaveLastGood(sampleSnapshot(), want); err != nil {
		t.Fatalf("SaveLastGood: %v", err)
	}
	snap, got, err := s.LoadLastGood()
	if err != nil {
		t.Fatalf("LoadLastGood: %v", err)
	}
	if snap.Revision != sampleSnapshot().Revision || len(got) != 1 {
		t.Fatalf("loaded snapshot/materials = (%+v, %+v)", snap, got)
	}
	if string(got[0].Plaintext.Value) != "super-secret-private-key" || got[0].Ref.Name != "tls-key" {
		t.Fatalf("material = %+v", got[0])
	}

	raw, err := os.ReadFile(filepath.Join(dir, snapshotFile))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("super-secret-private-key")) {
		t.Fatal("last-good file contains plaintext material")
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	_ = gz.Close()
	if bytes.Contains(decoded, []byte("super-secret-private-key")) {
		t.Fatal("decompressed last-good envelope contains plaintext material")
	}
	assertMode(t, filepath.Join(dir, snapshotFile), 0o600)
	assertMode(t, filepath.Join(dir, materialKeyFile), 0o600)
	assertMode(t, dir, 0o700)
}

func TestLoadLastGoodRejectsChangedCacheKey(t *testing.T) {
	dir := t.TempDir()
	s := openState(t, dir)
	materials := []material.ResolvedMaterial{{Ref: material.SecretRef{Provider: "file", Name: "token"}, Plaintext: material.Plaintext{Value: []byte("secret")}}}
	if err := s.SaveLastGood(sampleSnapshot(), materials); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, materialKeyFile), bytes.Repeat([]byte{0x42}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.LoadLastGood(); err == nil {
		t.Fatal("LoadLastGood with changed key = nil, want authentication error")
	}
}

func TestLoadSnapshotRejectsUnversionedSnapshot(t *testing.T) {
	dir := t.TempDir()
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if err := json.NewEncoder(gz).Encode(sampleSnapshot()); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotFile), compressed.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openState(t, dir).LoadLastGood(); err == nil {
		t.Fatal("accepted unversioned snapshot")
	}
}

func TestSaveLastGoodRejectsNilSnapshot(t *testing.T) {
	if err := openState(t, t.TempDir()).SaveLastGood(nil, nil); err == nil {
		t.Fatal("SaveLastGood(nil) = nil, want error")
	}
}

func TestLoadLastGoodRejectsEnvelopeWithoutSnapshot(t *testing.T) {
	dir := t.TempDir()
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write([]byte(`{"version":1,"snapshot":null}`)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotFile), compressed.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openState(t, dir).LoadLastGood(); err == nil {
		t.Fatal("LoadLastGood malformed envelope = nil, want error")
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode=%#o want %#o", path, got, want)
	}
}

func TestSentinelMessagesUsePackagePrefix(t *testing.T) {
	for err, want := range map[error]string{ErrNotFound: "state: not found", ErrCommitDecisionUncertain: "state: commit decision durability is uncertain", ErrRevisionRegression: "state: mesh input revision regressed"} {
		if err.Error() != want {
			t.Errorf("%q, want %q", err, want)
		}
	}
}
