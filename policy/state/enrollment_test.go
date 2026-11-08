package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnrollmentRequestSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.EnrollmentRequestID("secret-token", "spiffe://test/app")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	again, err := s.EnrollmentRequestID("secret-token", "spiffe://test/app")
	if err != nil || id != again {
		t.Fatalf("restart: %q %v", again, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "enrollment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-token") {
		t.Fatal("raw token persisted")
	}
	next, err := s.EnrollmentRequestID("new-token", "spiffe://test/app")
	if err != nil || id == next {
		t.Fatalf("new token: %q %v", next, err)
	}
	nextPeer, err := s.EnrollmentRequestID("new-token", "spiffe://test/other")
	if err != nil || next == nextPeer {
		t.Fatalf("new peer: %q %v", nextPeer, err)
	}
}
