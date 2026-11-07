package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

func TestSigningPayloadIsStableAcrossResourceOrder(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	a := signedPayloadSnapshot(at, []Resource{
		{Ref: ResourceRef{Tenant: "default", Type: ResourceType("route"), Name: "b"}, Data: []byte("b")},
		{Ref: ResourceRef{Tenant: "default", Type: ResourceType("route"), Name: "a"}, Data: []byte("a")},
	})
	b := signedPayloadSnapshot(at, []Resource{
		{Ref: ResourceRef{Tenant: "default", Type: ResourceType("route"), Name: "a"}, Data: []byte("a")},
		{Ref: ResourceRef{Tenant: "default", Type: ResourceType("route"), Name: "b"}, Data: []byte("b")},
	})

	ap, err := SigningPayload(a)
	if err != nil {
		t.Fatalf("SigningPayload(a): %v", err)
	}
	bp, err := SigningPayload(b)
	if err != nil {
		t.Fatalf("SigningPayload(b): %v", err)
	}
	if !bytes.Equal(ap, bp) {
		t.Fatal("payload changed when resource order changed")
	}
}

func TestSigningPayloadBindsMetadata(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	base := signedPayloadSnapshot(at, nil)
	basePayload, err := SigningPayload(base)
	if err != nil {
		t.Fatalf("SigningPayload(base): %v", err)
	}

	tests := []struct {
		name string
		mut  func(*Snapshot)
	}{
		{name: "tenant", mut: func(s *Snapshot) { s.Tenant = "prod"; s.Revision = CanonicalHash(s) }},
		{name: "node", mut: func(s *Snapshot) { s.NodeID = "node-2"; s.Revision = CanonicalHash(s) }},
		{name: "assignment", mut: func(s *Snapshot) { s.AssignmentID = "assign-2" }},
		{name: "time", mut: func(s *Snapshot) { s.GeneratedAt = s.GeneratedAt.Add(time.Second) }},
		{name: "revision", mut: func(s *Snapshot) {
			s.Resources = append(s.Resources, Resource{Ref: ResourceRef{Tenant: "default", Type: ResourceType("route"), Name: "home"}, Data: []byte("home")})
			s.Revision = CanonicalHash(s)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next := cloneSnapshot(base)
			tt.mut(next)
			payload, err := SigningPayload(next)
			if err != nil {
				t.Fatalf("SigningPayload: %v", err)
			}
			if bytes.Equal(payload, basePayload) {
				t.Fatal("payload did not change")
			}
		})
	}
}

func TestSigningPayloadRejectsTamperedRevision(t *testing.T) {
	t.Parallel()

	snap := signedPayloadSnapshot(time.Now().UTC(), []Resource{{
		Ref:  ResourceRef{Tenant: "default", Type: ResourceType("route"), Name: "home"},
		Data: []byte("home"),
	}})
	snap.Resources[0].Data = []byte("tampered")
	if _, err := SigningPayload(snap); !errors.Is(err, ErrInvalidSigningPayload) {
		t.Fatalf("SigningPayload err=%v want ErrInvalidSigningPayload", err)
	}
}

func signedPayloadSnapshot(at time.Time, resources []Resource) *Snapshot {
	snap := &Snapshot{
		Tenant:       "default",
		NodeID:       "node-1",
		AssignmentID: "assign-1",
		GeneratedAt:  at,
		Resources:    resources,
	}
	snap.Revision = CanonicalHash(snap)
	return snap
}

func cloneSnapshot(in *Snapshot) *Snapshot {
	out := *in
	out.Resources = append([]Resource(nil), in.Resources...)
	return &out
}

func TestSigningPayloadUsesMeshDomain(t *testing.T) {
	payload, err := SigningPayload(signedPayloadSnapshot(time.Now().UTC(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) < 8 {
		t.Fatal("missing domain length")
	}
	size := binary.BigEndian.Uint64(payload[:8])
	if size > uint64(len(payload)-8) {
		t.Fatal("invalid domain length")
	}
	if got := string(payload[8 : 8+size]); got != "fastmesh.snapshot.v1" {
		t.Fatalf("signing domain = %q", got)
	}
}

func TestSentinelMessagesUsePackagePrefix(t *testing.T) {
	for err, want := range map[error]string{ErrMissingSVIDSource: "snapshot: missing svid source"} {
		if err.Error() != want {
			t.Errorf("%q, want %q", err, want)
		}
	}
}
