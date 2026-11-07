package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"
)

var ErrInvalidSigningPayload = errors.New("snapshot: invalid signing payload")

// Signature carries detached snapshot authenticity metadata.
type Signature struct {
	Algorithm string   `json:"algorithm"`
	KeyID     string   `json:"key_id,omitempty"`
	Value     []byte   `json:"value"`
	CertChain [][]byte `json:"cert_chain,omitempty"`
}

// SigningPayload returns the stable bytes a controller signs for a full
// runtime snapshot. The canonical content hash remains the resource-content
// authority; this payload binds that revision to the destination metadata.
func SigningPayload(snap *Snapshot) ([]byte, error) {
	if snap == nil || snap.MeshPlan != nil || snap.MeshPlanEnvelope != nil {
		return nil, ErrInvalidSigningPayload
	}
	canonical := CanonicalHash(snap)
	if snap.Revision != canonical {
		return nil, errors.Join(ErrInvalidSigningPayload, errors.New("revision does not match canonical hash"))
	}
	var buf bytes.Buffer
	appendPayloadField(&buf, "fastmesh.snapshot.v1")
	appendPayloadField(&buf, snap.Tenant)
	appendPayloadField(&buf, snap.NodeID)
	appendPayloadField(&buf, snap.AssignmentID)
	appendPayloadField(&buf, string(snap.Revision))
	appendPayloadField(&buf, snap.GeneratedAt.UTC().Format(time.RFC3339Nano))
	appendPayloadField(&buf, string(canonical))
	return buf.Bytes(), nil
}

func appendPayloadField(buf *bytes.Buffer, value string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(value)))
	_, _ = buf.Write(n[:])
	_, _ = buf.WriteString(value)
}
