package snapshot

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"sort"
)

// Key returns the canonical identity string for a resource reference. It is
// stable across processes and used for sorting, indexing, and delta merges.
func (r ResourceRef) Key() string {
	return string(r.Type) + "/" + r.Tenant + "/" + r.Name
}

// CanonicalHash returns the content revision of a full snapshot. Revision and
// GeneratedAt are deliberately excluded so the hash is a pure function of the
// desired content: two builds of the same desired state hash identically.
func CanonicalHash(snap *Snapshot) Revision {
	h := sha256.New()
	if snap == nil {
		return Revision("sha256:" + hex.EncodeToString(h.Sum(nil)))
	}
	writeField(h, snap.Tenant)
	writeField(h, snap.NodeID)

	sorted := make([]Resource, len(snap.Resources))
	copy(sorted, snap.Resources)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Ref.Key() < sorted[j].Ref.Key() })
	for i := range sorted {
		hashResource(h, sorted[i])
	}
	return Revision("sha256:" + hex.EncodeToString(h.Sum(nil)))
}

func hashResource(h hash.Hash, r Resource) {
	writeField(h, r.Ref.Key())
	writeField(h, r.Version)
	writeBytes(h, r.Data)

	keys := make([]string, 0, len(r.Metadata))
	for k := range r.Metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		writeField(h, k)
		writeField(h, r.Metadata[k])
	}
}

func writeField(h hash.Hash, s string) { writeBytes(h, []byte(s)) }

// writeBytes writes a length prefix before the payload so that adjacent fields
// cannot collide (e.g. "ab"+"c" must differ from "a"+"bc").
func writeBytes(h hash.Hash, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}
