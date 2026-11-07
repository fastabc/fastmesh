package model

import (
	"encoding/json"
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func FuzzPrincipal(f *testing.F) {
	for _, body := range []string{`"spiffe://test/orders"`, `{"group":"orders"}`, `null`, `{`,
		`{"id":"spiffe://test/orders","group":"orders"}`, `{"group":"orders","future":"required"}`, `{"group":"bad group"}`} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		// Reusing a destination must not retain an old exact identity when a
		// new group (or null) is decoded into it.
		p := Principal{ID: spiffeid.RequireFromString("spiffe://test/previous")}
		if err := json.Unmarshal(body, &p); err != nil {
			return
		}
		var fresh Principal
		if err := json.Unmarshal(body, &fresh); err != nil || p != fresh {
			t.Fatal("principal inherited authority from previous value")
		}
		if !p.Valid() {
			return
		}
		encoded, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var roundtrip Principal
		if err := json.Unmarshal(encoded, &roundtrip); err != nil || roundtrip != p || !roundtrip.Valid() {
			t.Fatal("valid principal changed identity during roundtrip")
		}
	})
}
