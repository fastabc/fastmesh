package model

import (
	"encoding/json"
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestPrincipalAndExtendedL7Validation(t *testing.T) {
	id := spiffeid.RequireFromString("spiffe://test/orders")
	for _, p := range []Principal{{}, {ID: id, Group: "admins"}, {Group: "*"}} {
		if p.Valid() {
			t.Fatalf("invalid principal accepted: %+v", p)
		}
	}
	for _, p := range []Principal{{ID: id}, {Group: "orders"}} {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var round Principal
		if err = json.Unmarshal(b, &round); err != nil || p != round {
			t.Fatalf("roundtrip %s: %+v %v", b, round, err)
		}
	}
	var p Principal
	if err := json.Unmarshal([]byte(`{"id":"spiffe://test/orders","future_constraint":true}`), &p); err == nil {
		t.Fatal("unknown principal constraint ignored")
	}
	match := HTTPMatch{Methods: []string{"GET", "POST"}, PathPrefix: "/pay/"}
	if !match.Matches("GET", "/pay/one") || match.Matches("DELETE", "/pay/one") || match.Matches("GET", "/payments") {
		t.Fatal("expanded match broadened access")
	}
	for _, m := range []HTTPMatch{{Method: "GET", Methods: []string{"POST"}}, {Path: "/pay", PathPrefix: "/"}, {Methods: []string{""}}} {
		if m.Valid() {
			t.Fatalf("ambiguous predicate accepted: %+v", m)
		}
	}
}
