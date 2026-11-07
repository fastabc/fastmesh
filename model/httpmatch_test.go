package model

import "testing"

func TestCanonicalRequestPath(t *testing.T) {
	for _, c := range []struct {
		path, raw string
		ok        bool
	}{
		{"/", "", true}, {"/public/a", "", true}, {"/public/", "", true},
		{"/public/../admin", "", false}, {"/public/./a", "", false},
		{"/public//x", "", false}, {"public/a", "", false}, {"", "", false},
		{"/public/../admin", "/public/%2e%2e/admin", false},
		{"/public/../admin", "/public/%2E%2e/admin", false},
		{"/a/b", "/a%2Fb", false}, {"/a\\b", "/a%5cb", false},
		{"/a.txt", "/a%2Etxt", false},
	} {
		if got := CanonicalRequestPath(c.path, c.raw); got != c.ok {
			t.Errorf("CanonicalRequestPath(%q,%q)=%v want %v", c.path, c.raw, got, c.ok)
		}
	}
}

func TestPathPrefixUsesSegmentBoundary(t *testing.T) {
	bare := HTTPMatch{Method: "GET", PathPrefix: "/api"}
	slash := HTTPMatch{Method: "GET", PathPrefix: "/api/"}
	for path, want := range map[string][2]bool{
		"/api": {true, false}, "/api/": {true, true}, "/api/v1": {true, true},
		"/apiv2": {false, false}, "/api-admin": {false, false},
	} {
		if got := bare.Matches("GET", path); got != want[0] {
			t.Errorf("bare %q=%v want %v", path, got, want[0])
		}
		if got := slash.Matches("GET", path); got != want[1] {
			t.Errorf("slash %q=%v want %v", path, got, want[1])
		}
	}
}

func TestHTTPMatchRejectsDotSegmentRuleValues(t *testing.T) {
	for _, m := range []HTTPMatch{{PathPrefix: "/a/../b"}, {Path: "/a/./b"}, {PathPrefix: "/a//b"}} {
		if m.Valid() {
			t.Errorf("%+v must be invalid", m)
		}
	}
}
