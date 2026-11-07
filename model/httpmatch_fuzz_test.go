package model

import (
	"path"
	"strings"
	"testing"
)

func FuzzHTTPMatchPrefixIsCleanStable(f *testing.F) {
	for _, s := range []string{"/public/a", "/public/../x", "/public//x", "/publicx"} {
		f.Add(s)
	}
	m := HTTPMatch{PathPrefix: "/public/"}
	f.Fuzz(func(t *testing.T, p string) {
		if CanonicalRequestPath(p, "") && m.Matches("GET", p) && !strings.HasPrefix(path.Clean(p)+"/", "/public/") {
			t.Fatalf("%q matched but cleans outside the prefix", p)
		}
	})
}
