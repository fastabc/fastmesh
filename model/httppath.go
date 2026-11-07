package model

import "strings"

// CanonicalRequestPath reports whether path can be authorized without rewriting
// the path seen by the application. Ambiguous paths fail closed for L7 rules.
func CanonicalRequestPath(path, rawPath string) bool {
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "//") || strings.Contains(path, "\\") {
		return false
	}
	for segment := range strings.SplitSeq(path[1:], "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	raw := strings.ToLower(rawPath)
	return !strings.Contains(raw, "%2f") && !strings.Contains(raw, "%2e") && !strings.Contains(raw, "%5c")
}
