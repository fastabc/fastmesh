package release

import (
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Validate the documentation actually shipped with this baseline, including
// example guides. Historical/local documentation trees are outside the release.
func TestDocumentationLinks(t *testing.T) {
	root := "../.."
	paths := []string{"README.md", "README.zh.md", "CHANGELOG.md"}
	for _, tree := range []string{"docs", "api", "examples"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "archive" || d.Name() == "superpowers" || d.Name() == "plans" || d.Name() == "reviews") {
				return filepath.SkipDir
			}
			if !d.IsDir() && strings.HasSuffix(path, ".md") {
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				paths = append(paths, rel)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	links := regexp.MustCompile(`\[[^\]]*\]\(([^ )]+)(?:[^)]*)\)`)
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range links.FindAllStringSubmatch(string(raw), -1) {
			target, err := url.Parse(m[1])
			if err != nil {
				t.Errorf("%s: %v", path, err)
				continue
			}
			if target.IsAbs() || target.Path == "" {
				continue
			}
			if strings.Contains(target.Path, "archive/") {
				t.Errorf("%s links to archived documentation: %s", path, m[1])
			}
			if _, err := os.Stat(filepath.Join(root, filepath.Dir(path), target.Path)); err != nil {
				t.Errorf("%s: broken link %s: %v", path, m[1], err)
			}
		}
	}
}
