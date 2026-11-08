package release

import (
	"os/exec"
	"strings"
	"testing"
)

// Production SDK imports must not pull the optional etcd adapter into consumers.
// Integration tests deliberately import it and therefore do not use -test here.
func TestProductionDependencyBoundary(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "../../node", "../../controller")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("dependency graph: %v\n%s", err, output)
	}
	for _, name := range strings.Fields(string(output)) {
		if strings.HasPrefix(name, "go.etcd.io/") || name == "github.com/fastabc/fastmesh/store/etcd" {
			t.Errorf("optional etcd dependency reached production SDK: %s", name)
		}
	}
}
