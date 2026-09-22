package platform_test

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"omniflow/internal/platform/testinfra"
)

// viz-gateway is its own Go module and cannot import internal/platform, so five packages exist
// twice: here, and under services/viz-gateway/internal. Each copy's doc comment says the pair is
// kept identical by "the tests in each module" — which, until this test, was a claim nothing
// enforced. This parses both copies and compares them with comments stripped, so a fix that lands
// on one side and not the other fails the build instead of waiting for the incident.
var duplicated = []struct{ root, viz string }{
	{"internal/platform/health", "services/viz-gateway/internal/health"},
	{"internal/platform/crdbpool", "services/viz-gateway/internal/crdbpool"},
	{"internal/platform/delivery", "services/viz-gateway/internal/delivery"},
	{"internal/platform/kafkaconf", "services/viz-gateway/internal/kafkaconf"},
	{"internal/platform/telemetry", "services/viz-gateway/internal/telemetry"},
}

func TestVizGatewayCopiesMatchRootModule(t *testing.T) {
	repo := testinfra.RepoRoot(t)
	for _, d := range duplicated {
		t.Run(filepath.Base(d.root), func(t *testing.T) {
			rootFiles := goFiles(t, filepath.Join(repo, d.root))
			vizFiles := goFiles(t, filepath.Join(repo, d.viz))
			for name, rootSrc := range rootFiles {
				vizSrc, ok := vizFiles[name]
				if !ok {
					t.Errorf("%s exists in %s but not in %s", name, d.root, d.viz)
					continue
				}
				if rootSrc != vizSrc {
					t.Errorf("%s drifted between %s and %s (compared with comments stripped) — copy the fix to both", name, d.root, d.viz)
				}
			}
			for name := range vizFiles {
				if _, ok := rootFiles[name]; !ok {
					t.Errorf("%s exists in %s but not in %s", name, d.viz, d.root)
				}
			}
		})
	}
}

// goFiles returns each non-test .go file in dir, printed from its AST with comments removed and
// import paths normalised (the viz copy imports its sibling packages under its own module path).
func goFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	out := map[string]string{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0) // mode 0: comments dropped
		if err != nil {
			t.Fatalf("parse %s: %v", filepath.Join(dir, name), err)
		}
		f.Doc = nil
		ast.Inspect(f, func(n ast.Node) bool {
			if imp, ok := n.(*ast.ImportSpec); ok {
				v := strings.Trim(imp.Path.Value, `"`)
				v = strings.Replace(v, "omniflow/services/viz-gateway/internal/", "omniflow/internal/platform/", 1)
				imp.Path.Value = `"` + v + `"`
			}
			return true
		})
		var sb strings.Builder
		if err := printer.Fprint(&sb, fset, f); err != nil {
			t.Fatalf("print %s: %v", name, err)
		}
		out[name] = sb.String()
	}
	return out
}
