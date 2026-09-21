// Package testinfra derives test infrastructure from the shipped artefacts rather than restating
// them. Integration suites that boot a real CockroachDB must run the engine the compose stack
// deploys; the only way to guarantee that across a Dependabot bump is to read the pin from
// docker-compose.yml at test time instead of copying it into a Go constant.
//
// Before this package existed the image was a const in two test files. When compose was bumped
// v24.3.3 -> v26.2.4 both files silently kept testing the OLD engine and every job stayed green;
// once a guard test caught that, every Dependabot CRDB bump failed CI instead, because the bot
// cannot edit a Go string. Deriving the value removes both failure modes.
package testinfra

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var crdbImageRe = regexp.MustCompile(`(?m)^\s*image:\s*(cockroachdb/cockroach:\S+)`)

// RepoRoot walks up from the test's working directory to the directory holding the root go.mod,
// so real shipped files (schemas, init scripts, compose) can be read rather than hand-copied
// duplicates that could drift.
func RepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			// The frontend carries a fence go.mod and viz-gateway is its own module; the root is
			// the one whose directory also holds docker-compose.yml.
			if _, err := os.Stat(filepath.Join(dir, "docker-compose.yml")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repo root (no go.mod + docker-compose.yml found walking up)")
		}
		dir = parent
	}
}

// CRDBImage returns the cockroachdb/cockroach image docker-compose.yml pins, and fails the test if
// the compose file pins more than one distinct image: the `cockroachdb` node and the `crdb-init`
// CLI container must run the same version, or the schema is applied by a different binary than
// the one that serves it.
func CRDBImage(t *testing.T) string {
	t.Helper()
	composePath := filepath.Join(RepoRoot(t), "docker-compose.yml")
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	found := crdbImageRe.FindAllStringSubmatch(string(raw), -1)
	if len(found) == 0 {
		t.Fatalf("no cockroachdb/cockroach image found in %s — did the service get renamed?", composePath)
	}
	image := found[0][1]
	for _, m := range found[1:] {
		if m[1] != image {
			t.Fatalf("docker-compose.yml pins two different CockroachDB images (%s and %s); "+
				"the node and crdb-init must run the same engine", image, m[1])
		}
	}
	return image
}
