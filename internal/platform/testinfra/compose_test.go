package testinfra

import (
	"strings"
	"testing"
)

// The helper is itself a guard: if it stops finding the pin, every integration suite would start
// with an empty image string and fail confusingly inside testcontainers instead of here.
func TestCRDBImageIsReadFromCompose(t *testing.T) {
	img := CRDBImage(t)
	if !strings.HasPrefix(img, "cockroachdb/cockroach:v") {
		t.Fatalf("unexpected CockroachDB image %q — docker-compose.yml pin changed shape", img)
	}
	if strings.Contains(img, "latest") {
		t.Fatalf("docker-compose.yml pins %q; the stack must run an exact version, never a floating tag", img)
	}
}
