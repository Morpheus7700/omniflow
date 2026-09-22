package telemetry

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Init must succeed with NO environment: that is how every service boots in the proof stack.
// The first version used a semconv schema URL that disagreed with the SDK's resource.Default(),
// resource.Merge refused it, and all four services exited(1) on their first CI boot. This is the
// regression test for that.
func TestInitSucceedsWithoutEnvironment(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTLP_ENDPOINT", "")
	shutdown, err := Init(context.Background(), "probe")
	if err != nil {
		t.Fatalf("Init with no env: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if otel.GetTracerProvider() == nil {
		t.Fatal("no global tracer provider set")
	}
}

// The schema URL this package stamps must be the one the SDK's default resource carries; if the
// SDK is bumped past this semconv version the merge fails again, and this names the fix.
func TestSemconvMatchesSDKDefault(t *testing.T) {
	got := resource.Default().SchemaURL()
	if got != semconv.SchemaURL {
		t.Fatalf("resource.Default() schema %q != this package's semconv %q — bump the semconv import (both copies) to the SDK's", got, semconv.SchemaURL)
	}
}

func TestTraceHandlerStampsIDsOnlyWithASpan(t *testing.T) {
	var sb strings.Builder
	h := traceHandler{slog.NewJSONHandler(&sb, nil)}
	logger := slog.New(h)
	logger.InfoContext(context.Background(), "no span")
	if strings.Contains(sb.String(), "trace_id") {
		t.Fatalf("trace_id stamped without a span: %s", sb.String())
	}
}
