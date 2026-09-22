// Package telemetry is the one place a service configures logging, tracing and metrics.
//
// Before it existed the four services disagreed about all three: commbot had a real tracer
// provider with a service name, the orchestrator exported to a hardcoded localhost:4318 with NO
// resource (its traces arrived as unknown_service), inventory-intelligence had no tracer at all,
// and nothing exposed a single metric. Every log line was the slog text default with no level
// control and no way to join a line to a trace.
//
// Environment contract (all optional; the defaults are what the proof stack runs with):
//
//	LOG_LEVEL                     debug | info | warn | error        (default info)
//	LOG_FORMAT                    json | text                        (default json)
//	OTEL_EXPORTER_OTLP_ENDPOINT   collector URL, e.g. http://otel-collector:4318 — the standard
//	                              variable, read by the exporter itself. Unset = spans are created
//	                              (so trace ids still flow into logs) but never exported.
//	OTEL_SERVICE_VERSION          stamped on the resource; defaults to the module's build version.
//
// The legacy OTLP_ENDPOINT (host:port, commbot only) is honoured as an alias.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	// The semconv version MUST match the one the SDK's resource.Default() uses (see
	// sdk/resource/builtin.go): resource.Merge refuses two different schema URLs, and the mismatch
	// took every service down at boot on its first CI run.
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

// Shutdown flushes and stops what Init started. Bounded by the caller's context.
type Shutdown func(context.Context) error

// Init configures the process-wide slog default and the global OTel tracer provider + W3C
// propagator for the named service, and returns the shutdown hook. Call it first in main, right
// after the probe check.
func Init(ctx context.Context, service string) (Shutdown, error) {
	initLogging()

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
		semconv.ServiceVersion(version()),
	))
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}

	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	legacy := os.Getenv("OTLP_ENDPOINT")
	switch {
	case endpoint != "":
		// The exporter reads OTEL_EXPORTER_OTLP_ENDPOINT itself, including the scheme.
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	case legacy != "":
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(legacy), otlptracehttp.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("otlp exporter (OTLP_ENDPOINT): %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	default:
		// No collector configured: spans exist (ids reach the logs) but nothing leaves the process.
		// This replaces exporting to a hardcoded localhost that failed every batch in a container.
		slog.Info("tracing: no OTEL_EXPORTER_OTLP_ENDPOINT, spans will not be exported")
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(ctx)
	}, nil
}

// MetricsHandler serves the process's Prometheus registry. Mount it on the health mux so /metrics
// shares the port every service already exposes.
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})
}

func initLogging() {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	hopts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if strings.ToLower(os.Getenv("LOG_FORMAT")) == "text" {
		h = slog.NewTextHandler(os.Stderr, hopts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, hopts)
	}
	slog.SetDefault(slog.New(traceHandler{h}))
}

// traceHandler stamps trace_id / span_id on every record logged with a context that carries a
// span (slog.InfoContext and friends), so a log line can be joined to its trace without anyone
// remembering to add the attributes by hand.
type traceHandler struct{ slog.Handler }

func (t traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return t.Handler.Handle(ctx, r)
}

func (t traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{t.Handler.WithAttrs(attrs)}
}

func (t traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{t.Handler.WithGroup(name)}
}

func version() string {
	if v := os.Getenv("OTEL_SERVICE_VERSION"); v != "" {
		return v
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 12 {
				return s.Value[:12]
			}
		}
		if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			return bi.Main.Version
		}
	}
	return "dev"
}
