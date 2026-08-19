package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"omniflow/internal/platform/health"
	inkafka "omniflow/services/commbot/internal/adapters/inbound/kafka"
	"omniflow/services/commbot/internal/adapters/outbound/crdb"
	"omniflow/services/commbot/internal/adapters/outbound/llm"
	"omniflow/services/commbot/internal/core/domain"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func main() {
	// Container healthcheck mode. Checked before anything else so the probe never depends on the
	// configuration a running service needs — see internal/platform/health/probe.go.
	if health.RunProbe(os.Args[1:]) {
		return
	}
	if err := run(); err != nil {
		slog.Error("commbot exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// SIGTERM (K8s pod eviction) / SIGINT → cancel ctx → consumer drains gracefully.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := loadConfig()

	// ── OpenTelemetry ────────────────────────────────────────────────────────
	tp, err := initTracer(ctx, cfg.otlpEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(sctx)
	}()

	// ── CockroachDB pool (idempotency + transactional outbox) ────────────────
	pool, err := pgxpool.New(ctx, cfg.crdbDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	repo := crdb.NewRepository(pool) // satisfies both ports.Publisher and inkafka.IdempotencyStore

	// ── Outbound LLM gateway (SSRF allowlist + DoW token bucket) ─────────────
	gateway := llm.NewLiteLLMGateway(cfg.liteLLMURL, cfg.liteLLMKey, cfg.rateRPS, cfg.rateBurst, cfg.allowedHosts)

	// ── Core domain service ──────────────────────────────────────────────────
	svc := domain.NewCommBotService(gateway, repo, tp)

	// ── Kafka client — auto-commit DISABLED (required by the retry/DLQ contract) ──
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.kafkaBootstrap, ",")...),
		kgo.ConsumerGroup("commbot"),
		kgo.ConsumeTopics(cfg.inputTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	adapter, err := inkafka.NewConsumer(client, svc, repo, tp)
	if err != nil {
		return err
	}

	// ── Healthcheck Server (Cloud Run) ───────────────────────────────────────
	// Liveness and readiness are separate endpoints on purpose — see internal/platform/health.
	// The handler this replaces returned 200 from the moment the port was bound, i.e. before the
	// pgx pool had opened a single connection, so anything gating traffic on it (Cloud Run, a
	// readinessProbe, compose's depends_on: service_healthy) would route to an instance that could
	// not serve. readiness is only reported once wiring below has completed AND the database
	// answers a bounded ping.
	probes := health.New(health.DBCheck("crdb", pool))
	mux := http.NewServeMux()
	probes.Register(mux)
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("healthcheck server failed", "error", err)
		}
	}()

	// Everything above is wired: pool open, consumer constructed, HTTP server serving. Only now may
	// this instance accept traffic. Before this point readiness reports 503 "starting", which is the
	// window the previous unconditional 200 handler papered over.
	probes.MarkStarted()

	slog.Info("commbot starting", "input_topic", cfg.inputTopic, "bootstrap", cfg.kafkaBootstrap)
	adapter.Start(ctx) // blocks until ctx is cancelled

	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	_ = srv.Shutdown(sctx)

	slog.Info("commbot stopped cleanly")
	return nil
}

func initTracer(ctx context.Context, endpoint string) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("commbot"),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{}) // W3C traceparent end-to-end
	return tp, nil
}

type config struct {
	kafkaBootstrap string
	inputTopic     string
	crdbDSN        string
	liteLLMURL     string
	liteLLMKey     string
	otlpEndpoint   string
	allowedHosts   []string
	rateRPS        float64
	rateBurst      int
}

func loadConfig() config {
	return config{
		kafkaBootstrap: env("KAFKA_BOOTSTRAP", "localhost:9092"),
		inputTopic:     env("COMMBOT_INPUT_TOPIC", "omniflow.communication.v1"),
		crdbDSN:        env("CRDB_DSN", "postgresql://root@localhost:26257/defaultdb?sslmode=disable"),
		liteLLMURL:     env("LITELLM_URL", "http://localhost:4000"),
		liteLLMKey:     env("LITELLM_KEY", ""),
		otlpEndpoint:   env("OTLP_ENDPOINT", "localhost:4318"),
		allowedHosts:   splitCSV(env("OBJECT_STORE_HOSTS", "")), // e.g. "storage.googleapis.com,s3.amazonaws.com"
		rateRPS:        envFloat("LLM_RATE_RPS", 10),
		rateBurst:      envInt("LLM_RATE_BURST", 20),
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
