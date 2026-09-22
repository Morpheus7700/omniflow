package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"omniflow/internal/platform/crdbpool"
	"omniflow/internal/platform/health"
	"omniflow/internal/platform/kafkaconf"
	"omniflow/internal/platform/telemetry"
	inkafka "omniflow/services/commbot/internal/adapters/inbound/kafka"
	"omniflow/services/commbot/internal/adapters/outbound/crdb"
	"omniflow/services/commbot/internal/adapters/outbound/llm"
	"omniflow/services/commbot/internal/core/domain"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
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

	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// ── Logging + tracing + metrics, one call for every service ────────────
	shutdownTelemetry, err := telemetry.Init(ctx, "commbot")
	if err != nil {
		return err
	}
	defer func() {
		if err := shutdownTelemetry(context.Background()); err != nil {
			slog.Error("telemetry shutdown", "error", err)
		}
	}()
	tp := otel.GetTracerProvider()

	// ── CockroachDB pool (idempotency + transactional outbox) ────────────────
	// crdbpool, not pgxpool.New: it applies a statement_timeout so no query can hang forever.
	pool, err := crdbpool.New(ctx, cfg.crdbDSN)
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
	// Transport (brokers, TLS, SASL) comes from the environment via kafkaconf; the consumer
	// contract — manual commit — is fixed here and must stay.
	kopts, err := kafkaconf.FromEnv()
	if err != nil {
		return fmt.Errorf("kafka config: %w", err)
	}
	client, err := kgo.NewClient(append(kopts,
		kgo.ConsumerGroup("commbot"),
		kgo.ConsumeTopics(cfg.inputTopic),
		kgo.DisableAutoCommit(),
	)...)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
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
	mux.Handle("/metrics", telemetry.MetricsHandler())
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
		WriteTimeout:      15 * time.Second,
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

	slog.Info("commbot starting", "input_topic", cfg.inputTopic)
	adapter.Start(ctx) // blocks until ctx is cancelled

	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	_ = srv.Shutdown(sctx)

	slog.Info("commbot stopped cleanly")
	return nil
}

type config struct {
	inputTopic   string
	crdbDSN      string
	liteLLMURL   string
	liteLLMKey   string
	allowedHosts []string
	rateRPS      float64
	rateBurst    int
}

// loadConfig fails fast, naming the variable. Two of these used to be silent: CRDB_DSN defaulted
// to a localhost `defaultdb` (contradicting the locked "DB name = omniflow" decision), and an empty
// OBJECT_STORE_HOSTS made validateQuarantineURI reject EVERY email as terminal — 100% of traffic
// dead-lettered while /readyz reported the service healthy. A refused boot is the honest failure.
func loadConfig() (config, error) {
	var errs []error
	cfg := config{
		inputTopic:   env("COMMBOT_INPUT_TOPIC", "omniflow.communication.v1"),
		crdbDSN:      os.Getenv("CRDB_DSN"),
		liteLLMURL:   env("LITELLM_URL", "http://localhost:4000"),
		liteLLMKey:   os.Getenv("LITELLM_KEY"),
		allowedHosts: splitCSV(os.Getenv("OBJECT_STORE_HOSTS")), // e.g. "storage.googleapis.com,s3.amazonaws.com"
	}
	if cfg.crdbDSN == "" {
		errs = append(errs, errors.New("CRDB_DSN is not set"))
	}
	if len(cfg.allowedHosts) == 0 {
		errs = append(errs, errors.New("OBJECT_STORE_HOSTS is empty — every quarantine URI would be rejected and every email dead-lettered"))
	}
	var err error
	if cfg.rateRPS, err = envFloat("LLM_RATE_RPS", 10); err != nil {
		errs = append(errs, err)
	}
	if cfg.rateBurst, err = envInt("LLM_RATE_BURST", 20); err != nil {
		errs = append(errs, err)
	}
	return cfg, errors.Join(errs...)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envInt / envFloat return an error for a malformed value rather than the default. Falling back
// silently meant a typo in LLM_RATE_RPS ran the gateway at the default rate with nothing in the
// logs to say so.
func envInt(k string, def int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not an integer", k, v)
	}
	return n, nil
}

func envFloat(k string, def float64) (float64, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a number", k, v)
	}
	return f, nil
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
