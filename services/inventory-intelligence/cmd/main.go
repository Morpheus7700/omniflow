package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"omniflow/internal/platform/crdbpool"
	"omniflow/internal/platform/health"
	"omniflow/internal/platform/kafkaconf"
	"omniflow/internal/platform/telemetry"
	"omniflow/services/inventory-intelligence/internal/adapters/inbound/kafka"
	"omniflow/services/inventory-intelligence/internal/adapters/outbound/crdb"
	"omniflow/services/inventory-intelligence/internal/core/domain"

	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

func main() {
	// Container healthcheck mode. Checked before anything else so the probe never depends on the
	// configuration a running service needs — see internal/platform/health/probe.go.
	if health.RunProbe(os.Args[1:]) {
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Logging + tracing + metrics. This service had no tracer provider at all: the TraceParent it
	// carries on every movement went nowhere.
	shutdownTelemetry, err := telemetry.Init(ctx, "inventory-intelligence")
	if err != nil {
		slog.Error("telemetry init", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := shutdownTelemetry(context.Background()); err != nil {
			slog.Error("telemetry shutdown", "error", err)
		}
	}()

	// 1. Setup Database
	dbURL := os.Getenv("CRDB_DSN")
	if dbURL == "" {
		// No localhost default: inside a container it points at nothing, and the service would
		// report itself healthy against a database it cannot reach.
		slog.Error("CRDB_DSN is not set")
		os.Exit(1)
	}
	// crdbpool, not pgxpool.New: it applies a statement_timeout so no query can hang forever.
	dbpool, err := crdbpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("failed to connect to db", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	repo := crdb.NewRepository(dbpool)

	// 2. Setup Core Domain
	svc := domain.NewValuationService(repo)

	// 3. Setup Kafka Consumer
	// Transport (brokers, TLS, SASL) comes from the environment via kafkaconf; the consumer
	// contract — manual commit — is fixed here and must stay.
	kopts, err := kafkaconf.FromEnv()
	if err != nil {
		slog.Error("kafka config", "error", err)
		os.Exit(1)
	}
	client, err := kgo.NewClient(append(kopts,
		kgo.ConsumerGroup("inventory-intelligence-v1"),
		kgo.ConsumeTopics("omniflow.inventory.movement.v1"),
		kgo.DisableAutoCommit(), // We commit manually after processing
	)...)
	if err != nil {
		slog.Error("failed to create kafka client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	consumer, err := kafka.NewConsumer(client, svc)
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	// 4. Start Healthcheck Server
	// Liveness and readiness are separate endpoints on purpose — see internal/platform/health.
	// The handler this replaces returned 200 from the moment the port was bound, i.e. before the
	// pgx pool had opened a single connection, so anything gating traffic on it (Cloud Run, a
	// readinessProbe, compose's depends_on: service_healthy) would route to an instance that could
	// not serve. readiness is only reported once wiring below has completed AND the database
	// answers a bounded ping.
	probes := health.New(health.DBCheck("crdb", dbpool))
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

	// 5. Start processing
	slog.Info("starting inventory-intelligence service")
	consumer.Start(ctx)

	// Bounded: an unbounded Shutdown waits for every in-flight request to finish on its own, so a
	// single wedged handler turns a graceful stop into a SIGKILL. The error is logged rather than
	// discarded — "shutdown timed out" is the difference between a clean stop and a dropped request.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown did not finish cleanly", "error", err)
	}
}
