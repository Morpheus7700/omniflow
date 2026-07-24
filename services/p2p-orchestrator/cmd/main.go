package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"omniflow/services/p2p-orchestrator/internal/adapters/inbound/kafka"
	"omniflow/services/p2p-orchestrator/internal/adapters/outbound/crdb"
	"omniflow/services/p2p-orchestrator/internal/core"
	"omniflow/services/p2p-orchestrator/internal/core/domain"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Init OTel Tracing Provider with OTLP Exporter and Batcher
	otel.SetTextMapPropagator(propagation.TraceContext{})

	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint("localhost:4318"), otlptracehttp.WithInsecure())
	if err != nil {
		slog.Error("Failed to create OTLP exporter", "error", err)
		os.Exit(1)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	// 2. Init DB Pool
	dsn := os.Getenv("CRDB_DSN")
	if dsn == "" {
		dsn = "postgres://root@localhost:26257/defaultdb?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("Failed to connect to CRDB", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := crdb.NewStore(pool)

	dag := &domain.DAG{
		Nodes: map[string]*domain.Node{
			"human_approval": {ID: "human_approval", Dependencies: []string{}},
			"final_step":     {ID: "final_step", Dependencies: []string{"human_approval"}},
		},
	}

	svc := core.NewOrchestratorService(store, dag)

	// 3. Init Kafka
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup("p2p-orchestrator"),
		kgo.ConsumeTopics("omniflow.orchestration.v1", "omniflow.p2p.approval.v1"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		slog.Error("Failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	adapter := kafka.NewConsumer(client, svc)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		adapter.Start(ctx)
	}()

	// 4. Start Healthcheck Server
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down orchestrator gracefully...")
	cancel()
	srv.Shutdown(context.Background())

	wg.Wait()
}
