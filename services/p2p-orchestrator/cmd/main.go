package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	
	"omniflow/services/p2p-orchestrator/internal/adapters/inbound/kafka"
	"omniflow/services/p2p-orchestrator/internal/adapters/outbound/crdb"
	"omniflow/services/p2p-orchestrator/internal/core"
	"omniflow/services/p2p-orchestrator/internal/core/domain"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	
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
	pool, err := pgxpool.New(ctx, "postgres://root@localhost:26257/defaultdb?sslmode=disable")
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
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "p2p-orchestrator",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		slog.Error("Failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		slog.Error("Failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	adapter := kafka.NewConsumer(consumer, producer, svc)
	
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		adapter.Start(ctx)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down orchestrator gracefully...")
	cancel()
	
	wg.Wait()
	producer.Flush(5000) // Flush DLQ
}
