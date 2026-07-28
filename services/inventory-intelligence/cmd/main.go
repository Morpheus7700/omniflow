package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"omniflow/services/inventory-intelligence/internal/adapters/inbound/kafka"
	"omniflow/services/inventory-intelligence/internal/adapters/outbound/crdb"
	"omniflow/services/inventory-intelligence/internal/core/domain"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Setup Database
	dbURL := os.Getenv("CRDB_DSN")
	if dbURL == "" {
		dbURL = "postgres://root@localhost:26257/omniflow?sslmode=disable"
	}
	dbpool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("failed to connect to db", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	repo := crdb.NewRepository(dbpool)

	// 2. Setup Core Domain
	svc := domain.NewValuationService(repo)

	// 3. Setup Kafka Consumer
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaBrokers, ",")...),
		kgo.ConsumerGroup("inventory-intelligence-v1"),
		kgo.ConsumeTopics("omniflow.inventory.movement.v1"),
		kgo.DisableAutoCommit(), // We commit manually after processing
	)
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

	// 5. Start processing
	slog.Info("starting inventory-intelligence service")
	consumer.Start(ctx)

	srv.Shutdown(context.Background())
}
