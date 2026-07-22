package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"omniflow/services/inventory-intelligence/internal/adapters/inbound/kafka"
	"omniflow/services/inventory-intelligence/internal/adapters/outbound/crdb"
	"omniflow/services/inventory-intelligence/internal/core/domain"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
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
		kgo.ConsumeTopics("omniflow.p2p.completed.v1"),
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

	// 4. Start processing
	slog.Info("starting inventory-intelligence service")
	consumer.Start(ctx)
}
