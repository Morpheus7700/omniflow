package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"omniflow/internal/platform/aigov"
	"omniflow/services/p2p-orchestrator/internal/adapters/inbound/kafka"
	"omniflow/services/p2p-orchestrator/internal/adapters/outbound/agent"
	"omniflow/services/p2p-orchestrator/internal/adapters/outbound/crdb"
	"omniflow/services/p2p-orchestrator/internal/core"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"strconv"
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

	// The procurement DAG. draft_po runs BEFORE the approval gate, which is the whole point: a
	// human is asked to approve a concrete purchase order the agent has already drafted, not an
	// abstract intent to buy something.
	dag := &domain.DAG{
		Nodes: map[string]*domain.Node{
			"draft_po":       {ID: "draft_po", Dependencies: []string{}},
			"human_approval": {ID: "human_approval", Dependencies: []string{"draft_po"}},
			"final_step":     {ID: "final_step", Dependencies: []string{"human_approval"}},
		},
	}

	// Governance policy for the agent fleet. Every value is a refusal threshold, and a zero policy
	// denies everything — an unconfigured agent must not be an unbounded one.
	guard := aigov.NewGuard(aigov.Policy{
		AllowedModels: map[string]aigov.CostModel{
			agentModel: {
				PromptMicroUSDPer1K:     envUint("AGENT_PROMPT_COST_PER_1K", 100),
				CompletionMicroUSDPer1K: envUint("AGENT_COMPLETION_COST_PER_1K", 400),
			},
		},
		MaxCostPerWorkflowMicroUSD: envUint("AGENT_MAX_COST_PER_WORKFLOW_MICRO_USD", 500_000),     // $0.50
		MaxEstimatedCallMicroUSD:   envUint("AGENT_MAX_CALL_MICRO_USD", 100_000),                  // $0.10
		AutonomousCeilingMicroUSD:  envUint("AGENT_AUTONOMOUS_CEILING_MICRO_USD", 50_000_000_000), // $50,000
	}, envFloat("AGENT_RATE_RPS", 2), envRateBurst())

	drafter := agent.NewPODrafter(litellmURL, os.Getenv("LITELLM_KEY"), agentModel, guard, store)

	svc := core.NewOrchestratorService(store, dag, map[string]ports.NodeExecutor{
		"draft_po": drafter,
	})

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
	// Bounded, and the error is reported. An unbounded Shutdown can hang forever on one stuck
	// connection, which turns a graceful stop into a killed pod.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("health server shutdown", "error", err)
	}

	wg.Wait()
}

// agentModel is the model the drafting agent is permitted to call. It is also the ONLY key in the
// governance allowlist, so changing it here without pricing it there makes every call refuse —
// which is the correct failure direction for a component that spends money.
var agentModel = env("AGENT_MODEL", "gemini-2.5-flash")

// litellmURL is resolved once at startup; the agent shares the gateway CommBot uses.
var litellmURL = env("LITELLM_URL", "http://mock-llm:4000")

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envUint reads an unsigned governance threshold. A malformed value falls back to the default and
// says so loudly rather than silently becoming zero — a zero ceiling would refuse every call, and a
// silent zero budget is indistinguishable from a broken agent.
func envUint(k string, def uint64) uint64 {
	raw := os.Getenv(k)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		slog.Error("invalid governance threshold, using default", "key", k, "value", raw, "default", def, "error", err)
		return def
	}
	return v
}

func envFloat(k string, def float64) float64 {
	raw := os.Getenv(k)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		slog.Error("invalid rate setting, using default", "key", k, "value", raw, "default", def, "error", err)
		return def
	}
	return v
}

// envRateBurst reads the limiter burst as an int without an unchecked narrowing conversion.
//
// rate.NewLimiter takes an int, and on a 32-bit build a uint64 above MaxInt wraps NEGATIVE — a
// negative burst makes the limiter reject every call, so the drafting agent would refuse all work
// while reporting a healthy startup. Clamped to a sane ceiling instead.
func envRateBurst() int {
	const maxBurst = 1 << 20
	v := envUint("AGENT_RATE_BURST", 4)
	if v > maxBurst {
		slog.Warn("AGENT_RATE_BURST above the permitted ceiling, clamping", "value", v, "ceiling", maxBurst)
		return maxBurst
	}
	return int(v)
}
