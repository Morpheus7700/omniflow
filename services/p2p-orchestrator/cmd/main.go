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
	"omniflow/internal/platform/crdbpool"
	"omniflow/internal/platform/health"
	"omniflow/internal/platform/kafkaconf"
	"omniflow/internal/platform/telemetry"
	"omniflow/services/p2p-orchestrator/internal/adapters/inbound/kafka"
	"omniflow/services/p2p-orchestrator/internal/adapters/outbound/agent"
	"omniflow/services/p2p-orchestrator/internal/adapters/outbound/crdb"
	"omniflow/services/p2p-orchestrator/internal/core"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"

	"github.com/twmb/franz-go/pkg/kgo"
	"strconv"
	"time"
)

func main() {
	// Container healthcheck mode. Checked before anything else so the probe never depends on the
	// configuration a running service needs — see internal/platform/health/probe.go.
	if health.RunProbe(os.Args[1:]) {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Logging + tracing + metrics. This used to build an exporter against a hardcoded
	// localhost:4318 with no service.name — every batch failed inside a container, and the spans
	// that did escape arrived as unknown_service.
	shutdownTelemetry, err := telemetry.Init(ctx, "p2p-orchestrator")
	if err != nil {
		slog.Error("telemetry init", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := shutdownTelemetry(context.Background()); err != nil {
			slog.Error("telemetry shutdown", "error", err)
		}
	}()

	// 2. Init DB Pool
	dsn := os.Getenv("CRDB_DSN")
	if dsn == "" {
		// No default — the old one pointed at `defaultdb`, contradicting the locked "DB name =
		// omniflow" decision, and a silently-wrong database is worse than a refused boot.
		slog.Error("CRDB_DSN is not set")
		os.Exit(1)
	}
	// crdbpool, not pgxpool.New: it applies a statement_timeout so no query can hang forever.
	pool, err := crdbpool.New(ctx, dsn)
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
	}, core.Config{
		// The container hostname (Kubernetes sets it to the pod name) — so the workflow row names
		// the replica that parked it, not a literal.
		OwnerPod:     env("HOSTNAME", "orchestrator"),
		HITLLeaseTTL: envDuration("HITL_LEASE_TTL", core.DefaultHITLLeaseTTL),
	})

	// 3. Init Kafka
	// Transport (brokers, TLS, SASL) comes from the environment via kafkaconf; the consumer
	// contract — manual commit — is fixed here and must stay.
	kopts, err := kafkaconf.FromEnv()
	if err != nil {
		slog.Error("kafka config", "error", err)
		os.Exit(1)
	}
	client, err := kgo.NewClient(append(kopts,
		kgo.ConsumerGroup("p2p-orchestrator"),
		kgo.ConsumeTopics("omniflow.orchestration.v1", "omniflow.p2p.approval.v1"),
		kgo.DisableAutoCommit(),
	)...)
	if err != nil {
		slog.Error("Failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	adapter, err := kafka.NewConsumer(client, svc)
	if err != nil {
		slog.Error("Failed to build consumer", "error", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		adapter.Start(ctx)
	}()

	// The human-gate sweep. A workflow parked at human_approval carries a lease expiry that, until
	// this existed, nothing read: an approval that never arrived left the workflow SUSPENDED forever
	// and the purchase order it gated invisible. Every replica sweeps; the row lock decides who
	// fails a given workflow.
	sweepEvery := envDuration("HITL_SWEEP_INTERVAL", time.Minute)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(sweepEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := svc.ReapExpiredApprovals(ctx, 100)
				if err != nil {
					slog.Error("approval sweep failed", "error", err)
				} else if n > 0 {
					slog.Warn("approval sweep failed timed-out workflows", "count", n)
				}
			}
		}
	}()

	// 4. Start Healthcheck Server
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

// envDuration reads a Go duration ("24h", "90s"). A malformed value falls back and says so, for
// the same reason envUint does: a silently-zero TTL would fail every approval on the next sweep.
func envDuration(k string, def time.Duration) time.Duration {
	raw := os.Getenv(k)
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		slog.Error("invalid duration, using default", "key", k, "value", raw, "default", def, "error", err)
		return def
	}
	return v
}
