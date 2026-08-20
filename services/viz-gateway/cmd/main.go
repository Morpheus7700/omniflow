package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"omniflow/services/viz-gateway/internal/api"
	"omniflow/services/viz-gateway/internal/crdbpool"
	"omniflow/services/viz-gateway/internal/health"
	"omniflow/services/viz-gateway/internal/kafka"
	"omniflow/services/viz-gateway/internal/repository"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	// Container healthcheck mode. Checked before anything else so the probe never depends on the
	// configuration a running service needs — see internal/health/probe.go.
	if health.RunProbe(os.Args[1:]) {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup Postgres (for replay)
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://root@localhost:26257/omniflow?sslmode=disable"
	}
	// crdbpool, not pgxpool.New: it applies a statement_timeout so no query can hang forever.
	db, err := crdbpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	repo := repository.NewReplayRepository(db)

	// 2. Setup SSE Broker
	sseBroker := api.NewSSEBroker()
	go sseBroker.Run(ctx)

	// 3. Setup Kafka Consumer
	kafkaBrokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		// Split on comma like every other service. Without this a multi-broker KAFKA_BROKERS
		// value becomes one malformed seed address, so the gateway can only ever reach a
		// single-broker cluster.
		kafkaBrokers = strings.Split(envBrokers, ",")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers...),
		kgo.ConsumerGroup("viz-gateway"),
		kgo.ConsumeTopics("omniflow.p2p.completed.v1", "omniflow.inventory.fact_inventory_movement", "omniflow.inventory.fact_inventory_snapshot"),
		// Manual commit, matching the other three consumers. This option was previously absent,
		// which left franz-go's default autocommit active: offsets were committed on a 5s timer
		// for everything polled, regardless of whether the SSE broadcast had succeeded, and the
		// consumer's MarkCommitRecords call was a silent no-op (franz-go ignores marks unless
		// built with AutoCommitMarks). CLAUDE.md names the manual-commit contract load-bearing,
		// so this service was violating it by omission rather than by decision.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		slog.Error("Failed to create Kafka client", "error", err)
		os.Exit(1)
	}
	defer cl.Close()

	consumer := kafka.NewConsumer(cl, sseBroker)
	go consumer.Start(ctx)

	// 4. Start HTTP Server
	mux := http.NewServeMux()

	// CORS, against an allowlist rather than "*".
	//
	// This previously sent `Access-Control-Allow-Origin: *` on both /api/stream and /api/replay.
	// Neither endpoint authenticates anything, and both serve real procurement and inventory
	// records — workflow ids, trace parents, sequence keys, states. A wildcard on an unauthenticated
	// data endpoint means any page in any tab of any browser that can route to this gateway may read
	// the whole projection history with a single fetch. On a laptop that is a demo; behind a
	// corporate VPN it is an exfiltration path that never touches the network perimeter.
	//
	// The allowlist is echoed back per-request rather than sent as a static header because the
	// wildcard cannot be used with credentials and a fixed single origin breaks the moment there are
	// two front ends. `Vary: Origin` is required so caches never serve one origin's response to
	// another.
	allowedOrigins := parseOrigins(env("VIZ_ALLOWED_ORIGINS", "http://localhost:3000"))
	slog.Info("CORS allowlist", "origins", allowedOrigins)
	withCORS := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Vary", "Origin")
			if origin := r.Header.Get("Origin"); origin != "" && allowedOrigins[origin] {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			// A disallowed origin still reaches the handler: CORS is enforced by the browser, which
			// will refuse to hand the body to the page without the header. Blocking here instead
			// would also block same-origin and non-browser callers (curl, the E2E proof), which send
			// no Origin at all.
			h.ServeHTTP(w, r)
		})
	}

	mux.Handle("/api/stream", withCORS(http.HandlerFunc(sseBroker.StreamHandler)))
	mux.Handle("/api/replay", withCORS(http.HandlerFunc(api.NewReplayHandler(repo).HandleReplay)))

	// viz-gateway had NO health endpoint at all — not even the unconditional "/" the other three
	// carried — so nothing could distinguish "SSE broker running" from "process bound a port". It
	// is the one service the browser talks to directly, which makes its readiness the difference
	// between an empty dashboard and a dashboard that is honestly unavailable.
	probes := health.New(health.DBCheck("crdb", db))
	probes.Register(mux)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("Starting HTTP server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	// Broker running, consumer wired, server serving: only now is this instance able to answer a
	// stream request with real data.
	probes.MarkStarted()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down gracefully...")

	// Bounded, and on a FRESH context rather than the app one. Two reasons, both of which bite here
	// specifically: `ctx` is cancelled by the deferred cancel() as soon as main returns, and passing
	// an unbounded context to Shutdown means waiting for every open connection to drain on its own.
	// This process serves SSE — a stream has no natural end, so a client that never disconnects
	// would hold shutdown open forever and the container would die by SIGKILL instead.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown did not finish cleanly", "error", err)
	}
}

// env returns the value of k, or def when unset or empty.
func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// parseOrigins turns a comma-separated allowlist into a set for O(1) per-request lookup.
//
// Entries are compared byte-for-byte against the request's Origin header, which is scheme + host +
// port and never has a trailing slash. Any entry that carries one is trimmed, because
// "http://localhost:3000/" would otherwise silently match nothing and the failure would look like a
// CORS bug rather than a typo.
func parseOrigins(csv string) map[string]bool {
	out := map[string]bool{}
	for _, o := range strings.Split(csv, ",") {
		if o = strings.TrimRight(strings.TrimSpace(o), "/"); o != "" {
			out[o] = true
		}
	}
	return out
}
