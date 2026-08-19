// Package crdbpool builds the CockroachDB connection pool the root-module services use.
//
// It exists for one setting: statement_timeout.
//
// Every service opened its pool with a bare pgxpool.New(ctx, dsn), which leaves statements
// unbounded. An unbounded statement is not merely a slow statement — it is a statement that can
// never fail. A consumer blocked in a query that will never return holds its Kafka partition
// forever: it cannot commit, cannot dead-letter, and cannot be told to stop, because the
// cancellation it would respond to is checked between messages and not inside the driver call. The
// symptom is a service that looks alive (liveness answers, it is not wedged) and consumes nothing.
//
// Bounding it at the database rather than at 28 individual call sites is deliberate. The
// alternative — wrapping each Query/Exec in context.WithTimeout — touches the load-bearing
// checkpoint and DLQ paths, and gets the DLQ produce and the offset commit wrong the first time it
// is done carelessly: those run precisely when processing failed, often because a deadline expired,
// so sharing the expired context with them would break the confirmed-DLQ-before-commit ordering the
// whole delivery guarantee rests on. A session-level statement_timeout bounds every statement,
// including ones added later, and cannot make that mistake.
//
// Duplicated from internal/platform/crdbpool rather than imported: viz-gateway is a separate Go
// module and cannot reach into the root module's internal/, exactly as with internal/health.
//
// The failure it produces surfaces as SQLSTATE 57014 (query_canceled). This service's only database
// work is the /api/replay query, which is a read on a request-scoped context — a timed-out replay
// returns 500 to that one caller and nothing else is affected. There is no consumer offset at risk
// here, which is why this side needs no classification of its own.
package crdbpool

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultStatementTimeout bounds any single statement.
//
// Thirty seconds is far above anything this workload issues in health: the exactly-once CTE and the
// FOR UPDATE NOWAIT lease acquisition are sub-millisecond on a healthy single-node cluster, and
// NOWAIT means the lease path fails immediately rather than queueing. The value is chosen as a
// ceiling on pathology, not as a target — tight enough that a hung statement becomes an error
// within one probe interval, loose enough that it can never fire on a merely contended cluster and
// turn a slow moment into a retry storm.
const DefaultStatementTimeout = 30 * time.Second

// New parses dsn, applies a statement timeout unless the DSN already sets one, and opens the pool.
//
// Precedence is DSN, then CRDB_STATEMENT_TIMEOUT, then DefaultStatementTimeout. The DSN wins
// because it is the most specific and most visible place an operator can set it, and silently
// overriding a value someone wrote into the connection string would be the kind of surprise that
// makes configuration untrustworthy.
func New(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := buildConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open crdb pool: %w", err)
	}
	return pool, nil
}

// buildConfig is everything New does except opening a connection, so it can be tested without a
// live cluster. The integration tests already exercise the connecting half against real CRDB.
func buildConfig(dsn string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse crdb dsn: %w", err)
	}

	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if _, set := cfg.ConnConfig.RuntimeParams["statement_timeout"]; !set {
		d, err := timeoutFromEnv()
		if err != nil {
			return nil, err
		}
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = timeoutMillis(d)
	}
	return cfg, nil
}

// timeoutMillis renders a duration the way the session variable expects it: milliseconds as a bare
// integer, the spelling both CockroachDB and PostgreSQL accept in the startup packet.
func timeoutMillis(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}

// timeoutFromEnv reads CRDB_STATEMENT_TIMEOUT as a Go duration ("15s", "2m").
//
// A malformed value is an error rather than a silent fallback to the default. Falling back would
// mean an operator who typed "30" instead of "30s" gets a timeout they did not ask for and no
// indication of it, which is worse than refusing to start.
func timeoutFromEnv() (time.Duration, error) {
	v := os.Getenv("CRDB_STATEMENT_TIMEOUT")
	if v == "" {
		return DefaultStatementTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("parse CRDB_STATEMENT_TIMEOUT %q: %w", v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("CRDB_STATEMENT_TIMEOUT must be positive, got %q", v)
	}
	return d, nil
}
