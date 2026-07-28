// Package errclass is the single place that decides whether a failure is worth retrying.
//
// Before this package there were three classifiers — one per service — and they had drifted into
// disagreeing about the same question. Given an error nobody recognised, commbot and
// inventory-intelligence failed closed (straight to the DLQ) while p2p-orchestrator failed open
// ("assuming transient for safety", 5 retries over ~3.1s, then the DLQ anyway). Two of the three
// also classified no SQLSTATE at all beyond a single hand-picked code.
//
// The concrete cost of that drift: 40001 (serialization_failure) is CockroachDB's *designed*
// signal that a transaction lost an optimistic-concurrency race and should simply be replayed. It
// is expected steady-state behaviour under contention, not an incident. Because p2p classified
// only 55P03, every 40001 fell through to the unknown branch, burned 3.1 seconds of retries, and
// dead-lettered a perfectly valid business event.
//
// Classify returns a class and never a sentinel, so each service keeps its own domain.ErrTransient
// and domain.ErrTerminal. That is deliberate: unifying the sentinels would have meant a shared
// domain package, and the domain cores are currently free of cross-service coupling — a property
// worth more than the small amount of mapping code at each call site.
package errclass

import (
	"context"
	"errors"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Class is the retry disposition of an error.
type Class int

const (
	// Unknown means no rule matched. Callers should treat this as terminal-and-alert: an error
	// we cannot name is an error we cannot bound the retry cost of, and retrying it blocks the
	// partition while producing no new information.
	Unknown Class = iota
	// Transient means the same call may succeed if repeated. Safe to retry with backoff.
	Transient
	// Terminal means the call is deterministic and will fail identically forever. Retrying is
	// pure latency; route to the DLQ immediately.
	Terminal
)

func (c Class) String() string {
	switch c {
	case Transient:
		return "transient"
	case Terminal:
		return "terminal"
	default:
		return "unknown"
	}
}

// transientSQLStates are failures caused by the *state of the cluster*, not the statement.
// The identical statement re-executed a moment later is expected to succeed.
var transientSQLStates = map[string]struct{}{
	"40001": {}, // serialization_failure — CRDB's optimistic-concurrency retry signal. The single
	// most common error in a healthy contended CockroachDB workload, by design.
	"40003": {}, // statement_completion_unknown — outcome genuinely indeterminate; retry is
	// safe only because every write path here is idempotent (outbox + ON CONFLICT DO NOTHING).
	"08000": {}, // connection_exception
	"08001": {}, // sqlclient_unable_to_establish_sqlconnection
	"08003": {}, // connection_does_not_exist
	"08004": {}, // sqlserver_rejected_establishment_of_sqlconnection
	"08006": {}, // connection_failure
	"08007": {}, // transaction_resolution_unknown
	"57P01": {}, // admin_shutdown — a node draining during a rolling restart
	"57P03": {}, // cannot_connect_now — node still starting up
	"55P03": {}, // lock_not_available — the FOR UPDATE NOWAIT lease contention path
}

// terminalSQLStates are failures caused by the statement or the data. Byte-identical input will
// fail byte-identically on every retry, so retrying only delays the DLQ and holds the partition.
var terminalSQLStates = map[string]struct{}{
	"23505": {}, // unique_violation — deterministic. Note: on the outbox paths this is usually
	// *idempotent success* rather than a failure, but that judgement belongs to the caller that
	// knows which constraint was hit, not here.
	"23502": {}, // not_null_violation
	"23503": {}, // foreign_key_violation
	"23514": {}, // check_violation
	"22P02": {}, // invalid_text_representation — e.g. a non-UUID string bound to a UUID column
	"22003": {}, // numeric_value_out_of_range — the uint64/INT8 sequence_engine_key boundary
	"42804": {}, // datatype_mismatch — the round-10 bug: a Go string bound against a UUID column
	"42P01": {}, // undefined_table — the signature of a service pointed at the wrong database
	"42703": {}, // undefined_column — schema drift against a deployed binary
	"42601": {}, // syntax_error
	"0A000": {}, // feature_not_supported — the round-10 bug: a column-less CTE, which Postgres
	// accepts and CockroachDB rejects
}

// Classify decides the retry disposition of err.
//
// Order matters. Context cancellation is checked first because a cancelled context wraps whatever
// the driver was doing at the time, and shutdown is not a data problem. SQLSTATE is checked before
// net.Error because pgconn.PgError values arriving over a broken connection can satisfy both, and
// the server's own opinion of the failure is the more specific signal.
func Classify(err error) Class {
	if err == nil {
		return Unknown
	}

	// Shutdown and deadline expiry: the work was interrupted, not rejected.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Transient
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if _, ok := transientSQLStates[pgErr.Code]; ok {
			return Transient
		}
		if _, ok := terminalSQLStates[pgErr.Code]; ok {
			return Terminal
		}
		// A SQLSTATE we have not triaged. Class 08 is wholly connection-related and class 57
		// wholly operator-intervention, so a prefix match is safe for codes we have not
		// enumerated individually.
		if len(pgErr.Code) == 5 && (pgErr.Code[:2] == "08" || pgErr.Code[:2] == "57") {
			return Transient
		}
		return Unknown
	}

	// pgx signals "the query ran fine and matched nothing". That is data, not a fault, and it is
	// always deterministic for the same key.
	if errors.Is(err, pgx.ErrNoRows) {
		return Terminal
	}

	// Timeouts and refused connections at the socket layer.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return Transient
	}

	return Unknown
}

// IsTransient reports whether err is worth retrying. Unknown is deliberately NOT transient: see
// the Unknown doc comment.
func IsTransient(err error) bool { return Classify(err) == Transient }
