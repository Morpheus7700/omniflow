// Package delivery bounds the two calls that finish a Kafka message's handling: the DLQ produce and
// the offset commit.
//
// Those two calls are the confirmed-DLQ-before-commit contract CLAUDE.md names load-bearing. Both
// took the consumer's own long-lived context, which gave them two properties they should not have.
//
// **They could not fail.** `ProduceSync` against an unreachable or wedged broker blocks for as long
// as the broker stays that way. The consumer is single-threaded per partition, so a hung produce
// stalls that partition indefinitely: no commit, no progress, no error, and a service that looks
// alive because liveness performs no I/O by design. This is the same shape as the unbounded database
// statement fixed in internal/platform/crdbpool — an unbounded call is not a slow call, it is a call
// that can never fail.
//
// **They were abandoned on shutdown.** The consumer context is cancelled by SIGTERM. A message being
// dead-lettered at that moment lost its produce and its commit together, so the poison message came
// back on restart and was re-processed from the top. Correct under at-least-once, but it means a
// rolling restart quietly re-does work it had already decided was terminal.
//
// Context solves both by deriving from context.WithoutCancel: the deadline is ours, the parent's
// cancellation does not propagate. Shutdown therefore gets a bounded window to complete the durable
// handoff rather than abandoning it — and the window is bounded, so it cannot hold shutdown open the
// way an unbounded call would.
//
// Deliberately NOT used for the message processing itself. Processing must observe cancellation, or
// a stop request would be ignored for as long as work remains.
package delivery

import (
	"context"
	"fmt"
	"os"
	"time"
)

// DefaultTimeout bounds a DLQ produce or an offset commit.
//
// Ten seconds is chosen against Kafka's own behaviour, not picked round: a produce to a healthy
// broker completes in single-digit milliseconds, and franz-go retries internally across a leader
// election, which is the slowest event this call legitimately waits on. Anything past that window is
// a broker that is not coming back inside the request, and the honest response is to stop waiting,
// leave the offset uncommitted, and let redelivery retry it later — which is safe precisely because
// the ordering guarantees the DLQ write happened first or not at all.
const DefaultTimeout = 10 * time.Second

// Context returns a bounded context for a DLQ produce or an offset commit.
//
// The returned context does not inherit parent's cancellation, only its values — so a trace stays
// attached while a SIGTERM mid-handoff does not sever the write. Callers must call the returned
// cancel, as with any context.WithTimeout.
func Context(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), timeoutFromEnv())
}

// timeoutFromEnv reads KAFKA_DELIVERY_TIMEOUT as a Go duration ("10s", "1m").
//
// Malformed or non-positive values fall back to the default rather than refusing to start. This
// differs from CRDB_STATEMENT_TIMEOUT on purpose: a bad statement timeout should stop a service
// before it touches data, whereas refusing to boot a consumer over a mistyped delivery timeout would
// take the pipeline down to protect a value that has a safe default.
func timeoutFromEnv() time.Duration {
	v := os.Getenv("KAFKA_DELIVERY_TIMEOUT")
	if v == "" {
		return DefaultTimeout
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		fmt.Fprintf(os.Stderr, "ignoring invalid KAFKA_DELIVERY_TIMEOUT %q, using %s\n", v, DefaultTimeout)
		return DefaultTimeout
	}
	return d
}
