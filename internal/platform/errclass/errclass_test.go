package errclass

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func pgErr(code string) error { return &pgconn.PgError{Code: code, Message: "synthetic " + code} }

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Class
	}{
		// The regression this package exists to prevent. 40001 is CockroachDB's designed
		// contention signal; classifying it as anything but transient dead-letters valid work.
		{"40001 serialization_failure is transient", pgErr("40001"), Transient},
		{"55P03 lock_not_available is transient", pgErr("55P03"), Transient},
		{"08006 connection_failure is transient", pgErr("08006"), Transient},
		{"57P01 admin_shutdown is transient", pgErr("57P01"), Transient},

		// Deterministic failures. Each of these cost a real debugging round previously.
		{"42804 datatype_mismatch is terminal", pgErr("42804"), Terminal},
		{"0A000 feature_not_supported is terminal", pgErr("0A000"), Terminal},
		{"42P01 undefined_table is terminal", pgErr("42P01"), Terminal},
		{"23505 unique_violation is terminal", pgErr("23505"), Terminal},
		{"22P02 invalid_text_representation is terminal", pgErr("22P02"), Terminal},

		// Prefix fallback for untriaged codes in wholly-transient classes.
		{"untriaged 08xxx falls back to transient", pgErr("08P01"), Transient},
		{"untriaged 57xxx falls back to transient", pgErr("57014"), Transient},

		// An untriaged code outside those classes must NOT be guessed at.
		{"untriaged 25000 is unknown", pgErr("25000"), Unknown},

		{"context.Canceled is transient", context.Canceled, Transient},
		{"context.DeadlineExceeded is transient", context.DeadlineExceeded, Transient},
		{"pgx.ErrNoRows is terminal", pgx.ErrNoRows, Terminal},
		{"nil is unknown", nil, Unknown},
		{"a plain error is unknown", errors.New("something happened"), Unknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Errorf("Classify(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyUnwrapsWrappedErrors(t *testing.T) {
	// Every call site wraps with %w before this is reached, so unwrapping is load-bearing.
	wrapped := fmt.Errorf("save checkpoint: %w", fmt.Errorf("exec CTE: %w", pgErr("40001")))
	if got := Classify(wrapped); got != Transient {
		t.Errorf("Classify(doubly-wrapped 40001) = %v, want Transient", got)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassifyNetError(t *testing.T) {
	var netErr net.Error = timeoutErr{}
	if got := Classify(fmt.Errorf("dial: %w", netErr)); got != Transient {
		t.Errorf("Classify(net.Error) = %v, want Transient", got)
	}
}

func TestSQLStateTakesPrecedenceOverNetError(t *testing.T) {
	// A PgError arriving over a dying connection can satisfy both branches. The server's own
	// SQLSTATE is the more specific signal and must win, otherwise a terminal 23505 delivered
	// during a blip would be retried forever.
	if got := Classify(pgErr("23505")); got != Terminal {
		t.Errorf("Classify(23505) = %v, want Terminal", got)
	}
}

func TestUnknownIsNotTransient(t *testing.T) {
	// The whole point of the package: an error we cannot name must not be retried by default.
	// p2p-orchestrator previously did exactly that ("assuming transient for safety").
	if IsTransient(errors.New("mystery")) {
		t.Error("IsTransient(unknown) = true; unknown must not be retried by default")
	}
}

func TestClassStringIsStableForLogging(t *testing.T) {
	// These strings land in structured logs and get alerted on; they are an interface.
	for _, tc := range []struct {
		c    Class
		want string
	}{{Transient, "transient"}, {Terminal, "terminal"}, {Unknown, "unknown"}, {Class(99), "unknown"}} {
		if got := tc.c.String(); got != tc.want {
			t.Errorf("Class(%d).String() = %q, want %q", tc.c, got, tc.want)
		}
	}
}
