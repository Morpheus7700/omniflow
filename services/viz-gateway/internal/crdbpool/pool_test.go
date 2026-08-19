package crdbpool

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testDSN = "postgres://root@localhost:26257/omniflow?sslmode=disable"

// applied runs the real config builder so these tests exercise New's actual behaviour, minus the
// connection. The connecting half is covered by the integration tests against real CRDB.
func applied(t *testing.T, dsn string) *pgxpool.Config {
	t.Helper()
	cfg, err := buildConfig(dsn)
	if err != nil {
		t.Fatalf("buildConfig(%q): %v", dsn, err)
	}
	return cfg
}

func TestDefaultTimeoutIsApplied(t *testing.T) {
	cfg := applied(t, testDSN)
	got := cfg.ConnConfig.RuntimeParams["statement_timeout"]
	want := timeoutMillis(DefaultStatementTimeout)
	if got != want {
		t.Fatalf("statement_timeout = %q, want %q", got, want)
	}
}

func TestEnvOverridesDefault(t *testing.T) {
	t.Setenv("CRDB_STATEMENT_TIMEOUT", "5s")
	cfg := applied(t, testDSN)
	if got, want := cfg.ConnConfig.RuntimeParams["statement_timeout"], "5000"; got != want {
		t.Fatalf("statement_timeout = %q, want %q", got, want)
	}
}

// The DSN is the most specific and most visible place to set this, so it must win. Silently
// overriding a value an operator wrote into the connection string is the kind of surprise that
// makes configuration untrustworthy.
func TestDSNWinsOverEnvAndDefault(t *testing.T) {
	t.Setenv("CRDB_STATEMENT_TIMEOUT", "5s")
	cfg := applied(t, testDSN+"&statement_timeout=1234")
	if got, want := cfg.ConnConfig.RuntimeParams["statement_timeout"], "1234"; got != want {
		t.Fatalf("DSN value was overridden: statement_timeout = %q, want %q", got, want)
	}
}

// A malformed duration must refuse to start rather than quietly fall back. An operator who typed
// "30" instead of "30s" would otherwise get a timeout they never asked for, with no indication.
func TestMalformedEnvIsAnErrorNotAFallback(t *testing.T) {
	for _, v := range []string{"30", "banana", "0", "-5s"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("CRDB_STATEMENT_TIMEOUT", v)
			if _, err := timeoutFromEnv(); err == nil {
				t.Fatalf("CRDB_STATEMENT_TIMEOUT=%q was accepted, want an error", v)
			}
		})
	}
}

func TestEmptyEnvUsesDefault(t *testing.T) {
	t.Setenv("CRDB_STATEMENT_TIMEOUT", "")
	d, err := timeoutFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != DefaultStatementTimeout {
		t.Fatalf("timeoutFromEnv() = %v, want %v", d, DefaultStatementTimeout)
	}
}

func TestNewRejectsAMalformedDSN(t *testing.T) {
	if _, err := New(t.Context(), "://not a dsn"); err == nil {
		t.Fatal("expected an error for a malformed DSN, got nil")
	}
}
