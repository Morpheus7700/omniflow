package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakePinger struct {
	err   error
	block time.Duration
}

func (f fakePinger) Ping(ctx context.Context) error {
	if f.block > 0 {
		select {
		case <-time.After(f.block):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.err
}

func get(t *testing.T, h http.HandlerFunc, path string) (int, map[string]any) {
	t.Helper()
	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("probe returned non-JSON body %q: %v", rr.Body.String(), err)
	}
	return rr.Code, body
}

// The central invariant of this package. If liveness ever consults a dependency, a database blip
// stops being a degraded-service incident and becomes a fleet-wide restart, because the platform
// kills every container whose liveness probe fails. This test fails loudly if someone "helpfully"
// adds a DB check to Live.
func TestLivenessIgnoresDependencies(t *testing.T) {
	h := New(DBCheck("crdb", fakePinger{err: errors.New("connection refused")}))
	h.MarkStarted()

	code, body := get(t, h.Live, "/healthz")
	if code != http.StatusOK {
		t.Fatalf("liveness must stay 200 while a dependency is down, got %d (%v)", code, body)
	}

	// And the same handler's readiness must disagree — otherwise the two probes are not actually
	// distinct and the split is decorative.
	if code, _ := get(t, h.Ready, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("readiness must be 503 while the dependency is down, got %d", code)
	}
}

// The bug this whole package replaces: the old handler answered 200 from the instant the port was
// bound, so a platform gating traffic on it sent requests to a service still wiring its consumers.
func TestReadinessIsFalseBeforeStartupCompletes(t *testing.T) {
	h := New(DBCheck("crdb", fakePinger{}))

	code, body := get(t, h.Ready, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("readiness before MarkStarted must be 503, got %d", code)
	}
	if body["status"] != "starting" {
		t.Errorf("status = %v, want \"starting\" so an operator can tell boot from breakage", body["status"])
	}

	h.MarkStarted()
	if code, _ := get(t, h.Ready, "/readyz"); code != http.StatusOK {
		t.Fatalf("readiness after MarkStarted with a healthy dependency must be 200, got %d", code)
	}
}

// A 503 that does not say which dependency failed forces an operator to go digging at the worst
// possible moment, so the name and the reason are part of the contract.
func TestReadinessNamesTheFailingDependency(t *testing.T) {
	h := New(
		DBCheck("crdb", fakePinger{err: errors.New("connection refused")}),
		Checker{Name: "kafka", Check: func(context.Context) error { return nil }},
	)
	h.MarkStarted()

	code, body := get(t, h.Ready, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", code)
	}
	checks, ok := body["checks"].(map[string]any)
	if !ok {
		t.Fatalf("body has no checks map: %v", body)
	}
	if got := checks["crdb"]; got != "connection refused" {
		t.Errorf("crdb = %q, want the underlying error so the 503 is self-describing", got)
	}
	if got := checks["kafka"]; got != "ok" {
		t.Errorf("kafka = %q, want \"ok\": a healthy dependency must not be blamed for its neighbour", got)
	}
}

// A hung dependency must fail fast and be reported as a timeout, not block until the platform's own
// probe deadline fires with no information about which dependency hung.
func TestReadinessTimesOutRatherThanHanging(t *testing.T) {
	h := New(DBCheck("crdb", fakePinger{block: time.Hour})).WithTimeout(50 * time.Millisecond)
	h.MarkStarted()

	done := make(chan struct{})
	var code int
	var body map[string]any
	go func() {
		code, body = get(t, h.Ready, "/readyz")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness hung on an unresponsive dependency instead of timing out")
	}

	if code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", code)
	}
	checks := body["checks"].(map[string]any)
	if got, _ := checks["crdb"].(string); got == "" || got[:7] != "timeout" {
		t.Errorf("crdb = %q, want a timeout message distinct from a refusal", got)
	}
}

// One slow dependency must not consume a shared budget and cause the checks after it to report a
// context error they did not cause — that blames the wrong system during an incident.
func TestSlowDependencyDoesNotPoisonTheNextCheck(t *testing.T) {
	h := New(
		DBCheck("slow", fakePinger{block: time.Hour}),
		DBCheck("fast", fakePinger{}),
	).WithTimeout(50 * time.Millisecond)
	h.MarkStarted()

	_, body := get(t, h.Ready, "/readyz")
	checks := body["checks"].(map[string]any)
	if got := checks["fast"]; got != "ok" {
		t.Errorf("fast = %q, want \"ok\" — a per-check timeout must not leak into the next check", got)
	}
}

// Probes are polled forever; a cached 200 from an instance that has since gone unready is exactly
// what these endpoints exist to prevent.
func TestProbesAreNotCacheable(t *testing.T) {
	h := New()
	h.MarkStarted()
	for _, tc := range []struct {
		name string
		fn   http.HandlerFunc
	}{{"live", h.Live}, {"ready", h.Ready}} {
		rr := httptest.NewRecorder()
		tc.fn(rr, httptest.NewRequest(http.MethodGet, "/", nil))
		if got := rr.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s Cache-Control = %q, want no-store", tc.name, got)
		}
	}
}

// "/" predates this package and is what the Dockerfiles and Cloud Run config already probe. It must
// keep meaning exactly what it meant before — unconditional 200 — or this change silently alters
// the behaviour of every existing caller.
func TestRootStaysAliasedToLivenessNotReadiness(t *testing.T) {
	h := New(DBCheck("crdb", fakePinger{err: errors.New("down")}))
	// deliberately NOT started, and the dependency is failing: readiness would be 503 twice over.
	mux := http.NewServeMux()
	h.Register(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf(`"/" = %d, want 200: it has always been unconditional, and re-pointing it at readiness would break existing probes`, rr.Code)
	}
}

func TestRegisterWiresBothProbes(t *testing.T) {
	h := New()
	h.MarkStarted()
	mux := http.NewServeMux()
	h.Register(mux)

	for _, path := range []string{"/healthz", "/readyz"} {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Errorf("%s = %d, want 200", path, rr.Code)
		}
	}
}
