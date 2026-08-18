// Package health provides the liveness and readiness endpoints every service in this repo exposes.
//
// The two probes answer different questions, and conflating them is the classic way to turn a
// recoverable dependency outage into a self-inflicted outage:
//
//   - /healthz (liveness) — "is this process wedged?" It MUST NOT touch the database, the broker,
//     or anything else across the network. An orchestrator responds to a failed liveness probe by
//     KILLING the container. If liveness pinged CockroachDB, then a thirty-second database blip
//     would restart every replica of every service simultaneously — and they would all come back
//     cold, reconnect at once, and hammer the database that was already struggling. The blast
//     radius of a dependency wobble becomes a full-fleet restart. So liveness answers only for the
//     process itself.
//
//   - /readyz (readiness) — "should this instance receive traffic right now?" This one DOES check
//     dependencies, because the correct response to "my database is unreachable" is to be taken
//     out of the load-balancer rotation until it is reachable again, not to be killed. Readiness
//     may flap; that is the point.
//
// This distinction is why the previous single handler was not merely incomplete but misleading:
//
//	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
//	    w.WriteHeader(http.StatusOK)   // 200 before the pool has ever been touched
//	})
//
// It returned 200 from the moment the HTTP server bound its port, which is before the pgx pool has
// established a single connection. Any platform gating traffic on it — Cloud Run, a Kubernetes
// readinessProbe, an ALB target group, or compose's `depends_on: service_healthy` — would route
// requests to an instance that cannot serve them, and would report the deployment healthy while it
// failed every request. A probe that cannot fail is not a probe.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"
)

// DefaultTimeout bounds every readiness check.
//
// It exists because an unbounded check is worse than no check: if the database has stopped
// answering (rather than actively refusing), the ping blocks, the probe never responds, and the
// platform's own probe timeout eventually fires with no information about which dependency hung.
// Failing fast and naming the dependency turns a timeout into a diagnosis. Two seconds sits well
// inside the probe intervals platforms default to (5-10s) while being far longer than a healthy
// single-node CRDB ping, which is sub-millisecond.
const DefaultTimeout = 2 * time.Second

// Check reports whether one dependency is currently usable. It must respect ctx and return
// promptly; the caller applies its own timeout but cannot interrupt a function that ignores it.
type Check func(ctx context.Context) error

// Checker names a dependency so a failing probe says WHICH one is down. A bare 503 tells an
// operator only that something is wrong, which is the least useful moment to have to go digging.
type Checker struct {
	Name  string
	Check Check
}

// Handler serves liveness and readiness for one service.
type Handler struct {
	checks  []Checker
	timeout time.Duration
	// ready gates readiness during startup. A service is live as soon as its HTTP server is up,
	// but it is not ready until construction has finished wiring its consumers — otherwise the
	// window between "port bound" and "consumer running" reports ready and receives traffic.
	started atomic.Bool
	now     func() time.Time
}

// New builds a Handler over the given dependency checks. Call MarkStarted once the service has
// finished wiring itself; until then readiness reports 503 regardless of dependency state.
func New(checks ...Checker) *Handler {
	return &Handler{checks: checks, timeout: DefaultTimeout, now: time.Now}
}

// WithTimeout overrides the per-check deadline. Mainly for tests.
func (h *Handler) WithTimeout(d time.Duration) *Handler {
	h.timeout = d
	return h
}

// MarkStarted flips readiness on. Idempotent.
func (h *Handler) MarkStarted() { h.started.Store(true) }

// Live answers /healthz. It deliberately performs no I/O: reaching this handler at all proves the
// process is scheduled, its runtime is responsive, and its HTTP server is serving. That is the
// entire question liveness is allowed to ask.
func (h *Handler) Live(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// Ready answers /readyz: every dependency is reachable AND startup has completed.
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	if !h.started.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "starting",
			"detail": "service has not finished wiring its consumers",
		})
		return
	}

	results := make(map[string]string, len(h.checks))
	status := http.StatusOK

	for _, c := range h.checks {
		// Per-check timeout, not one budget shared across all of them: with a shared deadline a
		// single slow dependency consumes the whole budget and the checks after it report a
		// context error they did not cause, blaming the wrong system.
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		err := c.Check(ctx)
		cancel()

		switch {
		case err == nil:
			results[c.Name] = "ok"
		case errors.Is(err, context.DeadlineExceeded):
			// Distinguished from a plain error on purpose. "Refused" means the dependency is down
			// and will likely come back; "timed out" often means it is up but saturated, which is
			// a different incident with a different response.
			results[c.Name] = "timeout after " + h.timeout.String()
			status = http.StatusServiceUnavailable
		default:
			results[c.Name] = err.Error()
			status = http.StatusServiceUnavailable
		}
	}

	body := map[string]any{"status": "ok", "checks": results}
	if status != http.StatusOK {
		body["status"] = "unavailable"
	}
	writeJSON(w, status, body)
}

// Register wires both probes, plus "/" for backward compatibility.
//
// "/" is kept because it is what the Dockerfiles, compose, and any existing Cloud Run configuration
// currently point at; removing it in the same change that adds the real probes would break the
// deployment this change is meant to make trustworthy. It is aliased to LIVENESS, not readiness —
// it has always returned 200 unconditionally, so liveness preserves its existing meaning exactly,
// whereas aliasing it to readiness would silently change the behaviour of every existing caller.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.Live)
	mux.HandleFunc("/readyz", h.Ready)
	mux.HandleFunc("/", h.Live)
}

func writeJSON(w http.ResponseWriter, code int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	// Probes are polled every few seconds forever; a cached 200 from an instance that has since
	// gone unready is exactly the failure this endpoint exists to prevent.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// Pinger is the subset of *pgxpool.Pool that readiness needs. Narrowing it here keeps this package
// free of a pgx dependency and lets the tests drive it with a fake instead of a live database.
type Pinger interface {
	Ping(ctx context.Context) error
}

// DBCheck reports the database reachable. Ping acquires a connection from the pool and round-trips
// to the server, so it fails when the pool is exhausted as well as when the server is unreachable —
// both of which mean this instance cannot serve.
func DBCheck(name string, p Pinger) Checker {
	return Checker{Name: name, Check: p.Ping}
}
