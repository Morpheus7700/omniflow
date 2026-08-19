package health

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

// The self-probe exists because of what these services are packaged in.
//
// Every service image is `gcr.io/distroless/static:nonroot`. That image ships no shell, no curl,
// and no wget — deliberately, because the smaller the image the smaller the attack surface. The
// consequence is that a Compose or Kubernetes healthcheck written the usual way:
//
//	healthcheck:
//	  test: ["CMD-SHELL", "curl -f http://localhost:8080/readyz"]
//
// cannot run at all. There is no /bin/sh to interpret it and no curl to execute.
//
// That is not a loud failure. Compose marks the container unhealthy or the check errors, and the
// practical outcome has been that the app services carry NO healthcheck — so every
// `depends_on: service_healthy` in the stack silently degrades to `service_started`, which means
// only "the container process was created". Downstream services start against dependencies that
// have not opened a database connection yet, and the compose file reads as though ordering is
// guaranteed when it is not.
//
// The one executable guaranteed to exist inside the image is the service binary itself. So the
// binary learns to probe itself: `/svc -probe` performs a single readiness request against this
// process's own endpoint and exits 0 or 1. No shell, no extra layer, nothing to keep in sync with
// the image.
const (
	// ProbeFlag is the argv token that puts a service binary into probe mode instead of starting it.
	ProbeFlag = "-probe"

	// DefaultProbeURL targets loopback on purpose: the probe runs INSIDE the container it is
	// checking, so it must not depend on service discovery, DNS, or the container's published
	// ports. 127.0.0.1 is the only address guaranteed to mean "this process".
	DefaultProbeURL = "http://127.0.0.1:8080/readyz"

	// probeTimeout bounds the request. An unbounded probe hangs until the platform's own timeout
	// fires, which reports "the healthcheck timed out" instead of "readiness said no" — the same
	// distinction DefaultTimeout preserves for individual dependency checks.
	probeTimeout = 3 * time.Second
)

// ProbeRequested reports whether argv asks the binary to probe itself rather than start serving.
//
// This is checked before any flag parsing or configuration loading, so a probe never needs the
// environment a running service needs. A healthcheck that fails because it could not read
// KAFKA_BROKERS would report the service unhealthy for a reason that has nothing to do with it.
func ProbeRequested(args []string) bool {
	for _, a := range args {
		if a == ProbeFlag || a == "--probe" {
			return true
		}
	}
	return false
}

// ProbeURL returns the endpoint the self-probe should call, overridable for services that do not
// listen on the default port.
func ProbeURL() string {
	if v := os.Getenv("HEALTH_PROBE_URL"); v != "" {
		return v
	}
	return DefaultProbeURL
}

// SelfProbe performs one readiness request and returns nil only for HTTP 200.
//
// It deliberately reports the status code rather than the body: /readyz already names the failing
// dependency in its JSON, and a healthcheck's job is to produce an exit code, not a diagnosis. The
// body is where an operator looks next.
func SelfProbe(ctx context.Context, url string) error {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build probe request for %s: %w", url, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", url, err)
	}
	// The probe process exits immediately after this call, so the connection is torn down either
	// way; the close error cannot change the verdict and there is nothing to log it to.
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probe %s: readiness returned %s", url, resp.Status)
	}
	return nil
}

// RunProbe is the whole probe entrypoint: call it first in main, and if it reports true the binary
// has already done its job and main must return without starting the service.
//
//	if health.RunProbe(os.Args[1:]) {
//	    return
//	}
//
// It exits the process directly on failure so callers cannot accidentally start a service while
// running in probe mode.
func RunProbe(args []string) bool {
	if !ProbeRequested(args) {
		return false
	}
	if err := SelfProbe(context.Background(), ProbeURL()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return true
}
