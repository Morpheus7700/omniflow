package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeRequested(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"no args", []string{}, false},
		{"unrelated args", []string{"serve", "--verbose"}, false},
		{"single dash", []string{"-probe"}, true},
		{"double dash", []string{"--probe"}, true},
		{"among others", []string{"--verbose", "-probe"}, true},
		// Guards against a substring match: a service that later grows a --probe-interval flag
		// must not be mistaken for a probe invocation and exit instead of starting.
		{"prefix only is not a probe", []string{"-probe-interval=5s"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ProbeRequested(tc.args); got != tc.want {
				t.Fatalf("ProbeRequested(%q) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}

func TestSelfProbeAcceptsOnly200(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"ready", http.StatusOK, false},
		// The case that matters: /readyz answers 503 while starting or while a dependency is
		// down. A probe that treated any response as success would report every container healthy
		// the instant its HTTP server bound a port — exactly the bug the real handlers replaced.
		{"starting or dependency down", http.StatusServiceUnavailable, true},
		{"server error", http.StatusInternalServerError, true},
		{"redirect is not ready", http.StatusFound, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			err := SelfProbe(context.Background(), srv.URL)
			if tc.wantErr && err == nil {
				t.Fatalf("status %d: expected an error, got nil", tc.status)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("status %d: unexpected error: %v", tc.status, err)
			}
		})
	}
}

func TestSelfProbeFailsWhenNothingIsListening(t *testing.T) {
	// A closed port is what the probe sees while the process is still wiring itself up, and it
	// must be an error rather than a hang.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	if err := SelfProbe(context.Background(), url); err == nil {
		t.Fatal("expected an error probing a closed port, got nil")
	}
}

func TestProbeURLPrefersEnvOverride(t *testing.T) {
	if got := ProbeURL(); got != DefaultProbeURL {
		t.Fatalf("ProbeURL() = %q, want the default %q", got, DefaultProbeURL)
	}
	t.Setenv("HEALTH_PROBE_URL", "http://127.0.0.1:9999/readyz")
	if got := ProbeURL(); got != "http://127.0.0.1:9999/readyz" {
		t.Fatalf("ProbeURL() ignored HEALTH_PROBE_URL, got %q", got)
	}
}
