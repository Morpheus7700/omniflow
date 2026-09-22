package retry

import (
	"testing"
	"time"
)

func TestBackoffIsBoundedAndGrows(t *testing.T) {
	p := Policy{MaxAttempts: 8, Initial: 100 * time.Millisecond, Max: 1 * time.Second}
	// The un-jittered ceiling for attempt n is min(Initial*2^(n-1), Max); jitter is in (0, ceiling].
	ceilings := []time.Duration{100e6, 200e6, 400e6, 800e6, 1e9, 1e9, 1e9, 1e9}
	for attempt := 1; attempt <= 8; attempt++ {
		for i := 0; i < 200; i++ {
			d := p.Backoff(attempt)
			if d <= 0 || d > ceilings[attempt-1] {
				t.Fatalf("attempt %d: backoff %v outside (0, %v]", attempt, d, ceilings[attempt-1])
			}
		}
	}
	// Cumulative worst case with the shipped default must comfortably exceed a 7 s blip — the
	// exact failure the old 3–6 s ladders produced.
	var worst time.Duration
	for a := 1; a <= Default.MaxAttempts; a++ {
		c := Default.Initial
		for i := 1; i < a && c < Default.Max; i++ {
			c *= 2
		}
		if c > Default.Max {
			c = Default.Max
		}
		worst += c
	}
	if worst < 20*time.Second {
		t.Fatalf("default policy tolerates only %v of outage; want ≥ 20s", worst)
	}
}

func TestFromEnvFallsBackOnGarbage(t *testing.T) {
	t.Setenv("RETRY_MAX_ATTEMPTS", "lots")
	t.Setenv("RETRY_INITIAL_BACKOFF", "soon")
	t.Setenv("RETRY_MAX_BACKOFF", "0s")
	if p := FromEnv(); p != Default {
		t.Fatalf("garbage env produced %+v, want the default %+v", p, Default)
	}
	t.Setenv("RETRY_MAX_ATTEMPTS", "3")
	t.Setenv("RETRY_INITIAL_BACKOFF", "50ms")
	t.Setenv("RETRY_MAX_BACKOFF", "2s")
	want := Policy{MaxAttempts: 3, Initial: 50 * time.Millisecond, Max: 2 * time.Second}
	if p := FromEnv(); p != want {
		t.Fatalf("FromEnv = %+v, want %+v", p, want)
	}
}
