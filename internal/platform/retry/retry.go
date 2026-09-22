// Package retry is the in-place retry policy every consumer applies to a transient failure.
//
// The consumers used to carry their own constants — five attempts, 100–200 ms doubling, roughly
// three to six seconds in total — which meant any CockroachDB or broker blip longer than that
// dead-lettered every in-flight message as "transient retries exhausted". Real blips are longer
// than that. The default here survives about half a minute, with full jitter so a fleet of
// consumers retrying the same outage does not hammer it in lockstep.
//
// Environment: RETRY_MAX_ATTEMPTS (default 8), RETRY_INITIAL_BACKOFF (200ms), RETRY_MAX_BACKOFF
// (10s). Per-partition ordering is preserved by construction: the retry blocks the partition,
// which is the point — the alternative is committing past an offset that was never processed.
package retry

import (
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"time"
)

type Policy struct {
	MaxAttempts int
	Initial     time.Duration
	Max         time.Duration
}

// Default is what runs when nothing is configured: ~32 s of cumulative patience.
var Default = Policy{MaxAttempts: 8, Initial: 200 * time.Millisecond, Max: 10 * time.Second}

// FromEnv reads the policy, falling back per field and saying so; a malformed value must not
// silently become zero attempts.
func FromEnv() Policy {
	p := Default
	if v := os.Getenv("RETRY_MAX_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			p.MaxAttempts = n
		} else {
			slog.Error("invalid RETRY_MAX_ATTEMPTS, using default", "value", v, "default", p.MaxAttempts)
		}
	}
	if v := os.Getenv("RETRY_INITIAL_BACKOFF"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			p.Initial = d
		} else {
			slog.Error("invalid RETRY_INITIAL_BACKOFF, using default", "value", v, "default", p.Initial)
		}
	}
	if v := os.Getenv("RETRY_MAX_BACKOFF"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			p.Max = d
		} else {
			slog.Error("invalid RETRY_MAX_BACKOFF, using default", "value", v, "default", p.Max)
		}
	}
	return p
}

// Backoff returns how long to wait before attempt n+1 (attempt is 1-based): exponential from
// Initial, capped at Max, with full jitter in [0, cap] so retries desynchronise across replicas.
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	capped := p.Initial
	for i := 1; i < attempt && capped < p.Max; i++ {
		capped *= 2
	}
	if capped > p.Max {
		capped = p.Max
	}
	if capped <= 0 {
		return 0
	}
	// math/rand is correct here: this is timing jitter, not a secret; a CSPRNG would be pure cost.
	return time.Duration(rand.Int64N(int64(capped)) + 1) // #nosec G404 -- jitter only, not security-sensitive
}
