package api

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Per-client token bucket for the replay endpoint.
//
// The row cap bounds ONE response; nothing bounded how many responses a caller could pull per
// second, so the cap set the price of enumerating the projection history rather than preventing
// it. Keyed by client IP (the connecting peer, or the first X-Forwarded-For hop when
// VIZ_TRUST_PROXY=true); buckets idle for ten minutes are dropped.
//
//	VIZ_REPLAY_RPS    sustained requests per second per client (default 5)
//	VIZ_REPLAY_BURST  burst allowance (default 10)
type IPLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rps      rate.Limit
	burst    int
	trustXFF bool
}

type bucket struct {
	lim  *rate.Limiter
	seen time.Time
}

// NewReplayLimiter builds the per-client limiter from the environment.
func NewReplayLimiter() *IPLimiter {
	l := &IPLimiter{
		buckets:  map[string]*bucket{},
		rps:      rate.Limit(envFloat("VIZ_REPLAY_RPS", 5)),
		burst:    int(envFloat("VIZ_REPLAY_BURST", 10)),
		trustXFF: os.Getenv("VIZ_TRUST_PROXY") == "true",
	}
	go l.sweep()
	return l
}

func (l *IPLimiter) allow(r *http.Request) bool {
	key := clientIP(r, l.trustXFF)
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{lim: rate.NewLimiter(l.rps, l.burst)}
		l.buckets[key] = b
	}
	b.seen = time.Now()
	return b.lim.Allow()
}

func (l *IPLimiter) sweep() {
	for range time.Tick(time.Minute) {
		cutoff := time.Now().Add(-10 * time.Minute)
		l.mu.Lock()
		for k, b := range l.buckets {
			if b.seen.Before(cutoff) {
				delete(l.buckets, k)
			}
		}
		l.mu.Unlock()
	}
}

// Middleware returns 429 with Retry-After when the client's bucket is empty.
func (l *IPLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.allow(r) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate_limited", "too many replay requests; retry shortly")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP is the peer address unless a proxy is explicitly trusted — X-Forwarded-For is
// caller-controlled and honouring it blindly lets one client pick any bucket it likes.
func clientIP(r *http.Request, trustXFF bool) string {
	if trustXFF {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := indexByte(xff, ','); i > 0 {
				return xff[:i]
			}
			return xff
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		slog.Warn("ignoring invalid value, using default", "key", k, "value", v, "default", def)
		return def
	}
	return f
}
