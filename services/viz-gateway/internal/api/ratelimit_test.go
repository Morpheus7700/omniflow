package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"
)

func TestReplayLimiterIsPerClientAndAnswers429JSON(t *testing.T) {
	l := &IPLimiter{buckets: map[string]*bucket{}, rps: rate.Limit(1), burst: 2}
	h := l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	hit := func(ip string) int {
		req := httptest.NewRequest(http.MethodGet, "/api/replay", nil)
		req.RemoteAddr = ip + ":12345"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if a, b := hit("10.0.0.1"), hit("10.0.0.1"); a != 200 || b != 200 {
		t.Fatalf("burst of 2 refused: %d %d", a, b)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/replay", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third request in a burst of 2 got %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 without Retry-After")
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error.Code != "rate_limited" {
		t.Fatalf("429 body = %s (%v); want the JSON error shape with code rate_limited", rec.Body.String(), err)
	}
	// Another client has its own bucket.
	if c := hit("10.0.0.2"); c != 200 {
		t.Fatalf("a different client was limited by the first one's bucket: %d", c)
	}
}

func TestClientIPIgnoresForwardedForUnlessTrusted(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.9:4444"
	req.Header.Set("X-Forwarded-For", "1.1.1.1, 2.2.2.2")
	if got := clientIP(req, false); got != "203.0.113.9" {
		t.Fatalf("untrusted proxy: clientIP = %q, want the peer", got)
	}
	if got := clientIP(req, true); got != "1.1.1.1" {
		t.Fatalf("trusted proxy: clientIP = %q, want the first hop", got)
	}
}
