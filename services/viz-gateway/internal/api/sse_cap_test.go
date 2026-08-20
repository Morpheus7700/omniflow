package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// An SSE connection is held open by design, so the cap is the only thing standing between an
// anonymous caller and unbounded goroutines, buffers and sockets in this process.
func TestStreamHandlerRefusesClientsPastTheCap(t *testing.T) {
	b := NewSSEBroker()
	b.maxClients = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// Hold maxClients streams open. Each blocks until its request context is cancelled.
	var wg sync.WaitGroup
	held := make([]context.CancelFunc, 0, 2)
	for i := 0; i < 2; i++ {
		rctx, rcancel := context.WithCancel(context.Background())
		held = append(held, rcancel)
		req := httptest.NewRequest(http.MethodGet, "/api/stream", nil).WithContext(rctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.StreamHandler(httptest.NewRecorder(), req)
		}()
	}

	// Wait for both to register rather than sleeping a fixed amount and hoping.
	deadline := time.Now().Add(2 * time.Second)
	for b.active.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the first two clients never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The third must be refused, and refused with a status a client can act on.
	rec := httptest.NewRecorder()
	b.StreamHandler(rec, httptest.NewRequest(http.MethodGet, "/api/stream", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("client past the cap got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("a 503 from a capacity cap should carry Retry-After")
	}
	// The refusal must not masquerade as a stream: http.Error's content type has to win, which it
	// only does because the cap is checked before the SSE headers are set.
	if ct := rec.Header().Get("Content-Type"); ct == "text/event-stream" {
		t.Errorf("refusal was sent as Content-Type %q — the cap check is below the SSE headers", ct)
	}

	// Releasing a slot must let the next client in; the cap is a live gauge, not a high-water mark.
	held[0]()
	for b.active.Load() > 1 {
		if time.Now().After(deadline.Add(2 * time.Second)) {
			t.Fatal("a released slot was never returned")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if b.active.Load() >= b.maxClients {
		t.Fatalf("active = %d after a release, want < %d", b.active.Load(), b.maxClients)
	}

	for _, c := range held[1:] {
		c()
	}
	wg.Wait()
}

func TestMaxClientsFromEnv(t *testing.T) {
	t.Run("absent uses default", func(t *testing.T) {
		t.Setenv("VIZ_MAX_SSE_CLIENTS", "")
		if got := maxClientsFromEnv(); got != defaultMaxSSEClients {
			t.Fatalf("got %d, want %d", got, defaultMaxSSEClients)
		}
	})
	t.Run("valid override", func(t *testing.T) {
		t.Setenv("VIZ_MAX_SSE_CLIENTS", "7")
		if got := maxClientsFromEnv(); got != 7 {
			t.Fatalf("got %d, want 7", got)
		}
	})
	// A throttle falls back rather than refusing to boot: failing to start the dashboard gateway
	// over a mistyped cap is worse than running with the documented default.
	for _, v := range []string{"banana", "0", "-3"} {
		t.Run("invalid falls back: "+v, func(t *testing.T) {
			t.Setenv("VIZ_MAX_SSE_CLIENTS", v)
			if got := maxClientsFromEnv(); got != defaultMaxSSEClients {
				t.Fatalf("got %d, want the default %d", got, defaultMaxSSEClients)
			}
		})
	}
}
