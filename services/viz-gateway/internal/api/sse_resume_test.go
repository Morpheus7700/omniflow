package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"omniflow/services/viz-gateway/internal/domain"
)

type fakeResumer struct {
	calls []uint64
	rows  []domain.ProjectionEvent
}

func (f *fakeResumer) GetMovements(_ context.Context, fromSeq, _ uint64, _ int) ([]domain.ProjectionEvent, error) {
	f.calls = append(f.calls, fromSeq)
	var out []domain.ProjectionEvent
	for _, r := range f.rows {
		if k, _ := parseSeq(r.SequenceEngineKey, 0); k >= fromSeq {
			out = append(out, r)
		}
	}
	return out, nil
}

// A reconnecting browser sends Last-Event-ID; the gateway keeps no backlog, so the events broadcast
// during the outage must come from the replay repository — and from the key AFTER the last one the
// client saw, not from zero.
func TestStreamResumesFromLastEventID(t *testing.T) {
	res := &fakeResumer{rows: []domain.ProjectionEvent{
		{AggregateID: "a", SequenceEngineKey: "100", Stage: domain.StageApproved, Status: domain.StatusSuccess},
		{AggregateID: "b", SequenceEngineKey: "101", Stage: domain.StageApproved, Status: domain.StatusSuccess},
		{AggregateID: "c", SequenceEngineKey: "102", Stage: domain.StagePOCreated, Status: domain.StatusFailure},
	}}
	b := NewSSEBroker().WithResumer(res)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	srv := httptest.NewServer(http.HandlerFunc(b.StreamHandler))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("Last-Event-ID", "100")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no (proxies buffer SSE otherwise)", got)
	}

	frames := readFrames(t, resp.Body, 2, 3*time.Second)
	if len(res.calls) != 1 || res.calls[0] != 101 {
		t.Fatalf("resume queried from %v, want [101] (the key after Last-Event-ID)", res.calls)
	}
	if frames[0]["id"] != "101" || frames[1]["id"] != "102" {
		t.Fatalf("resumed ids = %q, %q; want 101, 102", frames[0]["id"], frames[1]["id"])
	}
	var ev domain.ProjectionEvent
	if err := json.Unmarshal([]byte(frames[1]["data"]), &ev); err != nil || ev.Status != domain.StatusFailure {
		t.Fatalf("resumed frame data = %s (%v)", frames[1]["data"], err)
	}
}

func TestStreamAdvertisesRetryAndIgnoresGarbageLastEventID(t *testing.T) {
	res := &fakeResumer{}
	b := NewSSEBroker().WithResumer(res)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)
	srv := httptest.NewServer(http.HandlerFunc(b.StreamHandler))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("Last-Event-ID", "not-a-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, _ := bufio.NewReader(resp.Body).ReadString('\n')
	if !strings.HasPrefix(line, "retry: ") {
		t.Fatalf("first line = %q, want a retry: directive", line)
	}
	if len(res.calls) != 0 {
		t.Fatalf("garbage Last-Event-ID triggered a resume query: %v", res.calls)
	}
}

// A client that stops reading must not stall everyone else. It is evicted once its buffer is
// full, and its stream ends so the browser reconnects (with Last-Event-ID) instead of hanging.
func TestSlowClientIsEvictedNotWaitedFor(t *testing.T) {
	b := NewSSEBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// A raw client channel, never drained, registered directly with the broker.
	slow := make(chan SSEEvent, clientBuffer)
	b.newClients <- slow
	// And a healthy client we keep draining.
	fast := make(chan SSEEvent, clientBuffer)
	b.newClients <- fast
	drained := make(chan struct{})
	go func() {
		for range fast {
		}
		close(drained)
	}()

	start := time.Now()
	for i := 0; i < clientBuffer+5; i++ {
		b.Broadcast(SSEEvent{ID: "x", Type: EventMovement, Data: map[string]string{}})
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("broadcasting past a slow client took %v — the old per-client 2s wait is back", elapsed)
	}
	// The slow channel was closed by the broker: reading past its buffered content yields !ok.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, ok := <-slow
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slow client was never evicted")
		}
	}
	b.closedClients <- fast
	<-drained
}

// readFrames parses n `id/event/data` blocks from an SSE body, skipping the retry line and
// heartbeat comments.
func readFrames(t *testing.T, body io.Reader, n int, timeout time.Duration) []map[string]string {
	t.Helper()
	out := make(chan []map[string]string, 1)
	go func() {
		sc := bufio.NewScanner(body)
		var frames []map[string]string
		cur := map[string]string{}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if len(cur) > 0 {
					frames = append(frames, cur)
					cur = map[string]string{}
					if len(frames) == n {
						out <- frames
						return
					}
				}
			case strings.HasPrefix(line, ":"), strings.HasPrefix(line, "retry:"):
			default:
				k, v, _ := strings.Cut(line, ": ")
				cur[k] = v
			}
		}
		out <- frames
	}()
	select {
	case f := <-out:
		if len(f) < n {
			t.Fatalf("got %d frames, want %d", len(f), n)
		}
		return f
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %d frames", n)
		return nil
	}
}
