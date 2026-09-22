package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"omniflow/services/viz-gateway/internal/domain"
)

type EventType string

const (
	EventMovement  EventType = "movement"
	EventWatermark EventType = "watermark"
	eventHeartbeat EventType = "heartbeat"
)

type SSEEvent struct {
	ID   string // sequence_engine_key or resolved_ts as string
	Type EventType
	Data any
}

// Watermark is the payload of a `watermark` frame: the changefeed's resolved timestamp, as a
// string. Mirrored by WatermarkEvent in frontend/src/store/index.ts.
type Watermark struct {
	ResolvedTS string `json:"resolved_ts"`
}

// defaultMaxSSEClients caps concurrent streams.
//
// An SSE connection is held open indefinitely by design, so without a cap the number of goroutines,
// buffered channels and open sockets this process holds is set entirely by how many times an
// anonymous caller chooses to connect. Each client also costs a clientBuffer-slot event buffer, and
// every broadcast iterates all of them — so an unbounded client count degrades delivery for the
// legitimate ones long before it exhausts memory. A cap turns that into an honest 503.
const defaultMaxSSEClients = 100

// clientBuffer is how far behind a client may fall before it is evicted. A client that cannot drain
// this many events is not going to catch up on its own; closing it lets the browser reconnect with
// Last-Event-ID and resume from the replay endpoint, which is what SSE is designed to do.
const clientBuffer = 100

// retryMillis is the reconnect delay the stream advertises. The browser default (~3s) is fine;
// stating it makes the behaviour explicit and lets a proxy operator see it.
const retryMillis = 3000

// Resumer supplies the events a reconnecting client missed. Implemented by the replay repository;
// narrowed to an interface so the broker's tests can drive it with a fake.
type Resumer interface {
	GetMovements(ctx context.Context, fromSeq, toSeq uint64, limit int) ([]domain.ProjectionEvent, error)
}

type SSEBroker struct {
	clients       map[chan SSEEvent]bool
	newClients    chan chan SSEEvent
	closedClients chan chan SSEEvent
	broadcast     chan SSEEvent

	// active is counted in the handler rather than read from len(clients), because that map is owned
	// by the Run goroutine and reading it from a request goroutine would be a data race.
	active     atomic.Int64
	maxClients int64

	// resumer, when set, backs Last-Event-ID resumption; resumeLimit bounds one catch-up.
	resumer     Resumer
	resumeLimit int
}

func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients:       make(map[chan SSEEvent]bool),
		newClients:    make(chan chan SSEEvent),
		closedClients: make(chan chan SSEEvent),
		broadcast:     make(chan SSEEvent),
		maxClients:    maxClientsFromEnv(),
		resumeLimit:   maxReplayLimit,
	}
}

// WithResumer enables Last-Event-ID resumption. Without it a reconnecting browser silently loses
// every event broadcast during the outage — the gateway keeps no backlog — which is the failure
// the frontend audit found: the `id:` field was written on every frame and read by nobody.
func (b *SSEBroker) WithResumer(r Resumer) *SSEBroker {
	b.resumer = r
	return b
}

// maxClientsFromEnv reads VIZ_MAX_SSE_CLIENTS, falling back to the default for absent, malformed, or
// non-positive values. Unlike the database timeout this falls back rather than refusing to start: a
// stream cap is a throttle, and refusing to boot the dashboard gateway over a mistyped throttle
// would be a worse outcome than running with the documented default.
func maxClientsFromEnv() int64 {
	v := os.Getenv("VIZ_MAX_SSE_CLIENTS")
	if v == "" {
		return defaultMaxSSEClients
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		slog.Warn("ignoring invalid VIZ_MAX_SSE_CLIENTS", "value", v, "using", defaultMaxSSEClients)
		return defaultMaxSSEClients
	}
	return n
}

func (b *SSEBroker) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case client := <-b.newClients:
			b.clients[client] = true
			sseClients.Set(float64(len(b.clients)))
			slog.Info("SSE client connected", "active_clients", len(b.clients))
		case client := <-b.closedClients:
			if b.clients[client] {
				delete(b.clients, client)
				close(client)
			}
			sseClients.Set(float64(len(b.clients)))
			slog.Info("SSE client disconnected", "active_clients", len(b.clients))
		case event := <-b.broadcast:
			// Non-blocking fan-out. The previous loop waited up to 2s PER slow client, serially,
			// so one stalled browser delayed every other client — and, upstream of this channel,
			// the Kafka consumer — by 2s per event. A client whose buffer is full is evicted
			// instead: it reconnects with Last-Event-ID and catches up from replay.
			for client := range b.clients {
				select {
				case client <- event:
				default:
					slog.Warn("evicting slow SSE client", "buffered", clientBuffer, "event_type", event.Type)
					delete(b.clients, client)
					close(client)
					sseEvictions.Inc()
				}
			}
			sseClients.Set(float64(len(b.clients)))
		case <-ticker.C:
			// Heartbeat comment to keep proxies alive. Never evicts: a full buffer is handled by
			// the next real event; a dropped heartbeat costs nothing.
			for client := range b.clients {
				select {
				case client <- SSEEvent{Type: eventHeartbeat}:
				default:
				}
			}
		}
	}
}

func (b *SSEBroker) Broadcast(event SSEEvent) {
	b.broadcast <- event
}

func (b *SSEBroker) StreamHandler(w http.ResponseWriter, r *http.Request) {
	// Reserve a slot before registering. Checked here, not inside Run, so a refused client never
	// allocates a buffer or occupies the broker's channel at all.
	if n := b.active.Add(1); n > b.maxClients {
		b.active.Add(-1)
		slog.Warn("refusing SSE client, cap reached", "max_clients", b.maxClients)
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "stream_capacity", "too many concurrent stream clients")
		return
	}
	defer b.active.Add(-1)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// nginx and similar buffer responses by default, which turns a live stream into a batch.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "retry: %d\n\n", retryMillis)
	flusher.Flush()

	clientChan := make(chan SSEEvent, clientBuffer)
	b.newClients <- clientChan
	defer func() {
		// Run may already have evicted (and closed) this channel; closedClients is idempotent.
		b.closedClients <- clientChan
	}()

	// Resume: register FIRST so nothing broadcast during the catch-up is lost, then replay what the
	// client missed. Overlap between the two is harmless — the client dedups on (aggregate, key).
	if last := r.Header.Get("Last-Event-ID"); last != "" {
		b.resume(r.Context(), w, flusher, last)
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-clientChan:
			if !ok {
				// Evicted by the broker for falling behind. Ending the response makes the browser
				// reconnect after `retry`, carrying Last-Event-ID.
				return
			}
			if event.Type == eventHeartbeat {
				fmt.Fprintf(w, ": heartbeat\n\n")
				flusher.Flush()
				continue
			}
			writeFrame(w, event)
			flusher.Flush()
		}
	}
}

func (b *SSEBroker) resume(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, last string) {
	if b.resumer == nil {
		return
	}
	from, err := strconv.ParseUint(last, 10, 64)
	if err != nil {
		// A resolved-timestamp id ("1790023750749228000.0000000000") is not a sequence key; take
		// its integer part. Anything else is ignored: resumption is best-effort.
		if dot := indexByte(last, '.'); dot > 0 {
			from, err = strconv.ParseUint(last[:dot], 10, 64)
		}
		if err != nil {
			return
		}
	}
	events, err := b.resumer.GetMovements(ctx, from+1, 0, b.resumeLimit)
	if err != nil {
		slog.Error("SSE resume query failed", "last_event_id", last, "error", err)
		return
	}
	for _, e := range events {
		writeFrame(w, SSEEvent{ID: e.SequenceEngineKey, Type: EventMovement, Data: e})
	}
	if len(events) > 0 {
		flusher.Flush()
		sseResumed.Add(float64(len(events)))
	}
	slog.Info("SSE client resumed", "last_event_id", last, "replayed", len(events))
}

func writeFrame(w http.ResponseWriter, event SSEEvent) {
	dataBytes, err := json.Marshal(event.Data)
	if err != nil {
		slog.Error("SSE frame encode failed", "event_type", event.Type, "error", err)
		return
	}
	fmt.Fprintf(w, "id: %s\n", event.ID)
	fmt.Fprintf(w, "event: %s\n", event.Type)
	fmt.Fprintf(w, "data: %s\n\n", string(dataBytes))
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// sseClients is the live connection count, the one number that says whether the cap
// (VIZ_MAX_SSE_CLIENTS) is about to turn a dashboard away.
var sseClients = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "omniflow_sse_clients",
	Help: "Connected SSE clients.",
})

var sseEvictions = promauto.NewCounter(prometheus.CounterOpts{
	Name: "omniflow_sse_evictions_total",
	Help: "SSE clients closed for falling more than the buffer behind.",
})

var sseResumed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "omniflow_sse_resumed_events_total",
	Help: "Events replayed to reconnecting clients via Last-Event-ID.",
})
