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
)

type EventType string

const (
	EventMovement  EventType = "movement"
	EventWatermark EventType = "watermark"
)

type SSEEvent struct {
	ID   string // sequence_engine_key or resolved_ts as string
	Type EventType
	Data any
}

// defaultMaxSSEClients caps concurrent streams.
//
// An SSE connection is held open indefinitely by design, so without a cap the number of goroutines,
// buffered channels and open sockets this process holds is set entirely by how many times an
// anonymous caller chooses to connect. Each client also costs a 100-slot event buffer, and every
// broadcast iterates all of them — so an unbounded client count degrades delivery for the legitimate
// ones long before it exhausts memory. A cap turns that into an honest 503 for the marginal client.
const defaultMaxSSEClients = 100

type SSEBroker struct {
	clients       map[chan SSEEvent]bool
	newClients    chan chan SSEEvent
	closedClients chan chan SSEEvent
	broadcast     chan SSEEvent

	// active is counted in the handler rather than read from len(clients), because that map is owned
	// by the Run goroutine and reading it from a request goroutine would be a data race.
	active     atomic.Int64
	maxClients int64
}

func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients:       make(map[chan SSEEvent]bool),
		newClients:    make(chan chan SSEEvent),
		closedClients: make(chan chan SSEEvent),
		broadcast:     make(chan SSEEvent),
		maxClients:    maxClientsFromEnv(),
	}
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
			slog.Info("SSE client connected", "active_clients", len(b.clients))
		case client := <-b.closedClients:
			delete(b.clients, client)
			close(client)
			slog.Info("SSE client disconnected", "active_clients", len(b.clients))
		case event := <-b.broadcast:
			for client := range b.clients {
				select {
				case client <- event:
				case <-time.After(2 * time.Second):
					slog.Warn("Dropping event to slow client", "event_type", event.Type)
				}
			}
		case <-ticker.C:
			// Heartbeat comment to keep proxies alive
			for client := range b.clients {
				select {
				case client <- SSEEvent{Type: "heartbeat"}:
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
		http.Error(w, "too many concurrent stream clients", http.StatusServiceUnavailable)
		return
	}
	defer b.active.Add(-1)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	clientChan := make(chan SSEEvent, 100)
	b.newClients <- clientChan

	defer func() {
		b.closedClients <- clientChan
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-clientChan:
			if event.Type == "heartbeat" {
				fmt.Fprintf(w, ": heartbeat\n\n")
				flusher.Flush()
				continue
			}

			dataBytes, err := json.Marshal(event.Data)
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "id: %s\n", event.ID)
			fmt.Fprintf(w, "event: %s\n", event.Type)
			fmt.Fprintf(w, "data: %s\n\n", string(dataBytes))
			flusher.Flush()
		}
	}
}
