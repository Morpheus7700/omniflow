package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type EventType string

const (
	EventMovement  EventType = "movement"
	EventWatermark EventType = "watermark"
)

type SSEEvent struct {
	ID   string    // sequence_engine_key or resolved_ts as string
	Type EventType
	Data any
}

type SSEBroker struct {
	clients       map[chan SSEEvent]bool
	newClients    chan chan SSEEvent
	closedClients chan chan SSEEvent
	broadcast     chan SSEEvent
}

func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients:       make(map[chan SSEEvent]bool),
		newClients:    make(chan chan SSEEvent),
		closedClients: make(chan chan SSEEvent),
		broadcast:     make(chan SSEEvent),
	}
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
