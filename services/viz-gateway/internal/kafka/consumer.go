package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"omniflow/services/viz-gateway/internal/api"
	"omniflow/services/viz-gateway/internal/domain"

	"omniflow/services/viz-gateway/internal/delivery"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client *kgo.Client
	broker *api.SSEBroker

	// highWatermark is the largest resolved timestamp broadcast so far. Two changefeeds (p2p and
	// inventory) each emit their own resolved messages, and the client keeps ONE settlement
	// cursor — so if the inventory feed's watermark lagged the p2p feed's, the line on the ledger
	// moved backwards and settled rows flipped to in-flight. The watermark is monotonic here so
	// the client never sees it retreat.
	highWatermark string
}

func NewConsumer(client *kgo.Client, broker *api.SSEBroker) *Consumer {
	return &Consumer{
		client: client,
		broker: broker,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		// See the equivalent check in the other consumers: a cancelled context yields a fetch
		// that IsClientClosed does not recognise, so without this the loop hot-spins on SIGTERM.
		if ctx.Err() != nil {
			return
		}
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("fetch error", "topic", topic, "partition", partition, "error", err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			switch record.Topic {
			case "omniflow.inventory.fact_inventory_movement", "omniflow.inventory.fact_inventory_snapshot":
				c.handleInventoryMovement(record)
			case "omniflow.p2p.completed.v1":
				c.handleP2PCompleted(record)
			}
			// CommitRecords, not MarkCommitRecords: the latter is a no-op unless the client was
			// built with AutoCommitMarks, so this gateway was committing purely on franz-go's
			// autocommit timer. Committing here means the offset advances only after the record
			// has been handed to the SSE broker.
			// Bounded, and not cancelled by the parent, so a SIGTERM mid-commit does not abandon an
			// offset whose projection has already been broadcast. See internal/delivery.
			dctx, dcancel := delivery.Context(ctx)
			err := c.client.CommitRecords(dctx, record)
			// Cancelled explicitly, not deferred: this runs once per record, and a deferred cancel
			// here would pile up one live context per message until the enclosing function returns.
			dcancel()
			if err != nil {
				slog.Error("offset commit failed", "error", err,
					"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
			}
		})
	}
}

func parsePayload(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber() // CRITICAL: prevent JS float64 precision loss for uint64/int64
	err := decoder.Decode(&result)
	return result, err
}

func (c *Consumer) handleInventoryMovement(record *kgo.Record) {
	payload, err := parsePayload(record.Value)
	if err != nil {
		return
	}

	// 1. Check for watermark (CRDB native changefeed resolved timestamp)
	if resolvedRaw, ok := payload["resolved"]; ok {
		c.emitWatermark(extractString(resolvedRaw))
		return
	}

	// 2. Map movement payload
	afterRaw, ok := payload["after"]
	if !ok {
		return
	}
	after, ok := afterRaw.(map[string]interface{})
	if !ok {
		return
	}

	// Safely extract sequence engine key as string
	seqKey := extractString(after["sequence_engine_key"])
	if seqKey == "" {
		return
	}

	occurredAt, _ := time.Parse(time.RFC3339Nano, extractString(after["occurred_at"]))

	proj := domain.ProjectionEvent{
		AggregateID:       extractString(after["event_id"]),
		Stage:             domain.StageReceived,
		Status:            "SUCCESS",
		SequenceEngineKey: seqKey, // string guaranteed
		OccurredAt:        occurredAt,
		TraceParent:       extractString(after["trace_parent"]),
		Metrics: &domain.Metrics{
			Value: parseFloat(after["fifo_total_value"]),
		},
	}

	c.broker.Broadcast(api.SSEEvent{
		ID:   seqKey,
		Type: api.EventMovement,
		Data: proj,
	})
}

func (c *Consumer) handleP2PCompleted(record *kgo.Record) {
	payload, err := parsePayload(record.Value)
	if err != nil {
		return
	}

	// 1. Watermark (CRDB native changefeed resolved timestamp) — mirror the inventory path so the
	// client keeps a single ordering signal across both streams.
	if resolvedRaw, ok := payload["resolved"]; ok {
		c.emitWatermark(extractString(resolvedRaw))
		return
	}

	// 2. The orchestrator_outbox changefeed wraps the row in an `after` envelope. The viz gateway is
	// deliberately JSON-native: it reads the projection columns and NEVER decodes the protobuf
	// `payload` BYTES. sequence_engine_key is the additive projection column populated from the
	// owning workflow.
	afterRaw, ok := payload["after"]
	if !ok {
		return
	}
	after, ok := afterRaw.(map[string]interface{})
	if !ok {
		// tombstone / delete — nothing to project
		return
	}

	// Read the HLC key as a STRING (UseNumber) — never float64 (JS precision loss on uint64).
	seqKey := extractString(after["sequence_engine_key"])
	if seqKey == "" {
		return
	}

	occurredAt, _ := time.Parse(time.RFC3339Nano, extractString(after["occurred_at"]))

	stage, status := domain.ProjectOutboxEvent(extractString(after["event_type"]))
	proj := domain.ProjectionEvent{
		AggregateID:       extractString(after["aggregate_id"]),
		Stage:             stage,
		Status:            status,
		SequenceEngineKey: seqKey, // string guaranteed
		OccurredAt:        occurredAt,
		TraceParent:       extractString(after["trace_parent"]),
	}

	c.broker.Broadcast(api.SSEEvent{
		ID:   seqKey,
		Type: api.EventMovement,
		Data: proj,
	})
}

func extractString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func parseFloat(val interface{}) float64 {
	switch v := val.(type) {
	case json.Number:
		f, _ := v.Float64()
		return f
	default:
		return 0
	}
}

// emitWatermark broadcasts a resolved timestamp only if it advances the high watermark. Resolved
// timestamps are HLC strings ("1790023750749228000.0000000000"); they compare correctly as
// numbers, which for equal-length integer parts is the same as comparing the strings — and the
// integer part is fixed-width nanoseconds for any timestamp this century.
func (c *Consumer) emitWatermark(resolved string) {
	if resolved == "" || !watermarkAdvances(c.highWatermark, resolved) {
		return
	}
	c.highWatermark = resolved
	c.broker.Broadcast(api.SSEEvent{
		ID:   resolved,
		Type: api.EventWatermark,
		Data: api.Watermark{ResolvedTS: resolved},
	})
}

// watermarkAdvances reports whether next > current, comparing the integer parts numerically and
// the fractional parts lexically (they are fixed-width).
func watermarkAdvances(current, next string) bool {
	if current == "" {
		return true
	}
	ci, cf := splitHLC(current)
	ni, nf := splitHLC(next)
	if len(ni) != len(ci) {
		return len(ni) > len(ci)
	}
	if ni != ci {
		return ni > ci
	}
	return nf > cf
}

func splitHLC(s string) (intPart, frac string) {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}
