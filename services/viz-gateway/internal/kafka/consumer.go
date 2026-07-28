package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"omniflow/services/viz-gateway/internal/api"
	"omniflow/services/viz-gateway/internal/domain"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client *kgo.Client
	broker *api.SSEBroker
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
			if record.Topic == "omniflow.inventory.fact_inventory_movement" || record.Topic == "omniflow.inventory.fact_inventory_snapshot" {
				c.handleInventoryMovement(record)
			} else if record.Topic == "omniflow.p2p.completed.v1" {
				c.handleP2PCompleted(record)
			}
			// CommitRecords, not MarkCommitRecords: the latter is a no-op unless the client was
			// built with AutoCommitMarks, so this gateway was committing purely on franz-go's
			// autocommit timer. Committing here means the offset advances only after the record
			// has been handed to the SSE broker.
			if err := c.client.CommitRecords(ctx, record); err != nil {
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
		resolvedStr := extractString(resolvedRaw)
		c.broker.Broadcast(api.SSEEvent{
			ID:   resolvedStr,
			Type: api.EventWatermark,
			Data: map[string]string{"resolved_ts": resolvedStr},
		})
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
		resolvedStr := extractString(resolvedRaw)
		c.broker.Broadcast(api.SSEEvent{
			ID:   resolvedStr,
			Type: api.EventWatermark,
			Data: map[string]string{"resolved_ts": resolvedStr},
		})
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

	proj := domain.ProjectionEvent{
		AggregateID:       extractString(after["aggregate_id"]),
		Stage:             domain.StagePOCreated,
		Status:            extractString(after["event_type"]), // 'NodeTransition'
		SequenceEngineKey: seqKey,                             // string guaranteed
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
