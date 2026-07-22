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
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("fetch error", "topic", topic, "partition", partition, "error", err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			if record.Topic == "fact_inventory_movement" {
				c.handleInventoryMovement(record)
			} else if record.Topic == "omniflow.p2p.completed.v1" {
				c.handleP2PCompleted(record)
			}
			c.client.MarkCommitRecords(record)
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

	seqKey := extractString(payload["sequence_engine_key"])
	if seqKey == "" {
		// Mock a seq key for testing if the orchestrator doesn't have it yet
		seqKey = "0"
	}

	proj := domain.ProjectionEvent{
		AggregateID:       extractString(payload["po_id"]),
		Stage:             domain.StagePOCreated,
		Status:            extractString(payload["status"]),
		SequenceEngineKey: seqKey,
		OccurredAt:        time.Now(),
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
