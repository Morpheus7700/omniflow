package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1 "omniflow/contracts/communication/v1"
	"omniflow/services/p2p-orchestrator/internal/core/domain"

	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
)

// decodeChangefeedBytes decodes a BYTES column emitted by a CockroachDB JSON changefeed.
// CRDB renders BYTES as a `\x`-prefixed hex string (bytea format), NOT base64 — but we accept
// base64 as a fallback so the consumer is correct regardless of the exact CRDB emit format.
func decodeChangefeedBytes(s string) ([]byte, error) {
	if strings.HasPrefix(s, `\x`) {
		return hex.DecodeString(s[2:])
	}
	return base64.StdEncoding.DecodeString(s)
}

type OrchestratorService interface {
	ProcessEvent(ctx context.Context, payload []byte, isApproval bool) error
}

type Consumer struct {
	client  *kgo.Client
	service OrchestratorService
}

func NewConsumer(c *kgo.Client, svc OrchestratorService) *Consumer {
	return &Consumer{client: c, service: svc}
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("Kafka error", "topic", topic, "partition", partition, "error", err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			c.processMessageWithRetry(ctx, record)
		})
	}
}

func (c *Consumer) processMessageWithRetry(ctx context.Context, msg *kgo.Record) {
	topic := msg.Topic

	// Captured before the changefeed envelope is unwrapped below, because msg.Value is
	// overwritten in place and the DLQ must carry what the source topic actually published.
	originalValue := msg.Value

	var traceParent string
	var isApproval bool

	if topic == "omniflow.p2p.approval.v1" {
		isApproval = true
		var env v1.HumanApprovalEvent
		if err := proto.Unmarshal(msg.Value, &env); err != nil {
			c.routeToDLQ(ctx, msg, originalValue, err)
			return
		}
		traceParent = env.TraceParent
	} else {
		var env struct {
			Resolved interface{} `json:"resolved"`
			After    struct {
				Payload string `json:"payload"`
			} `json:"after"`
		}

		if err := json.Unmarshal(msg.Value, &env); err != nil {
			c.routeToDLQ(ctx, msg, originalValue, err)
			return
		}

		// Skip resolved-timestamp messages without erroring
		if env.Resolved != nil {
			c.commitOffset(ctx, msg)
			return
		}

		if env.After.Payload == "" {
			// tombstone / delete or empty row — nothing to process
			c.commitOffset(ctx, msg)
			return
		}
		payloadBytes, err := decodeChangefeedBytes(env.After.Payload)
		if err != nil {
			c.routeToDLQ(ctx, msg, originalValue, err)
			return
		}

		var payload v1.VendorEmailReceived
		if err := proto.Unmarshal(payloadBytes, &payload); err != nil {
			c.routeToDLQ(ctx, msg, originalValue, err)
			return
		}
		traceParent = payload.TraceParent

		// Update msg.Value so the underlying service.ProcessEvent receives the unmarshaled payload bytes
		msg.Value = payloadBytes
	}

	carrier := propagation.MapCarrier{"traceparent": traceParent}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	tracer := otel.Tracer("p2p-orchestrator-consumer")
	ctx, span := tracer.Start(ctx, "ConsumeMessage")
	defer span.End()

	maxRetries := 5
	backoff := 100 * time.Millisecond
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = c.service.ProcessEvent(ctx, msg.Value, isApproval)
		if err == nil {
			c.commitOffset(ctx, msg)
			return
		}

		if errors.Is(err, domain.ErrTerminal) {
			slog.Error("Terminal error in orchestrator", "error", err, "attempt", attempt,
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
			c.routeToDLQ(ctx, msg, originalValue, err)
			return
		}

		if errors.Is(err, domain.ErrTransient) {
			slog.Warn("Transient error, retrying in-place", "error", err, "attempt", attempt,
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			continue
		}

		// Unclassified. This used to log "assuming transient for safety" and retry, which was not
		// safe at all: the retry budget is five attempts over ~3.1 seconds, so every unnameable
		// failure bought three seconds of latency and then dead-lettered anyway. Worse, the store
		// classified only 55P03, so CockroachDB's routine 40001 serialization_failure — the
		// expected steady-state signal under contention — landed here and dead-lettered valid
		// business events. 40001 is now classified transient in internal/platform/errclass, which
		// leaves this branch for genuinely unknown errors, and those fail closed.
		slog.Error("Unclassified error, failing closed to DLQ", "error", err, "attempt", attempt,
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
		c.routeToDLQ(ctx, msg, originalValue, fmt.Errorf("unclassified error (defaulting terminal): %w", err))
		return
	}

	slog.Error("Exhausted retries, routing to DLQ",
		"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	c.routeToDLQ(ctx, msg, originalValue, fmt.Errorf("transient retries exhausted after %d attempts: %w", maxRetries, err))
}

// routeToDLQ sends a failed record to the dead-letter topic of the topic it came from.
//
// This previously hardcoded "omniflow.orchestration.v1.dlq" for every failure, but this consumer
// subscribes to TWO topics carrying DIFFERENT wire formats: omniflow.orchestration.v1 carries a
// CockroachDB changefeed JSON envelope, while omniflow.p2p.approval.v1 carries a bare protobuf
// HumanApprovalEvent. Mixing them in one dead-letter topic meant a re-drive tool had no way to
// know how to decode a given record, and replaying a dead-lettered approval back into the
// orchestration topic re-poisoned it immediately — an infinite DLQ ping-pong. Approval-DLQ volume
// was also invisible to any alarm watching the orchestration DLQ.
//
// dlqValue is the ORIGINAL record value. processMessageWithRetry overwrites msg.Value with the
// unwrapped inner protobuf before the retry loop, so reading msg.Value here would dead-letter a
// payload that no longer matches the source topic's format and is therefore not replayable.
func (c *Consumer) routeToDLQ(ctx context.Context, msg *kgo.Record, dlqValue []byte, err error) {
	topic := msg.Topic + ".dlq"

	headers := make([]kgo.RecordHeader, 0, len(msg.Headers)+2)
	headers = append(headers, msg.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: "error_reason", Value: []byte(err.Error())},
		kgo.RecordHeader{Key: "source_topic", Value: []byte(msg.Topic)},
	)

	slog.Error("routing to DLQ", "error", err, "dlq_topic", topic,
		"source_topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)

	dlqRecord := &kgo.Record{
		Topic:   topic,
		Key:     msg.Key,
		Value:   dlqValue,
		Headers: headers,
	}

	errProduce := c.client.ProduceSync(ctx, dlqRecord).FirstErr()
	if errProduce != nil {
		slog.Error("Failed to enqueue to DLQ", "error", errProduce)
		return
	}
	c.commitOffset(ctx, msg)
}

func (c *Consumer) commitOffset(ctx context.Context, msg *kgo.Record) {
	err := c.client.CommitRecords(ctx, msg)
	if err != nil {
		slog.Error("Failed to commit offset", "error", err)
	}
}
