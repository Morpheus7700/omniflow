package kafka

import (
	"context"
	"errors"
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

	var traceParent string
	var isApproval bool

	if topic == "omniflow.p2p.approval.v1" {
		isApproval = true
		var env v1.HumanApprovalEvent
		if err := proto.Unmarshal(msg.Value, &env); err != nil {
			c.routeToDLQ(ctx, msg, err)
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
			c.routeToDLQ(ctx, msg, err)
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
			c.routeToDLQ(ctx, msg, err)
			return
		}

		var payload v1.VendorEmailReceived
		if err := proto.Unmarshal(payloadBytes, &payload); err != nil {
			c.routeToDLQ(ctx, msg, err)
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

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.service.ProcessEvent(ctx, msg.Value, isApproval)
		if err == nil {
			c.commitOffset(ctx, msg)
			return
		}

		if errors.Is(err, domain.ErrTerminal) {
			slog.Error("Terminal error in orchestrator", "error", err, "attempt", attempt)
			c.routeToDLQ(ctx, msg, err)
			return
		}

		if errors.Is(err, domain.ErrTransient) {
			slog.Warn("Transient error, retrying in-place", "error", err, "attempt", attempt)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			continue
		}

		slog.Warn("Unknown error, assuming transient for safety", "error", err, "attempt", attempt)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
	}

	slog.Error("Exhausted retries, routing to DLQ")
	c.routeToDLQ(ctx, msg, errors.New("retries exhausted"))
}

func (c *Consumer) routeToDLQ(ctx context.Context, msg *kgo.Record, err error) {
	topic := "omniflow.orchestration.v1.dlq"

	dlqRecord := &kgo.Record{
		Topic: topic,
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers, kgo.RecordHeader{
			Key:   "error_reason",
			Value: []byte(err.Error()),
		}),
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
