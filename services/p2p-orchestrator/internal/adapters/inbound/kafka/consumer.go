package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"omniflow/services/p2p-orchestrator/internal/core/domain"
	v1 "omniflow/contracts/communication/v1"
	
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	consumer *kafka.Consumer
	dlq      *kafka.Producer
	service  OrchestratorService
}

func NewConsumer(c *kafka.Consumer, p *kafka.Producer, svc OrchestratorService) *Consumer {
	return &Consumer{consumer: c, dlq: p, service: svc}
}

func (c *Consumer) Start(ctx context.Context) {
	// Subscribe to both changefeed and external approval events
	_ = c.consumer.SubscribeTopics([]string{"omniflow.orchestration.v1", "omniflow.p2p.approval.v1"}, nil)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				c.processMessageWithRetry(ctx, e)
			case kafka.Error:
				slog.Error("Kafka error", "error", e)
			}
		}
	}
}

func (c *Consumer) processMessageWithRetry(ctx context.Context, msg *kafka.Message) {
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

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
			c.commitOffset(msg)
			return
		}

		if env.After.Payload == "" {
			// tombstone / delete or empty row — nothing to process
			c.commitOffset(msg)
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
			c.commitOffset(msg)
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

func (c *Consumer) routeToDLQ(ctx context.Context, msg *kafka.Message, err error) {
	topic := "omniflow.orchestration.v1.dlq"
	deliveryChan := make(chan kafka.Event)

	errProduce := c.dlq.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            msg.Key,
		Value:          msg.Value,
		Headers: []kafka.Header{
			{Key: "error_reason", Value: []byte(err.Error())},
		},
	}, deliveryChan)

	if errProduce != nil {
		slog.Error("Failed to enqueue to DLQ", "error", errProduce)
		return
	}

	select {
	case <-ctx.Done():
		slog.Warn("Context cancelled while awaiting DLQ delivery")
		return
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			slog.Error("DLQ delivery failed", "error", m.TopicPartition.Error)
			return
		}
		c.commitOffset(msg)
	}
}

func (c *Consumer) commitOffset(msg *kafka.Message) {
	_, err := c.consumer.CommitMessage(msg)
	if err != nil {
		slog.Error("Failed to commit offset", "error", err)
	}
}
