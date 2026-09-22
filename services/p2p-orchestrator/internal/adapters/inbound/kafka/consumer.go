package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1 "omniflow/contracts/communication/v1"
	"omniflow/internal/platform/delivery"
	"omniflow/internal/platform/metrics"
	"omniflow/internal/platform/retry"
	"omniflow/services/p2p-orchestrator/internal/core/domain"

	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"

	"buf.build/go/protovalidate"
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
	// FailWorkflow is called with the workflow's event id before a record is dead-lettered, so a
	// workflow whose driving record is abandoned is marked FAILED rather than left RUNNING forever.
	FailWorkflow(ctx context.Context, eventID, nodeID, reason string) error
}

type Consumer struct {
	client    *kgo.Client
	service   OrchestratorService
	validator protovalidate.Validator
	retry     retry.Policy
}

const serviceName = "p2p-orchestrator"

// NewConsumer builds the validator eagerly. This consumer was the one of three that did not
// validate at the Kafka boundary at all — and its second topic is the approval control plane,
// where a message is an instruction to release a purchase order. Both wire formats are validated:
// the changefeed-wrapped VendorEmailReceived and the bare HumanApprovalEvent.
//
// WithDisableLazy + WithMessages: rules are compiled here, so a non-compiling ruleset is a boot
// failure rather than a per-message error that would dead-letter 100% of traffic while the
// constructor reported success.
func NewConsumer(c *kgo.Client, svc OrchestratorService) (*Consumer, error) {
	v, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&v1.VendorEmailReceived{}, &v1.HumanApprovalEvent{}),
	)
	if err != nil {
		return nil, fmt.Errorf("init protovalidate: %w", err)
	}
	return &Consumer{client: c, service: svc, validator: v, retry: retry.FromEnv()}, nil
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
	// The workflow this record drives, known once the payload decodes. Used to mark the workflow
	// FAILED if the record is abandoned to the DLQ; empty for records that never decoded.
	var eventID string

	if topic == "omniflow.p2p.approval.v1" {
		isApproval = true
		var env v1.HumanApprovalEvent
		if err := proto.Unmarshal(msg.Value, &env); err != nil {
			c.routeToDLQ(ctx, msg, originalValue, err)
			return
		}
		// An approval that fails validation is dead-lettered WITHOUT failing the workflow: the
		// workflow is still legitimately waiting, and a malformed approval must not be able to
		// abort it — that would let anyone who can produce garbage to the topic kill any PO.
		if err := c.validator.Validate(&env); err != nil {
			c.routeToDLQ(ctx, msg, originalValue, fmt.Errorf("%w: approval failed validation: %w", domain.ErrTerminal, err))
			return
		}
		traceParent = env.TraceParent
		eventID = env.EventId
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
			metrics.ConsumerRecords.WithLabelValues(serviceName, msg.Topic, metrics.OutcomeSkipped).Inc()
			return
		}

		if env.After.Payload == "" {
			// tombstone / delete or empty row — nothing to process
			c.commitOffset(ctx, msg)
			metrics.ConsumerRecords.WithLabelValues(serviceName, msg.Topic, metrics.OutcomeSkipped).Inc()
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
		if err := c.validator.Validate(&payload); err != nil {
			c.routeToDLQ(ctx, msg, originalValue, fmt.Errorf("%w: trigger failed validation: %w", domain.ErrTerminal, err))
			return
		}
		traceParent = payload.TraceParent
		eventID = payload.EventId

		// Update msg.Value so the underlying service.ProcessEvent receives the unmarshaled payload bytes
		msg.Value = payloadBytes
	}

	carrier := propagation.MapCarrier{"traceparent": traceParent}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	tracer := otel.Tracer("p2p-orchestrator-consumer")
	ctx, span := tracer.Start(ctx, "ConsumeMessage")
	defer span.End()

	maxRetries := c.retry.MaxAttempts
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = c.service.ProcessEvent(ctx, msg.Value, isApproval)
		if err == nil {
			c.commitOffset(ctx, msg)
			metrics.ConsumerRecords.WithLabelValues(serviceName, msg.Topic, metrics.OutcomeOK).Inc()
			return
		}

		if errors.Is(err, domain.ErrTerminal) {
			slog.Error("Terminal error in orchestrator", "error", err, "attempt", attempt,
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
			c.failThenDLQ(ctx, msg, originalValue, eventID, isApproval, err)
			return
		}

		if errors.Is(err, domain.ErrTransient) {
			slog.Warn("Transient error, retrying in-place", "error", err, "attempt", attempt,
				"max_attempts", maxRetries, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
			metrics.ConsumerRetries.WithLabelValues(serviceName, msg.Topic).Inc()
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.retry.Backoff(attempt)):
			}
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
		c.failThenDLQ(ctx, msg, originalValue, eventID, isApproval, fmt.Errorf("unclassified error (defaulting terminal): %w", err))
		return
	}

	slog.Error("Exhausted retries, routing to DLQ",
		"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	c.failThenDLQ(ctx, msg, originalValue, eventID, isApproval, fmt.Errorf("transient retries exhausted after %d attempts: %w", maxRetries, err))
}

// failThenDLQ marks the workflow FAILED, then dead-letters the record. The order matters and so
// does the independence: the failure is recorded first so the DLQ record and the FAILED row agree,
// but a failure to record it (the workflow may not exist yet, or its row may be locked) must
// never block the dead-letter — the confirmed-DLQ-before-commit contract is what guarantees the
// message is not lost, and it takes precedence.
//
// An abandoned APPROVAL does not fail the workflow. The workflow is still waiting, correctly; the
// approval sweep decides when waiting has gone on too long.
func (c *Consumer) failThenDLQ(ctx context.Context, msg *kgo.Record, dlqValue []byte, eventID string, isApproval bool, cause error) {
	if eventID != "" && !isApproval {
		if err := c.service.FailWorkflow(ctx, eventID, "", cause.Error()); err != nil {
			slog.Error("could not mark workflow FAILED before dead-lettering", "event_id", eventID, "error", err)
		}
	}
	c.routeToDLQ(ctx, msg, dlqValue, cause)
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

	// Bounded, and deliberately not cancelled by the parent: this is the DLQ half of the
	// confirmed-DLQ-before-commit contract. See internal/platform/delivery.
	dctx, dcancel := delivery.Context(ctx)
	defer dcancel()
	errProduce := c.client.ProduceSync(dctx, dlqRecord).FirstErr()
	if errProduce != nil {
		slog.Error("Failed to enqueue to DLQ", "error", errProduce)
		return
	}
	c.commitOffset(ctx, msg)
	metrics.ConsumerRecords.WithLabelValues(serviceName, msg.Topic, metrics.OutcomeDLQ).Inc()
}

func (c *Consumer) commitOffset(ctx context.Context, msg *kgo.Record) {
	// Bounded, and not cancelled by the parent — a SIGTERM mid-handoff must not abandon the commit
	// that the confirmed DLQ write has already earned. See internal/platform/delivery.
	dctx, dcancel := delivery.Context(ctx)
	defer dcancel()
	err := c.client.CommitRecords(dctx, msg)
	if err != nil {
		slog.Error("Failed to commit offset", "error", err)
	}
}
