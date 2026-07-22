package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	communicationv1 "omniflow/contracts/communication/v1"
	"omniflow/services/commbot/internal/core/domain"

	"github.com/bufbuild/protovalidate-go"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	dlqTopic          = "omniflow.communication.v1.dlq"
	maxTransientRetry = 5
	pollTimeoutMs     = 250
	dlqFlushTimeoutMs = 5000
	initialBackoff    = 200 * time.Millisecond
)

// IdempotencyStore enforces exactly-once *effects* on an at-least-once stream.
// Implementations MUST persist the processed marker in the SAME transaction as the
// downstream publish (consumer-side outbox), or duplicates re-appear on crash.
type IdempotencyStore interface {
	AlreadyProcessed(ctx context.Context, eventID string) (bool, error)
}

type Consumer struct {
	consumer    *kafka.Consumer
	dlq         *kafka.Producer
	service     *domain.CommBotService
	validator   *protovalidate.Validator
	idempotency IdempotencyStore
	propagator  propagation.TextMapPropagator
	tracer      trace.Tracer
}

// NewConsumer REQUIRES a *kafka.Consumer created with `enable.auto.commit=false`.
// Auto-commit acknowledges offsets on poll regardless of processing outcome, which
// silently defeats the retry/DLQ contract below.
func NewConsumer(
	c *kafka.Consumer,
	dlq *kafka.Producer,
	svc *domain.CommBotService,
	idem IdempotencyStore,
	tp trace.TracerProvider,
) (*Consumer, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("init protovalidate: %w", err)
	}
	return &Consumer{
		consumer:    c,
		dlq:         dlq,
		service:     svc,
		validator:   v,
		idempotency: idem,
		propagator:  propagation.TraceContext{}, // real W3C traceparent propagation
		tracer:      tp.Tracer("commbot.adapter.kafka"),
	}, nil
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutdown: draining DLQ producer")
			c.dlq.Flush(dlqFlushTimeoutMs)
			return
		default:
			ev := c.consumer.Poll(pollTimeoutMs) // bounded poll → ctx is observed on every loop
			switch e := ev.(type) {
			case *kafka.Message:
				c.processMessage(ctx, e)
			case kafka.Error:
				if e.IsFatal() {
					slog.Error("fatal kafka error; stopping", "error", e)
					return
				}
				slog.Warn("kafka poll error", "error", e)
			default:
				// nil (timeout) / rebalance events: loop and re-check ctx.
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) {
	// Phase-1 reconciliation: the CRDB `format=protobuf` changefeed emits the outbox ROW.
	// If your changefeed emits the full row envelope, decode it and unmarshal the `payload`
	// column here. This assumes the feed is projected to the bare payload — verify against Phase 1.
	var pb communicationv1.VendorEmailReceived
	if err := proto.Unmarshal(msg.Value, &pb); err != nil {
		c.deadLetter(ctx, msg, fmt.Errorf("deserialization poison pill: %w", err))
		return
	}
	if err := c.validator.Validate(&pb); err != nil {
		c.deadLetter(ctx, msg, fmt.Errorf("protovalidate poison pill (event_id=%s): %w", pb.GetEventId(), err))
		return
	}

	// Real W3C traceparent extraction → child span of the producer's trace (not a new root).
	ctx = c.propagator.Extract(ctx, propagation.MapCarrier{"traceparent": pb.GetTraceParent()})
	ctx, span := c.tracer.Start(ctx, "Kafka.ConsumeVendorEmail")
	defer span.End()

	email, err := mapToDomain(&pb)
	if err != nil {
		// Malformed event is deterministic — retrying cannot help. Terminal → DLQ.
		c.deadLetter(ctx, msg, fmt.Errorf("malformed event: %w", err))
		return
	}

	// Bounded, backed-off retry of the SAME message. Blocking here preserves per-partition
	// ordering; we never advance the commit past an unresolved offset (offsets are a
	// high-water mark — committing N+1 acknowledges N).
	backoff := initialBackoff
	var procErr error
	for attempt := 1; attempt <= maxTransientRetry; attempt++ {
		procErr = c.handle(ctx, email)
		switch {
		case procErr == nil:
			c.commit(msg)
			return
		case errors.Is(procErr, errAlreadyProcessed):
			span.AddEvent("duplicate event; committing without reprocessing")
			c.commit(msg)
			return
		case errors.Is(procErr, domain.ErrInvalidQuarantineURI), errors.Is(procErr, domain.ErrTerminal):
			c.deadLetter(ctx, msg, fmt.Errorf("terminal processing error: %w", procErr))
			return
		case errors.Is(procErr, domain.ErrTransient):
			span.AddEvent(fmt.Sprintf("transient failure %d/%d: %v", attempt, maxTransientRetry, procErr))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
		default:
			// Unclassified: fail closed to terminal to avoid an infinite poison loop.
			c.deadLetter(ctx, msg, fmt.Errorf("unclassified error (defaulting terminal): %w", procErr))
			return
		}
	}
	// Retries exhausted → DLQ so the partition is not blocked forever.
	c.deadLetter(ctx, msg, fmt.Errorf("transient retries exhausted: %w", procErr))
}

var errAlreadyProcessed = errors.New("event already processed")

func (c *Consumer) handle(ctx context.Context, email *domain.VendorEmail) error {
	done, err := c.idempotency.AlreadyProcessed(ctx, email.EventID)
	if err != nil {
		return fmt.Errorf("%w: idempotency check: %v", domain.ErrTransient, err)
	}
	if done {
		return errAlreadyProcessed
	}
	if _, err := c.service.ProcessVendorEmail(ctx, email); err != nil {
		return err // already classified transient/terminal by the domain + gateway
	}
	return nil
}

// deadLetter publishes to the DLQ and commits the source offset ONLY after delivery is
// confirmed. A failed produce + committed offset = permanent data loss.
func (c *Consumer) deadLetter(ctx context.Context, msg *kafka.Message, cause error) {
	slog.Error("routing to DLQ", "error", cause, "partition", msg.TopicPartition)
	topic := dlqTopic
	deliveryChan := make(chan kafka.Event, 1)
	err := c.dlq.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            msg.Key,
		Value:          msg.Value,
		Headers:        []kafka.Header{{Key: "error_reason", Value: []byte(cause.Error())}},
	}, deliveryChan)
	if err != nil {
		slog.Error("DLQ enqueue failed; NOT committing (will re-read on restart)", "error", err)
		return
	}
	select {
	case <-ctx.Done():
		return
	case ev := <-deliveryChan:
		if m, ok := ev.(*kafka.Message); ok && m.TopicPartition.Error != nil {
			slog.Error("DLQ delivery failed; NOT committing", "error", m.TopicPartition.Error)
			return
		}
	}
	c.commit(msg)
}

func (c *Consumer) commit(msg *kafka.Message) {
	if _, err := c.consumer.CommitMessage(msg); err != nil {
		slog.Error("offset commit failed", "error", err, "partition", msg.TopicPartition)
	}
}

func mapToDomain(pb *communicationv1.VendorEmailReceived) (*domain.VendorEmail, error) {
	// occurred_at / published_at are required by contract; absence is a terminal malformed event.
	if pb.GetOccurredAt() == nil || pb.GetPublishedAt() == nil {
		return nil, fmt.Errorf("%w: missing required timestamps", domain.ErrTerminal)
	}
	atts := make([]domain.Attachment, len(pb.GetAttachments()))
	for i, a := range pb.GetAttachments() {
		atts[i] = domain.Attachment{
			FileName:         a.GetFileName(),
			MimeType:         a.GetMimeType(),
			SecureStorageURI: a.GetSecureStorageUri(),
			Sha256Hash:       a.GetSha256Hash(),
		}
	}
	return &domain.VendorEmail{
		EventID:              pb.GetEventId(),
		TraceParent:          pb.GetTraceParent(),
		AggregateID:          pb.GetAggregateId(),
		OccurredAt:           pb.GetOccurredAt().AsTime(),
		PublishedAt:          pb.GetPublishedAt().AsTime(),
		CDCEmitTs:            tsOrZero(pb.GetCdcEmitTs()), // nil-safe: unset at ingest, stamped downstream
		SequenceEngineKey:    pb.GetSequenceEngineKey(),
		VisualizationStage:   domain.VisualizationStage(pb.GetVisualizationStage()),
		IntentClassification: domain.Intent(pb.GetIntentClassification()),
		VendorEmail:          pb.GetVendorEmail(),
		MDMVendorID:          pb.GetMdmVendorId(),
		SecureSubjectURI:     pb.GetSecureSubjectUri(),
		SecureBodyURI:        pb.GetSecureBodyUri(),
		Attachments:          atts,
	}, nil
}

func tsOrZero(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}
