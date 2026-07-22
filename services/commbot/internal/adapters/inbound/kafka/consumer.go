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
	"github.com/twmb/franz-go/pkg/kgo"
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
	client      *kgo.Client
	service     *domain.CommBotService
	validator   *protovalidate.Validator
	idempotency IdempotencyStore
	propagator  propagation.TextMapPropagator
	tracer      trace.Tracer
}

// NewConsumer REQUIRES a client created with `kgo.DisableAutoCommit()`.
// Auto-commit acknowledges offsets on poll regardless of processing outcome, which
// silently defeats the retry/DLQ contract below.
func NewConsumer(
	client *kgo.Client,
	svc *domain.CommBotService,
	idem IdempotencyStore,
	tp trace.TracerProvider,
) (*Consumer, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("init protovalidate: %w", err)
	}
	return &Consumer{
		client:      client,
		service:     svc,
		validator:   v,
		idempotency: idem,
		propagator:  propagation.TraceContext{}, // real W3C traceparent propagation
		tracer:      tp.Tracer("commbot.adapter.kafka"),
	}, nil
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if fetches.IsClientClosed() {
			slog.Info("shutdown: client closed")
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("kafka fetch error", "topic", topic, "partition", partition, "error", err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			c.processMessage(ctx, record)
		})
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kgo.Record) {
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
			c.commit(ctx, msg)
			return
		case errors.Is(procErr, errAlreadyProcessed):
			span.AddEvent("duplicate event; committing without reprocessing")
			c.commit(ctx, msg)
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
func (c *Consumer) deadLetter(ctx context.Context, msg *kgo.Record, cause error) {
	slog.Error("routing to DLQ", "error", cause, "partition", msg.Partition)
	dlqRecord := &kgo.Record{
		Topic:   dlqTopic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: append(msg.Headers, kgo.RecordHeader{Key: "error_reason", Value: []byte(cause.Error())}),
	}
	if err := c.client.ProduceSync(ctx, dlqRecord).FirstErr(); err != nil {
		slog.Error("DLQ delivery failed; NOT committing", "error", err)
		return
	}
	c.commit(ctx, msg)
}

func (c *Consumer) commit(ctx context.Context, msg *kgo.Record) {
	err := c.client.CommitRecords(ctx, msg)
	if err != nil {
		slog.Error("offset commit failed", "error", err, "partition", msg.Partition)
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
