package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	inventoryv1 "omniflow/contracts/inventory/v1"
	"omniflow/internal/platform/delivery"
	"omniflow/services/inventory-intelligence/internal/core/domain"

	"buf.build/go/protovalidate"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Consumer struct {
	client    *kgo.Client
	service   *domain.ValuationService
	validator protovalidate.Validator
}

func NewConsumer(client *kgo.Client, svc *domain.ValuationService) (*Consumer, error) {
	// Compile the ruleset EAGERLY at startup rather than on first message.
	//
	// Lazily-compiled rules are cached as a per-message error and returned from EVERY subsequent
	// Validate call. Because a validation failure is (correctly) terminal, a ruleset that fails to
	// compile would route 100% of production traffic to the DLQ while the process reports healthy
	// and the constructor reports success. WithDisableLazy + WithMessages turns that silent outage
	// into a startup crash, which the constructor is already fallible enough to express.
	v, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&inventoryv1.InventoryMovementReceived{}),
	)
	if err != nil {
		return nil, fmt.Errorf("init protovalidate: %w", err)
	}
	return &Consumer{
		client:    client,
		service:   svc,
		validator: v,
	}, nil
}

const (
	maxTransientRetries = 5
	initialBackoff      = 100 * time.Millisecond
)

func (c *Consumer) Start(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		// PollFetches returns a fetch wrapping ctx.Err() on cancellation, and IsClientClosed is
		// false for that case. Without this check SIGTERM turns the loop into a hot spin that
		// logs "context canceled" at full CPU and never returns, so the caller's graceful
		// shutdown is unreachable and the process has to be SIGKILLed.
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
			c.processRecordWithRetry(ctx, record)
		})
	}
}

// processRecordWithRetry mirrors the bounded-retry ladder in commbot and p2p-orchestrator.
//
// It replaces an earlier shape that returned from the EachRecord closure on a transient error
// under the comment "no commit -> redelivered". That comment described Kafka semantics this code
// does not have: not committing does not rewind franz-go's in-session consume cursor, so the
// failed record was never retried and the *next* record was processed instead — a silent drop
// that also broke per-partition ordering. Retrying in place is what actually redelivers.
func (c *Consumer) processRecordWithRetry(ctx context.Context, record *kgo.Record) {
	backoff := initialBackoff
	var err error

	for attempt := 1; attempt <= maxTransientRetries; attempt++ {
		err = c.processRecord(ctx, record)
		switch {
		case err == nil:
			c.commit(ctx, record)
			return

		case errors.Is(err, domain.ErrTerminal):
			c.deadLetter(ctx, record, err)
			return

		case errors.Is(err, domain.ErrTransient):
			slog.Warn("transient error, retrying in place",
				"error", err, "attempt", attempt, "max_attempts", maxTransientRetries,
				"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2

		default:
			// Unclassified. Fail closed, matching commbot and errclass.Unknown: an error we
			// cannot name is an error whose retry cost we cannot bound, and retrying it blocks
			// the partition while producing no new information.
			c.deadLetter(ctx, record, fmt.Errorf("unclassified error (defaulting terminal): %w", err))
			return
		}
	}

	c.deadLetter(ctx, record, fmt.Errorf("transient retries exhausted after %d attempts: %w", maxTransientRetries, err))
}

// commit synchronously commits the record's offset.
//
// This was previously MarkCommitRecords, which franz-go documents as a no-op unless the client was
// built with kgo.AutoCommitMarks() — and this client is built with kgo.DisableAutoCommit(), which
// config.go makes mutually exclusive with it. The result was that this service committed ZERO
// offsets for its entire lifetime and replayed the whole topic on every restart. The consumer-side
// processed_events idempotency table absorbed the duplicates, which is precisely why it went
// unnoticed. CommitRecords is what commbot and p2p-orchestrator already use.
func (c *Consumer) commit(ctx context.Context, record *kgo.Record) {
	// Bounded, and not cancelled by the parent — a SIGTERM mid-handoff must not abandon the commit
	// that the confirmed DLQ write has already earned. See internal/platform/delivery.
	dctx, dcancel := delivery.Context(ctx)
	defer dcancel()
	if err := c.client.CommitRecords(dctx, record); err != nil {
		slog.Error("offset commit failed", "error", err,
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
	}
}

// deadLetter publishes to the DLQ and commits the source offset ONLY after delivery is confirmed.
// A failed produce plus a committed offset is permanent data loss.
func (c *Consumer) deadLetter(ctx context.Context, record *kgo.Record, cause error) {
	slog.Error("routing to DLQ", "error", cause,
		"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
	if err := c.produceToDLQConfirmed(ctx, record, cause); err != nil {
		return // do not commit; the record will be redelivered to a future member
	}
	c.commit(ctx, record)
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) error {
	var pb inventoryv1.InventoryMovementReceived
	if err := proto.Unmarshal(record.Value, &pb); err != nil {
		return fmt.Errorf("%w: deserialization poison pill: %v", domain.ErrTerminal, err)
	}
	if err := c.validator.Validate(&pb); err != nil {
		return fmt.Errorf("%w: protovalidate poison pill: %v", domain.ErrTerminal, err)
	}

	event, err := mapToDomain(&pb)
	if err != nil {
		return fmt.Errorf("%w: malformed event: %v", domain.ErrTerminal, err)
	}

	if err := c.service.Process(ctx, *event); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) produceToDLQConfirmed(ctx context.Context, record *kgo.Record, err error) error {
	dlqRecord := &kgo.Record{
		Topic:   record.Topic + ".dlq",
		Key:     record.Key,
		Value:   record.Value,
		Headers: dlqHeaders(record, err),
	}
	// Bounded, and deliberately not cancelled by the parent: this is the DLQ half of the
	// confirmed-DLQ-before-commit contract. See internal/platform/delivery.
	dctx, dcancel := delivery.Context(ctx)
	defer dcancel()
	if e := c.client.ProduceSync(dctx, dlqRecord).FirstErr(); e != nil {
		slog.Error("failed to produce to DLQ", "error", e)
		return e
	}
	return nil
}

// dlqHeaders copies the source headers rather than appending to them in place. append() on a slice
// we do not own can write into franz-go's backing array when it has spare capacity, corrupting a
// record we are about to hand back to the client. source_topic makes a DLQ record routable by a
// re-drive tool without having to guess its wire format from the error string.
func dlqHeaders(record *kgo.Record, cause error) []kgo.RecordHeader {
	headers := make([]kgo.RecordHeader, 0, len(record.Headers)+2)
	headers = append(headers, record.Headers...)
	return append(headers,
		kgo.RecordHeader{Key: "error", Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: "source_topic", Value: []byte(record.Topic)},
	)
}

func mapToDomain(pb *inventoryv1.InventoryMovementReceived) (*domain.InventoryMovementReceived, error) {
	if pb.GetOccurredAt() == nil || pb.GetPublishedAt() == nil {
		return nil, fmt.Errorf("%w: missing required timestamps", domain.ErrTerminal)
	}

	qty, err := decimal.NewFromString(pb.GetQuantity())
	if err != nil {
		return nil, err
	}
	cost, err := decimal.NewFromString(pb.GetUnitCost())
	if err != nil {
		return nil, err
	}

	return &domain.InventoryMovementReceived{
		EventID:           pb.GetEventId(),
		TraceParent:       pb.GetTraceParent(),
		AggregateID:       pb.GetAggregateId(),
		OccurredAt:        pb.GetOccurredAt().AsTime(),
		PublishedAt:       pb.GetPublishedAt().AsTime(),
		CDCEmitTs:         tsOrZero(pb.GetCdcEmitTs()),
		SequenceEngineKey: pb.GetSequenceEngineKey(),
		MovementType:      domain.MovementType(pb.GetMovementType()),
		SKU:               pb.GetSku(),
		MDMVendorID:       pb.GetMdmVendorId(),
		LocationID:        pb.GetLocationId(),
		Quantity:          qty,
		UnitCost:          cost,
	}, nil
}

func tsOrZero(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}
