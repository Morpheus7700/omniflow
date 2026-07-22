package kafka

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	inventoryv1 "omniflow/contracts/inventory/v1"
	"omniflow/services/inventory-intelligence/internal/core/domain"

	"github.com/bufbuild/protovalidate-go"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Consumer struct {
	client    *kgo.Client
	service   *domain.ValuationService
	validator *protovalidate.Validator
}

func NewConsumer(client *kgo.Client, svc *domain.ValuationService) (*Consumer, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("init protovalidate: %w", err)
	}
	return &Consumer{
		client:    client,
		service:   svc,
		validator: v,
	}, nil
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
			// Skip resolved messages natively
			if isResolvedMessage(record) {
				c.client.MarkCommitRecords(record)
				return
			}

			err := c.processRecord(ctx, record)
			switch {
			case err == nil:
				c.client.MarkCommitRecords(record)
			case errors.Is(err, domain.ErrTransient):
				slog.Warn("transient error, skipping commit", "error", err)
				return // no commit -> redelivered
			default:
				slog.Error("terminal error, routing to DLQ", "error", err)
				if e := c.produceToDLQConfirmed(ctx, record, err); e != nil {
					return // block on delivery report; don't commit if it failed
				}
				c.client.MarkCommitRecords(record)
			}
		})
	}
}

func isResolvedMessage(record *kgo.Record) bool {
	var envelope map[string]interface{}
	if err := json.Unmarshal(record.Value, &envelope); err == nil {
		if _, ok := envelope["resolved"]; ok {
			return true
		}
	}
	return false
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) error {
	var envelope struct {
		Payload struct {
			After string `json:"after"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		return fmt.Errorf("%w: invalid JSON envelope: %v", domain.ErrTerminal, err)
	}

	rawStr := envelope.Payload.After
	var payloadBytes []byte
	var err error

	if len(rawStr) >= 2 && rawStr[:2] == "\\x" {
		payloadBytes, err = hex.DecodeString(rawStr[2:])
		if err != nil {
			return fmt.Errorf("%w: invalid hex: %v", domain.ErrTerminal, err)
		}
	} else {
		payloadBytes, err = base64.StdEncoding.DecodeString(rawStr)
		if err != nil {
			return fmt.Errorf("%w: invalid base64: %v", domain.ErrTerminal, err)
		}
	}

	var pb inventoryv1.InventoryMovementReceived
	if err := proto.Unmarshal(payloadBytes, &pb); err != nil {
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
		Topic: record.Topic + ".dlq",
		Key:   record.Key,
		Value: record.Value,
		Headers: append(record.Headers, kgo.RecordHeader{
			Key:   "error",
			Value: []byte(err.Error()),
		}),
	}
	if e := c.client.ProduceSync(ctx, dlqRecord).FirstErr(); e != nil {
		slog.Error("failed to produce to DLQ", "error", e)
		return e
	}
	return nil
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


