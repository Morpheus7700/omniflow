package domain

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"strconv"
)

var (
	ErrNilEmail             = errors.New("cannot process a nil vendor email")
	ErrInvalidQuarantineURI = errors.New("quarantine URIs cannot be empty")
	ErrClassificationFailed = errors.New("failed to classify email intent")

	// Cross-cutting retry taxonomy. Adapters wrap their failures with one of these so the
	// inbound consumer can decide retry-vs-DLQ without string-matching error text.
	ErrTransient = errors.New("transient error (safe to retry)")
	ErrTerminal  = errors.New("terminal error (route to DLQ)")
)

type CommBotService struct {
	llmGateway LLMGateway
	publisher  Publisher
	tracer     trace.Tracer
}

// Inject TracerProvider instead of relying on the global provider inside the domain core.
func NewCommBotService(llmGateway LLMGateway, publisher Publisher, tracerProvider trace.TracerProvider) *CommBotService {
	return &CommBotService{
		llmGateway: llmGateway,
		publisher:  publisher,
		tracer:     tracerProvider.Tracer("commbot.domain.service"),
	}
}

func (s *CommBotService) ProcessVendorEmail(ctx context.Context, email *VendorEmail) (*VendorEmail, error) {
	if email == nil {
		return nil, ErrNilEmail
	}

	ctx, span := s.tracer.Start(ctx, "CommBotService.ProcessVendorEmail",
		trace.WithAttributes(
			attribute.String("event.id", email.EventID),
			attribute.String("vendor.mdm_id", email.MDMVendorID),
			// STRING, not Int64. sequence_engine_key is a 19-digit HLC carried as a string
			// end-to-end (a locked decision — see CLAUDE.md), and int64() here both
			// contradicts that and narrows a uint64: a key above MaxInt64 wraps negative
			// and the span silently claims an ordering that never existed.
			attribute.String("sequence.engine.key", strconv.FormatUint(email.SequenceEngineKey, 10)),
		),
	)
	defer span.End()

	span.AddEvent("Validating zero-trust quarantine state")
	if email.SecureSubjectURI == "" || email.SecureBodyURI == "" {
		span.RecordError(ErrInvalidQuarantineURI)
		return nil, ErrInvalidQuarantineURI // already a terminal sentinel
	}

	span.AddEvent("Delegating to LLMGateway for secure intent classification")
	intent, err := s.llmGateway.ClassifyIntent(ctx, email.SecureSubjectURI, email.SecureBodyURI)
	if err != nil {
		span.RecordError(err)
		// errors.Join (not "%w: %v") so the adapter's ErrTransient/ErrTerminal marker AND
		// ErrClassificationFailed both survive errors.Is() at the consumer.
		return nil, errors.Join(ErrClassificationFailed, err)
	}

	// Do not mutate in place; return a new value to prevent state-bleed on DLQ retries.
	processed := *email
	processed.IntentClassification = intent
	processed.VisualizationStage = VisualizationStageClassified

	span.AddEvent("Intent classification successful", trace.WithAttributes(
		attribute.Int("intent.id", int(intent)),
		attribute.String("visualization.stage", "CLASSIFIED"),
	))

	span.AddEvent("Publishing classified event to orchestrator")
	if err := s.publisher.PublishClassifiedEmail(ctx, &processed); err != nil {
		span.RecordError(err)
		// Publish failures are typically transient (broker/network); let the consumer retry.
		return nil, fmt.Errorf("%w: route to orchestrator: %v", ErrTransient, err)
	}

	return &processed, nil
}
