package crdb

import (
	"context"
	"fmt"
	"time"

	communicationv1 "omniflow/contracts/communication/v1"
	"omniflow/services/commbot/internal/core/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const orchestrationEventType = "VendorEmailClassified"

// Repository is the CockroachDB outbound adapter. A single instance satisfies BOTH:
//   - kafka.IdempotencyStore (AlreadyProcessed) — fast-path dedup, and
//   - ports.Publisher       (PublishClassifiedEmail) — transactional outbox write.
//
// Claiming the event_id and appending the outbox row commit in ONE transaction, so there is
// no dual-write. A changefeed on commbot_outbox (see infrastructure/storage/commbot_outbox_schema.sql)
// emits the classified event onward to the P2P Orchestrator — mirroring the Phase 1 pattern.
type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

// AlreadyProcessed is the cheap pre-check that avoids a redundant LLM call on obvious replays.
// The authoritative exactly-once guarantee is enforced transactionally in PublishClassifiedEmail.
func (r *Repository) AlreadyProcessed(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	if err := r.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM commbot_processed_events WHERE event_id = $1)`, eventID,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("idempotency lookup: %w", err)
	}
	return exists, nil
}

// PublishClassifiedEmail atomically (a) claims the event_id and (b) appends the classified event
// to the outbox. If a concurrent delivery already claimed it, we roll back and return nil — the
// downstream event is emitted exactly once.
//
// Tradeoff (documented): the claim happens at publish time, not before the LLM call, so two
// concurrent deliveries can each incur one LLM call, but only one downstream event is ever emitted.
// To also dedup the LLM spend, claim the row as 'processing' before classification (saga style).
func (r *Repository) PublishClassifiedEmail(ctx context.Context, email *domain.VendorEmail) error {
	payload, err := marshalClassified(email)
	if err != nil {
		return fmt.Errorf("%w: marshal classified event: %v", domain.ErrTerminal, err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("%w: begin tx: %v", domain.ErrTransient, err)
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	tag, err := tx.Exec(ctx,
		`INSERT INTO commbot_processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		email.EventID,
	)
	if err != nil {
		return fmt.Errorf("%w: claim event: %v", domain.ErrTransient, err)
	}
	if tag.RowsAffected() == 0 {
		// Already claimed by a concurrent worker — effect is done. Idempotent success.
		return nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO commbot_outbox (aggregate_id, event_type, trace_parent, payload, occurred_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		email.AggregateID, orchestrationEventType, email.TraceParent, payload, email.OccurredAt,
	); err != nil {
		return fmt.Errorf("%w: append outbox: %v", domain.ErrTransient, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit: %v", domain.ErrTransient, err)
	}
	return nil
}

// marshalClassified maps the domain model back to the wire contract. The domain package stays
// protobuf-free; the mapping lives here in the adapter (Ports & Adapters boundary).
func marshalClassified(e *domain.VendorEmail) ([]byte, error) {
	pb := &communicationv1.VendorEmailReceived{
		EventId:              e.EventID,
		TraceParent:          e.TraceParent,
		AggregateId:          e.AggregateID,
		OccurredAt:           timestamppb.New(e.OccurredAt),
		PublishedAt:          timestamppb.New(time.Now().UTC()),
		SequenceEngineKey:    e.SequenceEngineKey,
		VisualizationStage:   communicationv1.VendorEmailReceived_VisualizationStage(int32(e.VisualizationStage)),
		IntentClassification: communicationv1.VendorEmailReceived_Intent(int32(e.IntentClassification)),
		VendorEmail:          e.VendorEmail,
		MdmVendorId:          e.MDMVendorID,
		SecureSubjectUri:     e.SecureSubjectURI,
		SecureBodyUri:        e.SecureBodyURI,
	}
	for _, a := range e.Attachments {
		pb.Attachments = append(pb.Attachments, &communicationv1.Attachment{
			FileName:         a.FileName,
			MimeType:         a.MimeType,
			SecureStorageUri: a.SecureStorageURI,
			Sha256Hash:       a.Sha256Hash,
		})
	}
	return proto.Marshal(pb)
}
