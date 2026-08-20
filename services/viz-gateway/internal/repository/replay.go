package repository

import (
	"context"
	"strconv"

	"omniflow/services/viz-gateway/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ReplayRepository struct {
	db *pgxpool.Pool
}

func NewReplayRepository(db *pgxpool.Pool) *ReplayRepository {
	return &ReplayRepository{db: db}
}

// GetMovements returns at most limit rows, ordered by sequence key.
//
// The LIMIT is not an optimisation, it is the bound on the response. Without it this returned the
// entire workflows table for `?from_seq=0`, materialised into one slice and encoded into one
// response body — so an unauthenticated caller could pull the whole projection history in a single
// request, and this service's memory ceiling was "however many workflows exist". The
// statement_timeout added in crdbpool bounds how LONG a query may run; it does not bound how MUCH
// it may return.
func (r *ReplayRepository) GetMovements(ctx context.Context, fromSeq, toSeq uint64, limit int) ([]domain.ProjectionEvent, error) {
	rows, err := r.db.Query(ctx, `
		SELECT event_id, trace_parent, sequence_engine_key, state
		FROM workflows
		WHERE sequence_engine_key >= $1 AND ($2 = 0 OR sequence_engine_key <= $2)
		ORDER BY sequence_engine_key ASC
		LIMIT $3`, fromSeq, toSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.ProjectionEvent
	for rows.Next() {
		var e domain.ProjectionEvent
		var seqKey uint64
		var stateStr string
		if err := rows.Scan(&e.AggregateID, &e.TraceParent, &seqKey, &stateStr); err != nil {
			return nil, err
		}

		e.SequenceEngineKey = strconv.FormatUint(seqKey, 10)
		e.Stage = domain.VisualizationStage(stateStr) // Real stage
		e.Status = "SUCCESS"                          // Or logic based on state
		events = append(events, e)
	}

	if events == nil {
		events = []domain.ProjectionEvent{}
	}

	return events, rows.Err()
}
