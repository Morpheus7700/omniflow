package crdb

import (
	"context"
	"errors"

	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txWrapper struct {
	tx pgx.Tx
}

func (t *txWrapper) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *txWrapper) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(p *pgxpool.Pool) *Store {
	return &Store{pool: p}
}

func (s *Store) LoadOrCreateWorkflow(ctx context.Context, eventID string, traceParent string, seqKey uint64, sortedNodes []string) (*domain.Workflow, error) {
	var wf domain.Workflow
	var ownerPod, leaseExpiresAt *string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO workflows (event_id, trace_parent, sequence_engine_key, state, current_node_index, sorted_nodes)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO UPDATE SET event_id = workflows.event_id
		RETURNING id, event_id, trace_parent, sequence_engine_key, state, current_node_index, sorted_nodes, owner_pod, lease_expires_at::text
	`, eventID, traceParent, seqKey, string(domain.StatePending), 0, sortedNodes).Scan(
		&wf.ID, &wf.EventID, &wf.TraceParent, &wf.SequenceEngineKey, &wf.State,
		&wf.CurrentNodeIndex, &wf.SortedNodes, &ownerPod, &leaseExpiresAt,
	)
	if err != nil {
		return nil, err
	}
	if ownerPod != nil {
		wf.OwnerPod = *ownerPod
	}
	return &wf, nil
}

const loadWorkflowByEventIDSQL = `
	SELECT id, event_id, trace_parent, sequence_engine_key, state, current_node_index, sorted_nodes, owner_pod, lease_expires_at::text
	FROM workflows WHERE event_id = $1`

// scanWorkflow decodes one workflows row. Shared so the pool-scoped and tx-scoped loaders cannot
// drift apart in their column list.
func scanWorkflow(row pgx.Row) (*domain.Workflow, error) {
	var wf domain.Workflow
	var ownerPod, leaseExpiresAt *string
	err := row.Scan(
		&wf.ID, &wf.EventID, &wf.TraceParent, &wf.SequenceEngineKey, &wf.State,
		&wf.CurrentNodeIndex, &wf.SortedNodes, &ownerPod, &leaseExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTransient
		}
		return nil, err
	}
	if ownerPod != nil {
		wf.OwnerPod = *ownerPod
	}
	return &wf, nil
}

func (s *Store) LoadWorkflowByEventID(ctx context.Context, eventID string) (*domain.Workflow, error) {
	return scanWorkflow(s.pool.QueryRow(ctx, loadWorkflowByEventIDSQL, eventID))
}

// LoadWorkflowByEventIDTx reads inside the caller's transaction. This exists because reading on a
// POOL connection while that transaction holds `SELECT … FOR UPDATE NOWAIT` on the same row is a
// self-deadlock: the pool read waits on a lock its own caller owns and can never release, so the
// transaction never commits and the row stays locked indefinitely.
//
// That is not hypothetical — it was the live defect. Symptoms: workflows stuck after their first
// node transition, exactly one orchestrator_outbox row instead of two, and every external read of
// that workflows row blocking forever (even AS OF SYSTEM TIME '-30s', because the transaction stayed
// open for minutes) while SELECT 1 and other tables answered instantly.
func (s *Store) LoadWorkflowByEventIDTx(ctx context.Context, tx ports.Transaction, eventID string) (*domain.Workflow, error) {
	ptx := tx.(*txWrapper).tx
	return scanWorkflow(ptx.QueryRow(ctx, loadWorkflowByEventIDSQL, eventID))
}

func (s *Store) AcquireLease(ctx context.Context, workflowID string) (ports.Transaction, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(ctx, `SELECT id FROM workflows WHERE id = $1 FOR UPDATE NOWAIT`, workflowID)
	if err != nil {
		tx.Rollback(ctx)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			return nil, domain.ErrTransient
		}
		return nil, err
	}

	return &txWrapper{tx: tx}, nil
}

func (s *Store) CheckIdempotency(ctx context.Context, workflowID, nodeID string, attempt int) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM node_execution_ledger 
			WHERE workflow_id = $1 AND node_id = $2 AND attempt = $3
		)
	`, workflowID, nodeID, attempt).Scan(&exists)
	return exists, err
}

func (s *Store) SaveCheckpoint(ctx context.Context, tx ports.Transaction, wf *domain.Workflow, nodeID string, attempt int, payload []byte) error {
	ptx := tx.(*txWrapper).tx

	var leaseParam interface{} = wf.LeaseExpiresAt
	if wf.LeaseExpiresAt.IsZero() {
		leaseParam = nil
	}

	var podParam interface{} = wf.OwnerPod
	if wf.OwnerPod == "" {
		podParam = nil
	}

	// H-B fix: Squashing into a single CTE to ensure exactly-once outbox emission
	//
	// wf.ID is bound TWICE on purpose — as $5 and again as $11 — because this one statement needs it
	// as two different SQL types:
	//   workflows.id, node_execution_ledger.workflow_id  ->  UUID    ($5)
	//   orchestrator_outbox.aggregate_id                 ->  STRING  ($11; deliberately STRING, it
	//                                                        doubles as the Kafka partition key)
	// A placeholder carries exactly ONE type, so a single $5 spanning both is rejected:
	//   ERROR: placeholder $5 already has type uuid, cannot assign string (SQLSTATE 42804)
	// Casting does NOT fix this: `$5::UUID` makes CockroachDB infer $5 AS uuid from the cast target,
	// so the STRING use still collides — that attempt was tried and failed in CI identically. Two
	// placeholders is the only unambiguous form; do not "simplify" it back to one.
	if nodeID != "" && len(payload) > 0 {
		_, err := ptx.Exec(ctx, `
			WITH update_wf AS (
				UPDATE workflows
				SET state = $1, current_node_index = $2, owner_pod = $3, lease_expires_at = $4
				WHERE id = $5
				-- RETURNING is REQUIRED, not decorative. CockroachDB rejects a CTE that produces no
				-- columns: 'WITH clause "update_wf" does not return any columns (SQLSTATE 0A000)'.
				-- PostgreSQL permits it, which is why this read as valid SQL. The value is never
				-- consumed, and it does not need to be: per CockroachDB's subquery semantics a
				-- data-modifying statement in a CTE "is always executed to completion, even if the
				-- surrounding query only uses a subset of the results", so the UPDATE still applies.
				RETURNING 1
			),
			insert_ledger AS (
				INSERT INTO node_execution_ledger (workflow_id, node_id, attempt)
				VALUES ($5, $6, $7)
				ON CONFLICT (workflow_id, node_id, attempt) DO NOTHING
				RETURNING 1
			)
			INSERT INTO orchestrator_outbox (aggregate_id, event_type, trace_parent, payload, sequence_engine_key)
			SELECT $11, 'NodeTransition', $8, $9, $10   -- $11, not $5: aggregate_id is STRING
			WHERE EXISTS (SELECT 1 FROM insert_ledger)
		`, wf.State, wf.CurrentNodeIndex, podParam, leaseParam, wf.ID, nodeID, attempt, wf.TraceParent, payload, wf.SequenceEngineKey, wf.ID)
		return err
	} else if nodeID != "" {
		_, err := ptx.Exec(ctx, `
			WITH update_wf AS (
				UPDATE workflows
				SET state = $1, current_node_index = $2, owner_pod = $3, lease_expires_at = $4
				WHERE id = $5
				RETURNING 1 -- required by CockroachDB; see the note on the branch above
			)
			INSERT INTO node_execution_ledger (workflow_id, node_id, attempt)
			VALUES ($5, $6, $7)
			ON CONFLICT (workflow_id, node_id, attempt) DO NOTHING
		`, wf.State, wf.CurrentNodeIndex, podParam, leaseParam, wf.ID, nodeID, attempt)
		return err
	} else {
		_, err := ptx.Exec(ctx, `
			UPDATE workflows 
			SET state = $1, current_node_index = $2, owner_pod = $3, lease_expires_at = $4
			WHERE id = $5
		`, wf.State, wf.CurrentNodeIndex, podParam, leaseParam, wf.ID)
		return err
	}
}
