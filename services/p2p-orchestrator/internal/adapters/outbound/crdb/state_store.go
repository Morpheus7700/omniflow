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

func (s *Store) LoadWorkflowByEventID(ctx context.Context, eventID string) (*domain.Workflow, error) {
	var wf domain.Workflow
	var ownerPod, leaseExpiresAt *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, event_id, trace_parent, sequence_engine_key, state, current_node_index, sorted_nodes, owner_pod, lease_expires_at::text
		FROM workflows WHERE event_id = $1
	`, eventID).Scan(
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
	if nodeID != "" && len(payload) > 0 {
		_, err := ptx.Exec(ctx, `
			WITH update_wf AS (
				UPDATE workflows 
				SET state = $1, current_node_index = $2, owner_pod = $3, lease_expires_at = $4
				WHERE id = $5
			),
			insert_ledger AS (
				INSERT INTO node_execution_ledger (workflow_id, node_id, attempt) 
				VALUES ($5, $6, $7)
				ON CONFLICT (workflow_id, node_id, attempt) DO NOTHING
				RETURNING 1
			)
			INSERT INTO orchestrator_outbox (aggregate_id, event_type, trace_parent, payload) 
			SELECT $5, 'NodeTransition', $8, $9
			WHERE EXISTS (SELECT 1 FROM insert_ledger)
		`, wf.State, wf.CurrentNodeIndex, podParam, leaseParam, wf.ID, nodeID, attempt, wf.TraceParent, payload)
		return err
	} else if nodeID != "" {
		_, err := ptx.Exec(ctx, `
			WITH update_wf AS (
				UPDATE workflows 
				SET state = $1, current_node_index = $2, owner_pod = $3, lease_expires_at = $4
				WHERE id = $5
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
