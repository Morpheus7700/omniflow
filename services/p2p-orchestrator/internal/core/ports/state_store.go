package ports

import (
	"context"

	"omniflow/services/p2p-orchestrator/internal/core/domain"
)

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Checkpointer interface {
	LoadOrCreateWorkflow(ctx context.Context, eventID string, traceParent string, seqKey uint64, sortedNodes []string) (*domain.Workflow, error)
	LoadWorkflowByEventID(ctx context.Context, eventID string) (*domain.Workflow, error)
	
	AcquireLease(ctx context.Context, workflowID string) (Transaction, error)
	CheckIdempotency(ctx context.Context, workflowID, nodeID string, attempt int) (bool, error)
	SaveCheckpoint(ctx context.Context, tx Transaction, wf *domain.Workflow, nodeID string, attempt int, payload []byte) error
}
