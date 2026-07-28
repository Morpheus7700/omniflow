package ports

import (
	"context"

	"omniflow/internal/platform/aigov"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
)

// NodeExecutor performs the real work of a DAG node.
//
// Execution happens OUTSIDE the workflow transaction — an LLM call must never hold a row lock — so
// it is at-least-once by construction. Implementations must therefore be safe to run twice for the
// same NodeRequest, and any side effect they return is persisted idempotently by the orchestrator.
//
// Nodes with no registered executor are pass-through checkpoints, which is what keeps a partially
// implemented DAG runnable.
//
// This is the seam the agent fleet fans out along: a second agent is another implementation, and a
// remote agent is this same interface over the network.
type NodeExecutor interface {
	Execute(ctx context.Context, req domain.NodeRequest) (*domain.NodeResult, error)
}

// AgentAuditor is the durable governance trail. Spend is read from it before every agent call, so
// budget ceilings hold across pods and across retries rather than per process.
type AgentAuditor interface {
	SpentMicroUSD(ctx context.Context, workflowID string) (uint64, error)
	RecordDecision(ctx context.Context, d aigov.Decision, attempt int) error
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Checkpointer interface {
	// triggerPayload is persisted on the workflow row. A workflow must carry its own input to be
	// resumable: nodes execute outside the transaction, so the pod that resumes one is often not
	// the pod that consumed the originating Kafka record.
	LoadOrCreateWorkflow(ctx context.Context, eventID string, traceParent string, seqKey uint64, sortedNodes []string, triggerPayload []byte) (*domain.Workflow, error)
	LoadWorkflowByEventID(ctx context.Context, eventID string) (*domain.Workflow, error)
	// LoadWorkflowByEventIDTx reads within the caller's transaction. Required whenever the caller
	// already holds a lock on that workflows row: the pool-scoped variant would block on that lock
	// forever, because the transaction owning it cannot proceed to release it.
	LoadWorkflowByEventIDTx(ctx context.Context, tx Transaction, eventID string) (*domain.Workflow, error)

	AcquireLease(ctx context.Context, workflowID string) (Transaction, error)
	CheckIdempotency(ctx context.Context, workflowID, nodeID string, attempt int) (bool, error)
	SaveCheckpoint(ctx context.Context, tx Transaction, wf *domain.Workflow, nodeID string, attempt int, payload []byte) error

	// SaveCheckpointTyped is SaveCheckpoint with a caller-chosen outbox event_type, so a node can
	// emit a real domain event instead of the generic node-transition record.
	SaveCheckpointTyped(ctx context.Context, tx Transaction, wf *domain.Workflow, nodeID string, attempt int, eventType string, payload []byte) error

	// InsertPurchaseOrder persists an agent's side effect in the CALLER'S transaction, so the PO,
	// the idempotency ledger row, and the outbox event either all land or none do.
	//
	// It reports whether a row was actually inserted. False means this node already produced a PO
	// (a re-execution after a crash) — which is the exactly-once effect working, not an error.
	InsertPurchaseOrder(ctx context.Context, tx Transaction, workflowID, nodeID string, attempt int, po *domain.PurchaseOrderEffect) (bool, error)
}
