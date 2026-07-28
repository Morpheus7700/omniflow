package domain

import "time"

type WorkflowState string

const (
	StatePending   WorkflowState = "PENDING"
	StateRunning   WorkflowState = "RUNNING"
	StateSuspended WorkflowState = "SUSPENDED" // HITL cross-wait state
	StateCompleted WorkflowState = "COMPLETED"
	StateFailed    WorkflowState = "FAILED"
)

type Workflow struct {
	ID                string
	EventID           string
	TraceParent       string
	SequenceEngineKey uint64
	State             WorkflowState

	// Pre-computed and persisted Kahn's sort to prevent re-evaluation
	SortedNodes      []string
	CurrentNodeIndex int

	// Durable Lease for Human-In-The-Loop cross-wait ownership
	OwnerPod       string
	LeaseExpiresAt time.Time

	// TriggerPayload is the marshalled event that started this workflow, persisted on the row.
	//
	// A workflow has to carry its own input to be resumable. Nodes execute outside the transaction,
	// so the pod that resumes a half-executed workflow is frequently not the pod that consumed the
	// originating Kafka record — and that record belongs to a partition the survivor may not own.
	// Without this, a resumed node has nothing to re-execute from.
	TriggerPayload []byte
}

func (w *Workflow) NextNode() string {
	if w.CurrentNodeIndex < len(w.SortedNodes) {
		return w.SortedNodes[w.CurrentNodeIndex]
	}
	return ""
}
