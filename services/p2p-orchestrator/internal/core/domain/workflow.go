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
}

func (w *Workflow) NextNode() string {
	if w.CurrentNodeIndex < len(w.SortedNodes) {
		return w.SortedNodes[w.CurrentNodeIndex]
	}
	return ""
}
