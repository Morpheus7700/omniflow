package domain

// Status values the dashboard can act on. The frontend branches on FAILURE (a failed row is drawn
// as an exception); everything else is informational.
const (
	StatusSuccess    = "SUCCESS"
	StatusPending    = "PENDING"     // parked at the human gate
	StatusInProgress = "IN_PROGRESS" // running, not yet terminal
	StatusFailure    = "FAILURE"
)

// Orchestrator outbox event types this gateway projects. They are the orchestrator's vocabulary
// (see services/p2p-orchestrator/internal/core/service.go); a type not listed here projects as a
// generic in-progress transition rather than being dropped, so a new event type shows up on the
// ledger before anyone teaches the gateway its name.
const (
	EventPurchaseOrderDrafted = "PurchaseOrderDrafted"
	EventHumanApproved        = "HumanApproved"
	EventNodeTransition       = "NodeTransition"
	EventWorkflowFailed       = "WorkflowFailed"
)

// ProjectOutboxEvent maps an orchestrator_outbox event_type to what the ledger shows. Before this
// existed every live row projected as PO_CREATED with the raw event_type in the status column, so a
// failed workflow was indistinguishable from a healthy transition.
func ProjectOutboxEvent(eventType string) (VisualizationStage, string) {
	switch eventType {
	case EventPurchaseOrderDrafted:
		return StagePOCreated, StatusSuccess
	case EventHumanApproved:
		return StageApproved, StatusSuccess
	case EventWorkflowFailed:
		return StagePOCreated, StatusFailure
	case EventNodeTransition:
		return StageInTransit, StatusSuccess
	default:
		return StageInTransit, StatusInProgress
	}
}

// ProjectWorkflowState maps a workflows.state value (the replay source) to the same vocabulary, so
// a replayed row and a live row for the same workflow agree. Replay previously hardcoded SUCCESS
// for every state, including FAILED.
func ProjectWorkflowState(state string) (VisualizationStage, string) {
	switch state {
	case "COMPLETED":
		return StageReceived, StatusSuccess
	case "FAILED":
		return StagePOCreated, StatusFailure
	case "SUSPENDED":
		return StagePOCreated, StatusPending
	default: // PENDING, RUNNING
		return StagePOCreated, StatusInProgress
	}
}
