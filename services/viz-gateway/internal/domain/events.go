package domain

import "time"

type VisualizationStage string

const (
	StagePOCreated       VisualizationStage = "PO_CREATED"
	StageVendorConfirmed VisualizationStage = "VENDOR_CONFIRMED"
	StageApproved        VisualizationStage = "APPROVED"
	StageInTransit       VisualizationStage = "IN_TRANSIT"
	StageReceived        VisualizationStage = "RECEIVED"
)

// ProjectionEvent is the wire shape of one ledger row, on both the SSE stream and the replay
// response. It is mirrored field-for-field by P2PEvent in frontend/src/store/index.ts; change
// both or neither. (cdc_emit_ts and edge were removed: nothing populated them, and `omitempty` on
// a time.Time never omits, so every row carried a zero timestamp the frontend ignored.)
type ProjectionEvent struct {
	AggregateID       string             `json:"aggregate_id"`
	Stage             VisualizationStage `json:"stage"`
	Status            string             `json:"status"`
	SequenceEngineKey string             `json:"sequence_engine_key"` // string for safe JS parsing
	OccurredAt        time.Time          `json:"occurred_at"`
	TraceParent       string             `json:"trace_parent,omitempty"`
	Metrics           *Metrics           `json:"metrics,omitempty"`
}

type Metrics struct {
	// Not omitempty: a genuine zero-value movement must reach the ledger as 0, not as "—".
	Value       float64 `json:"value"`
	SLABreached bool    `json:"sla_breached,omitempty"`
}
