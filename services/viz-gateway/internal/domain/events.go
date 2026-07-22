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

type ProjectionEvent struct {
	AggregateID       string             `json:"aggregate_id"`
	Stage             VisualizationStage `json:"stage"`
	Status            string             `json:"status"`
	SequenceEngineKey string             `json:"sequence_engine_key"` // string for safe JS parsing
	OccurredAt        time.Time          `json:"occurred_at"`
	CDCEmitTs         time.Time          `json:"cdc_emit_ts,omitempty"`
	TraceParent       string             `json:"trace_parent,omitempty"`
	Edge              *Edge              `json:"edge,omitempty"`
	Metrics           *Metrics           `json:"metrics,omitempty"`
}

type Edge struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

type Metrics struct {
	Value       float64 `json:"value,omitempty"`
	SLABreached bool    `json:"sla_breached,omitempty"`
}
