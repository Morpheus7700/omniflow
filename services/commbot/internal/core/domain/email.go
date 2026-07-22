package domain

import (
	"time"
)

type Intent int

const (
	IntentUnspecified Intent = iota
	IntentInvoiceSubmission
	IntentPaymentDispute
	IntentPurchaseOrderInquiry
	IntentGeneralSupport
)

type VisualizationStage int

const (
	VisualizationStageUnspecified VisualizationStage = iota
	VisualizationStageIngested
	VisualizationStageClassified
	VisualizationStageOrchestrating
)

// Attachment defines the business representation of an attachment.
type Attachment struct {
	FileName         string
	MimeType         string
	SecureStorageURI string
	Sha256Hash       string
}

// VendorEmail is the pure domain model representing an ingested email.
// Includes all required telemetry and temporal tracking fields.
type VendorEmail struct {
	EventID              string
	TraceParent          string
	AggregateID          string
	OccurredAt           time.Time
	PublishedAt          time.Time
	CDCEmitTs            time.Time
	SequenceEngineKey    uint64
	VisualizationStage   VisualizationStage
	IntentClassification Intent
	VendorEmail          string
	MDMVendorID          string
	SecureSubjectURI     string
	SecureBodyURI        string
	Attachments          []Attachment
}
