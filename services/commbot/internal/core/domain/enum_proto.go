package domain

import communicationv1 "omniflow/contracts/communication/v1"

// Explicit domain → protobuf enum mapping.
//
// This replaces `communicationv1.VendorEmailReceived_Intent(int32(e.IntentClassification))` and its
// VisualizationStage twin. Those conversions worked, and they were wrong in a way that could not
// fail loudly.
//
// They depended on the domain's `iota` values and the generated proto enum's numbers staying in
// lockstep forever — a coupling that appeared nowhere in either file. The proto file is the wire
// contract and is expected to grow; inserting `INTENT_CREDIT_NOTE = 2` there, or adding a stage to
// the domain block, silently shifts every value after it. Nothing errors. Every subsequent event is
// published under the wrong intent, the classifier looks like it regressed, and the actual cause is
// three files away in a generated file nobody diffs. `go vet` cannot see it, the type system cannot
// see it, and neither can a reader — the numeric cast makes two independent enumerations look like
// one type.
//
// A switch makes the coupling the thing you are looking at. Adding a domain value without mapping
// it is caught by TestEveryDomainValueMapsToItsProtoTwin rather than by a customer noticing their
// invoices are filed as payment disputes.
//
// It also removes the int -> int32 narrowing that gosec G115 and CodeQL
// go/incorrect-integer-conversion flagged (alerts #1, #2, #3, #4). That those two findings pointed
// at a real design problem, rather than at noise to be silenced with a bounds check, is the reason
// this is a mapping and not a `//nolint`.

// Proto returns the wire enum for this intent. An unrecognised value maps to UNSPECIFIED: a
// consumer that receives "unspecified" can route the message for human triage, whereas one that
// receives a confidently wrong intent acts on it.
func (i Intent) Proto() communicationv1.VendorEmailReceived_Intent {
	switch i {
	case IntentUnspecified:
		return communicationv1.VendorEmailReceived_INTENT_UNSPECIFIED
	case IntentInvoiceSubmission:
		return communicationv1.VendorEmailReceived_INTENT_INVOICE_SUBMISSION
	case IntentPaymentDispute:
		return communicationv1.VendorEmailReceived_INTENT_PAYMENT_DISPUTE
	case IntentPurchaseOrderInquiry:
		return communicationv1.VendorEmailReceived_INTENT_PURCHASE_ORDER_INQUIRY
	case IntentGeneralSupport:
		return communicationv1.VendorEmailReceived_INTENT_GENERAL_SUPPORT
	default:
		return communicationv1.VendorEmailReceived_INTENT_UNSPECIFIED
	}
}

// Proto returns the wire enum for this visualization stage. Same failure-safe default as Intent.
func (v VisualizationStage) Proto() communicationv1.VendorEmailReceived_VisualizationStage {
	switch v {
	case VisualizationStageUnspecified:
		return communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_UNSPECIFIED
	case VisualizationStageIngested:
		return communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_INGESTED
	case VisualizationStageClassified:
		return communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_CLASSIFIED
	case VisualizationStageOrchestrating:
		return communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_ORCHESTRATING
	default:
		return communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_UNSPECIFIED
	}
}
