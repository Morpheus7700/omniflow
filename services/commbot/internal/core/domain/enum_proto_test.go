package domain

import (
	"testing"

	communicationv1 "omniflow/contracts/communication/v1"
)

// Every domain value must map to its named proto twin. This is the test that turns "someone
// inserted a value in the middle of the proto enum" from a silent, permanent mislabelling of live
// traffic into a red build.
//
// Note the assertions are against NAMED proto constants, never against numbers. Asserting
// `Proto() == 2` would just re-encode the same positional assumption this file exists to remove.
func TestEveryDomainValueMapsToItsProtoTwin(t *testing.T) {
	t.Run("intent", func(t *testing.T) {
		cases := map[Intent]communicationv1.VendorEmailReceived_Intent{
			IntentUnspecified:          communicationv1.VendorEmailReceived_INTENT_UNSPECIFIED,
			IntentInvoiceSubmission:    communicationv1.VendorEmailReceived_INTENT_INVOICE_SUBMISSION,
			IntentPaymentDispute:       communicationv1.VendorEmailReceived_INTENT_PAYMENT_DISPUTE,
			IntentPurchaseOrderInquiry: communicationv1.VendorEmailReceived_INTENT_PURCHASE_ORDER_INQUIRY,
			IntentGeneralSupport:       communicationv1.VendorEmailReceived_INTENT_GENERAL_SUPPORT,
		}
		for in, want := range cases {
			if got := in.Proto(); got != want {
				t.Errorf("Intent(%d).Proto() = %v, want %v", in, got, want)
			}
		}
		// Guards the case the map above cannot: a value added to the domain block and never mapped
		// would fall through to the default and be published as UNSPECIFIED forever.
		if len(cases) != int(IntentGeneralSupport)+1 {
			t.Errorf("Intent has %d values but %d are mapped — add the new value to Proto() and to this table",
				int(IntentGeneralSupport)+1, len(cases))
		}
	})

	t.Run("visualization stage", func(t *testing.T) {
		cases := map[VisualizationStage]communicationv1.VendorEmailReceived_VisualizationStage{
			VisualizationStageUnspecified:   communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_UNSPECIFIED,
			VisualizationStageIngested:      communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_INGESTED,
			VisualizationStageClassified:    communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_CLASSIFIED,
			VisualizationStageOrchestrating: communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_ORCHESTRATING,
		}
		for in, want := range cases {
			if got := in.Proto(); got != want {
				t.Errorf("VisualizationStage(%d).Proto() = %v, want %v", in, got, want)
			}
		}
		if len(cases) != int(VisualizationStageOrchestrating)+1 {
			t.Errorf("VisualizationStage has %d values but %d are mapped",
				int(VisualizationStageOrchestrating)+1, len(cases))
		}
	})
}

// A value outside the enum must not be published as a confidently wrong intent. UNSPECIFIED is
// routable for human triage; a plausible-but-wrong value gets acted on automatically.
func TestUnknownValuesFailSafeToUnspecified(t *testing.T) {
	if got := Intent(9999).Proto(); got != communicationv1.VendorEmailReceived_INTENT_UNSPECIFIED {
		t.Errorf("Intent(9999).Proto() = %v, want INTENT_UNSPECIFIED", got)
	}
	if got := Intent(-1).Proto(); got != communicationv1.VendorEmailReceived_INTENT_UNSPECIFIED {
		t.Errorf("Intent(-1).Proto() = %v, want INTENT_UNSPECIFIED", got)
	}
	if got := VisualizationStage(9999).Proto(); got != communicationv1.VendorEmailReceived_VISUALIZATION_STAGE_UNSPECIFIED {
		t.Errorf("VisualizationStage(9999).Proto() = %v, want VISUALIZATION_STAGE_UNSPECIFIED", got)
	}
}

// The narrowing this replaces was int -> int32. On a 64-bit build a domain value above MaxInt32
// wrapped to a negative number and became a valid-looking proto enum. The mapping cannot wrap
// because it never converts — it selects.
func TestLargeValuesCannotWrapIntoAValidEnum(t *testing.T) {
	// 1<<32 + 1 narrows to exactly 1 under the old int32() cast — i.e. INVOICE_SUBMISSION.
	const wrapsToOne = Intent(1<<32 + 1)
	if got := wrapsToOne.Proto(); got != communicationv1.VendorEmailReceived_INTENT_UNSPECIFIED {
		t.Errorf("Intent(1<<32+1).Proto() = %v, want INTENT_UNSPECIFIED — a wrapped value became a real intent", got)
	}
}
