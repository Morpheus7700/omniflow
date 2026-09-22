package domain

import "testing"

func TestProjectOutboxEvent(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		stage  VisualizationStage
		status string
	}{
		{"drafted", EventPurchaseOrderDrafted, StagePOCreated, StatusSuccess},
		{"approved", EventHumanApproved, StageApproved, StatusSuccess},
		{"failed is FAILURE", EventWorkflowFailed, StagePOCreated, StatusFailure},
		{"transition", EventNodeTransition, StageInTransit, StatusSuccess},
		{"unknown type still projects", "SomethingNew", StageInTransit, StatusInProgress},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stage, status := ProjectOutboxEvent(tc.in)
			if stage != tc.stage || status != tc.status {
				t.Fatalf("ProjectOutboxEvent(%q) = (%s, %s), want (%s, %s)", tc.in, stage, status, tc.stage, tc.status)
			}
		})
	}
}

func TestProjectWorkflowState(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		stage  VisualizationStage
		status string
	}{
		{"completed", "COMPLETED", StageReceived, StatusSuccess},
		{"failed is FAILURE, never SUCCESS", "FAILED", StagePOCreated, StatusFailure},
		{"suspended is pending", "SUSPENDED", StagePOCreated, StatusPending},
		{"running", "RUNNING", StagePOCreated, StatusInProgress},
		{"pending", "PENDING", StagePOCreated, StatusInProgress},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stage, status := ProjectWorkflowState(tc.in)
			if stage != tc.stage || status != tc.status {
				t.Fatalf("ProjectWorkflowState(%q) = (%s, %s), want (%s, %s)", tc.in, stage, status, tc.stage, tc.status)
			}
		})
	}
}
