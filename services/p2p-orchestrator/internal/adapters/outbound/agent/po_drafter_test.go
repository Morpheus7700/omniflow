package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commv1 "omniflow/contracts/communication/v1"
	procv1 "omniflow/contracts/procurement/v1"
	"omniflow/internal/platform/aigov"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
)

// fakeAuditor records what the agent wrote, so the tests can assert the governance trail rather
// than just the happy path.
type fakeAuditor struct {
	spent     uint64
	decisions []aigov.Decision
	spendErr  error
	recordErr error
}

func (f *fakeAuditor) SpentMicroUSD(context.Context, string) (uint64, error) {
	return f.spent, f.spendErr
}

func (f *fakeAuditor) RecordDecision(_ context.Context, d aigov.Decision, _ int) error {
	f.decisions = append(f.decisions, d)
	return f.recordErr
}

func policy() aigov.Policy {
	return aigov.Policy{
		AllowedModels:              map[string]aigov.CostModel{"test-model": {PromptMicroUSDPer1K: 100, CompletionMicroUSDPer1K: 400}},
		MaxCostPerWorkflowMicroUSD: 500_000,
		MaxEstimatedCallMicroUSD:   100_000,
		AutonomousCeilingMicroUSD:  50_000_000_000, // $50,000
	}
}

func triggerPayload(t *testing.T) []byte {
	t.Helper()
	b, err := proto.Marshal(&commv1.VendorEmailReceived{
		EventId:     "11111111-1111-1111-1111-111111111111",
		TraceParent: "00-00000000000000000000000000000001-0000000000000001-01",
		MdmVendorId: "VENDOR-1",
		OccurredAt:  timestamppb.Now(),
		PublishedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("marshal trigger: %v", err)
	}
	return b
}

func newDrafter(t *testing.T, content string, status int, aud *fakeAuditor) *PODrafter {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":` + jsonQuote(content) + `}}],"usage":{"prompt_tokens":300,"completion_tokens":150}}`))
	}))
	t.Cleanup(srv.Close)
	return NewPODrafter(srv.URL, "", "test-model", aigov.NewGuard(policy(), 100, 100), aud)
}

func jsonQuote(s string) string {
	out := []byte{'"'}
	for _, r := range []byte(s) {
		switch r {
		case '"':
			out = append(out, '\\', '"')
		case '\\':
			out = append(out, '\\', '\\')
		case '\n':
			out = append(out, '\\', 'n')
		default:
			out = append(out, r)
		}
	}
	return string(append(out, '"'))
}

func request() domain.NodeRequest {
	return domain.NodeRequest{
		WorkflowID:  "abc12345-0000-0000-0000-000000000000",
		EventID:     "11111111-1111-1111-1111-111111111111",
		NodeID:      "draft_po",
		Attempt:     1,
		TraceParent: "00-00000000000000000000000000000001-0000000000000001-01",
		SequenceKey: 1234567890123456789,
	}
}

const goodPO = `{"vendor_id":"VENDOR-1","currency":"USD","lines":[{"sku":"SKU-1","description":"Widget","quantity":"100","unit_price":"2.5000","extended_amount":"250.0000"}],"total_amount":"250.0000"}`

func TestDraftsAValidPurchaseOrder(t *testing.T) {
	aud := &fakeAuditor{}
	d := newDrafter(t, goodPO, http.StatusOK, aud)

	req := request()
	req.TriggerPayload = triggerPayload(t)

	res, err := d.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if res.Effect == nil {
		t.Fatal("no PO effect returned")
	}
	if got, want := res.Effect.PONumber, "PO-abc12345-1"; got != want {
		t.Errorf("PONumber = %q, want %q", got, want)
	}
	if got := res.Effect.TotalAmount.String(); got != "250" {
		t.Errorf("TotalAmount = %s, want 250", got)
	}
	if res.EventType != "PurchaseOrderDrafted" {
		t.Errorf("EventType = %q", res.EventType)
	}
	if res.RequiresHumanApproval {
		t.Error("a $250 PO is under the $50,000 ceiling and should not require approval")
	}

	// The emitted event must be a decodable contract message carrying provenance.
	var ev procv1.PurchaseOrderDrafted
	if err := proto.Unmarshal(res.Payload, &ev); err != nil {
		t.Fatalf("payload is not a PurchaseOrderDrafted: %v", err)
	}
	if ev.GetProvenance() == nil {
		t.Fatal("event carries no provenance")
	}
	if ev.GetSequenceEngineKey() != req.SequenceKey {
		t.Errorf("HLC key = %d, want %d", ev.GetSequenceEngineKey(), req.SequenceKey)
	}
	if len(ev.GetProvenance().GetPromptSha256()) != 64 {
		t.Errorf("prompt hash = %q, want 64 hex chars", ev.GetProvenance().GetPromptSha256())
	}
	if ev.GetProvenance().GetDecisionId() != res.Effect.DecisionID {
		t.Error("event provenance decision_id does not match the persisted PO decision_id")
	}
	// Usage came from the gateway response, not the pre-call estimate.
	if ev.GetProvenance().GetPromptTokens() != 300 || ev.GetProvenance().GetCompletionTokens() != 150 {
		t.Errorf("tokens = %d/%d, want the gateway-reported 300/150",
			ev.GetProvenance().GetPromptTokens(), ev.GetProvenance().GetCompletionTokens())
	}
	// 300 prompt tokens @100/1k + 150 completion @400/1k = 30 + 60 = 90 µUSD.
	if got := ev.GetProvenance().GetCostMicroUsd(); got != 90 {
		t.Errorf("cost = %d µUSD, want 90", got)
	}

	assertOneDecision(t, aud, aigov.OutcomeAccepted)
}

// The headline governance test: a model returning confident, wrong arithmetic must not produce a PO.
func TestRejectsFabricatedArithmeticAndStillAudits(t *testing.T) {
	const bad = `{"vendor_id":"VENDOR-1","currency":"USD","lines":[{"sku":"SKU-1","description":"Widget","quantity":"100","unit_price":"2.5000","extended_amount":"250.0000"}],"total_amount":"25.0000"}`
	aud := &fakeAuditor{}
	d := newDrafter(t, bad, http.StatusOK, aud)

	req := request()
	req.TriggerPayload = triggerPayload(t)

	res, err := d.Execute(context.Background(), req)
	if err == nil {
		t.Fatal("Execute() = nil error, want rejection of a PO whose total contradicts its lines")
	}
	if res != nil {
		t.Error("a rejected draft must not return a result")
	}
	if !errors.Is(err, domain.ErrTerminal) {
		t.Errorf("want terminal (the same prompt yields the same bad answer), got %v", err)
	}
	assertOneDecision(t, aud, aigov.OutcomeRejectedValidation)
}

func TestRejectsMalformedModelOutput(t *testing.T) {
	aud := &fakeAuditor{}
	d := newDrafter(t, "I'm afraid I can't do that.", http.StatusOK, aud)
	req := request()
	req.TriggerPayload = triggerPayload(t)

	if _, err := d.Execute(context.Background(), req); !errors.Is(err, domain.ErrTerminal) {
		t.Fatalf("Execute() = %v, want terminal", err)
	}
	assertOneDecision(t, aud, aigov.OutcomeRejectedValidation)
}

func TestEscalatesAboveTheAutonomousCeiling(t *testing.T) {
	const huge = `{"vendor_id":"VENDOR-1","currency":"USD","lines":[{"sku":"SKU-9","description":"Press","quantity":"1","unit_price":"250000.0000","extended_amount":"250000.0000"}],"total_amount":"250000.0000"}`
	aud := &fakeAuditor{}
	d := newDrafter(t, huge, http.StatusOK, aud)
	req := request()
	req.TriggerPayload = triggerPayload(t)

	res, err := d.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute() = %v, want nil (a large PO is valid, it just needs a human)", err)
	}
	if !res.RequiresHumanApproval {
		t.Error("a $250,000 PO must require human approval")
	}
	var ev procv1.PurchaseOrderDrafted
	if err := proto.Unmarshal(res.Payload, &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ev.GetProvenance().GetHumanApproved() {
		t.Error("human_approved must be false on an event that still needs approval")
	}
}

// Budget refusal must never reach the model, and must still leave a trail.
func TestRefusesWhenTheWorkflowBudgetIsExhausted(t *testing.T) {
	aud := &fakeAuditor{spent: 500_000} // at the cap
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	d := NewPODrafter(srv.URL, "", "test-model", aigov.NewGuard(policy(), 100, 100), aud)

	req := request()
	req.TriggerPayload = triggerPayload(t)

	_, err := d.Execute(context.Background(), req)
	if !errors.Is(err, domain.ErrTerminal) {
		t.Fatalf("Execute() = %v, want terminal budget refusal", err)
	}
	if called {
		t.Error("the model was called despite the budget being exhausted; the cap must be enforced BEFORE spend")
	}
	assertOneDecision(t, aud, aigov.OutcomeRejectedPolicy)
}

func TestGatewayFailuresAreClassifiedForRetry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		transient bool
	}{
		{"500 is transient", http.StatusInternalServerError, true},
		{"429 is transient", http.StatusTooManyRequests, true},
		{"401 is terminal", http.StatusUnauthorized, false},
		{"400 is terminal", http.StatusBadRequest, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aud := &fakeAuditor{}
			d := newDrafter(t, "", tc.status, aud)
			req := request()
			req.TriggerPayload = triggerPayload(t)

			_, err := d.Execute(context.Background(), req)
			if err == nil {
				t.Fatal("want an error")
			}
			if tc.transient && !errors.Is(err, domain.ErrTransient) {
				t.Errorf("status %d: want transient, got %v", tc.status, err)
			}
			if !tc.transient && !errors.Is(err, domain.ErrTerminal) {
				t.Errorf("status %d: want terminal, got %v", tc.status, err)
			}
			// Even an outright gateway failure leaves a governance row.
			assertOneDecision(t, aud, aigov.OutcomeError)
		})
	}
}

func TestRejectsATriggerItCannotDecode(t *testing.T) {
	aud := &fakeAuditor{}
	d := newDrafter(t, goodPO, http.StatusOK, aud)
	req := request()
	req.TriggerPayload = []byte{0x6e, 0x6f, 0x70, 0x65}

	if _, err := d.Execute(context.Background(), req); !errors.Is(err, domain.ErrTerminal) {
		t.Fatalf("Execute() = %v, want terminal", err)
	}
}

// A re-executed node must derive the same PO number, or the unique constraint cannot suppress a
// duplicate purchase order.
func TestReExecutionProducesTheSamePONumber(t *testing.T) {
	aud := &fakeAuditor{}
	d := newDrafter(t, goodPO, http.StatusOK, aud)
	req := request()
	req.TriggerPayload = triggerPayload(t)

	first, err := d.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := d.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.Effect.PONumber != second.Effect.PONumber {
		t.Errorf("PO number differs across executions: %q vs %q", first.Effect.PONumber, second.Effect.PONumber)
	}
}

func assertOneDecision(t *testing.T, aud *fakeAuditor, want aigov.Outcome) {
	t.Helper()
	if len(aud.decisions) != 1 {
		t.Fatalf("audit rows = %d, want exactly 1 (every call must leave exactly one)", len(aud.decisions))
	}
	d := aud.decisions[0]
	if d.Outcome != want {
		t.Errorf("outcome = %q, want %q", d.Outcome, want)
	}
	if d.PromptSHA256 == "" || d.Prompt == "" {
		t.Error("audit row is missing the prompt or its hash")
	}
	if d.ID == "" {
		t.Error("audit row has no id")
	}
}
