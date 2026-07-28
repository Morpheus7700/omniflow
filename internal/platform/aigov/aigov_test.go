package aigov

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func testPolicy() Policy {
	return Policy{
		AllowedModels: map[string]CostModel{
			"gemini-2.5-flash": {PromptMicroUSDPer1K: 100, CompletionMicroUSDPer1K: 400},
		},
		MaxCostPerWorkflowMicroUSD: 10_000,
		MaxEstimatedCallMicroUSD:   5_000,
		AutonomousCeilingMicroUSD:  50_000_000, // $50
	}
}

func req(prompt string) Request {
	return Request{
		AgentID:             "po-drafter",
		Model:               "gemini-2.5-flash",
		WorkflowID:          "wf-1",
		NodeID:              "draft_po",
		Prompt:              prompt,
		MaxCompletionTokens: 512,
	}
}

func TestAuthorizeAllowsAWellFormedCall(t *testing.T) {
	g := NewGuard(testPolicy(), 100, 100)
	if _, err := g.Authorize(req("draft a PO"), 0); err != nil {
		t.Fatalf("Authorize() = %v, want nil", err)
	}
}

func TestModelAllowlistIsExactMatch(t *testing.T) {
	g := NewGuard(testPolicy(), 100, 100)
	r := req("draft a PO")
	r.Model = "gpt-4-turbo"
	_, err := g.Authorize(r, 0)
	if !errors.Is(err, ErrModelNotAllowed) {
		t.Fatalf("Authorize(unlisted model) = %v, want ErrModelNotAllowed", err)
	}
}

func TestUnconfiguredGuardDeniesEverything(t *testing.T) {
	// A zero Policy must refuse rather than default to permitting unbounded spend.
	g := NewGuard(Policy{}, 100, 100)
	if _, err := g.Authorize(req("anything"), 0); err == nil {
		t.Fatal("Authorize() on a zero Policy = nil, want a refusal")
	}
}

func TestWorkflowBudgetCountsPriorSpend(t *testing.T) {
	g := NewGuard(testPolicy(), 100, 100)
	// Already at the cap: any further call must be refused, which is the control that bounds a
	// redelivery loop.
	_, err := g.Authorize(req("draft a PO"), 10_000)
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Authorize(at cap) = %v, want ErrBudgetExhausted", err)
	}
}

func TestPerCallCeilingRefusesAHugePrompt(t *testing.T) {
	g := NewGuard(testPolicy(), 100, 100)
	huge := strings.Repeat("x", 4_000_000) // ~1M tokens
	_, err := g.Authorize(req(huge), 0)
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Authorize(huge prompt) = %v, want ErrBudgetExhausted", err)
	}
}

func TestRateLimitRefusesRatherThanBlocks(t *testing.T) {
	g := NewGuard(testPolicy(), 1, 1)
	if _, err := g.Authorize(req("first"), 0); err != nil {
		t.Fatalf("first call = %v, want nil", err)
	}
	start := time.Now()
	_, err := g.Authorize(req("second"), 0)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("second call = %v, want ErrRateLimited", err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Error("Authorize blocked waiting for a token; it must refuse so the consumer can retry")
	}
}

func TestPolicyRefusalDoesNotConsumeARateLimitToken(t *testing.T) {
	// Otherwise a stream of misconfigured requests starves legitimate ones.
	g := NewGuard(testPolicy(), 1, 1)
	bad := req("x")
	bad.Model = "not-allowed"
	if _, err := g.Authorize(bad, 0); !errors.Is(err, ErrModelNotAllowed) {
		t.Fatalf("setup: got %v", err)
	}
	if _, err := g.Authorize(req("legitimate"), 0); err != nil {
		t.Fatalf("legitimate call after a policy refusal = %v, want nil (token was consumed)", err)
	}
}

func TestCostRoundsUpSoBudgetsCannotDrift(t *testing.T) {
	cm := CostModel{PromptMicroUSDPer1K: 100, CompletionMicroUSDPer1K: 400}
	// 1 prompt token => 100/1000 = 0.1 µUSD, must round to 1 not 0. A thousand such calls would
	// otherwise cost 100 µUSD while the budget recorded zero.
	if got := cm.Cost(1, 0); got != 1 {
		t.Errorf("Cost(1,0) = %d, want 1 (rounded up)", got)
	}
	if got := cm.Cost(1000, 1000); got != 500 {
		t.Errorf("Cost(1000,1000) = %d, want 500", got)
	}
	if got := cm.Cost(0, 0); got != 0 {
		t.Errorf("Cost(0,0) = %d, want 0", got)
	}
}

func TestRequiresHumanApprovalIsInclusiveAtTheCeiling(t *testing.T) {
	g := NewGuard(testPolicy(), 100, 100)
	for _, tc := range []struct {
		value uint64
		want  bool
	}{
		{49_999_999, false},
		{50_000_000, true}, // exactly at the ceiling still needs a human
		{50_000_001, true},
	} {
		if got := g.RequiresHumanApproval(tc.value); got != tc.want {
			t.Errorf("RequiresHumanApproval(%d) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestEstimateTokensBiasesHigh(t *testing.T) {
	// Must never under-estimate, or the pre-authorisation ceiling under-refuses.
	if got := EstimateTokens(""); got != 1 {
		t.Errorf("EstimateTokens(\"\") = %d, want 1", got)
	}
	if got := EstimateTokens("abc"); got != 1 {
		t.Errorf("EstimateTokens(3 bytes) = %d, want 1", got)
	}
	if got := EstimateTokens("abcde"); got != 2 {
		t.Errorf("EstimateTokens(5 bytes) = %d, want 2 (rounds up)", got)
	}
}

func TestHashPromptIsStableAndHex(t *testing.T) {
	h := HashPrompt("draft a purchase order")
	if len(h) != 64 {
		t.Fatalf("HashPrompt length = %d, want 64", len(h))
	}
	if h != HashPrompt("draft a purchase order") {
		t.Error("HashPrompt is not deterministic")
	}
	if h == HashPrompt("draft a purchase order.") {
		t.Error("HashPrompt collided on different inputs")
	}
	// The contract pins ^[0-9a-f]{64}$; an uppercase encoding would fail validation at the edge.
	if strings.ToLower(h) != h {
		t.Error("HashPrompt must be lowercase hex to satisfy the protobuf pattern")
	}
}
