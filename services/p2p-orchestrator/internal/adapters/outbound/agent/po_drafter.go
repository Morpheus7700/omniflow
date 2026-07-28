// Package agent holds the autonomous agents that execute DAG nodes.
//
// An agent is an adapter, not domain logic: it turns a NodeRequest into a NodeResult by talking to
// a model, and everything it returns is verified by the domain before it is persisted. The model
// proposes; the code disposes.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commv1 "omniflow/contracts/communication/v1"
	procv1 "omniflow/contracts/procurement/v1"
	"omniflow/internal/platform/aigov"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"
)

const agentID = "po-drafter"

// maxResponseBytes caps what we read from the gateway. An unbounded io.ReadAll on a remote response
// is a memory-exhaustion vector regardless of how much we trust the endpoint.
const maxResponseBytes = 1 << 20

// PODrafter drafts a purchase order from the event that triggered the workflow.
type PODrafter struct {
	client   *http.Client
	baseURL  string
	apiKey   string
	model    string
	guard    *aigov.Guard
	auditor  ports.AgentAuditor
	maxToks  uint32
	callTOut time.Duration
}

func NewPODrafter(baseURL, apiKey, model string, guard *aigov.Guard, auditor ports.AgentAuditor) *PODrafter {
	return &PODrafter{
		// A per-call timeout on the client as well as on the context: an agent call that never
		// returns would otherwise pin a workflow and, with it, a Kafka partition.
		client:   &http.Client{Timeout: 30 * time.Second},
		baseURL:  baseURL,
		apiKey:   apiKey,
		model:    model,
		guard:    guard,
		auditor:  auditor,
		maxToks:  1024,
		callTOut: 25 * time.Second,
	}
}

// draftResponse is the structured output contract we require from the model.
//
// Amounts are strings, not numbers: decoding money through float64 would reintroduce exactly the
// precision loss the rest of this system takes pains to avoid, and it would do so silently.
type draftResponse struct {
	VendorID string `json:"vendor_id"`
	Currency string `json:"currency"`
	Lines    []struct {
		SKU            string `json:"sku"`
		Description    string `json:"description"`
		Quantity       string `json:"quantity"`
		UnitPrice      string `json:"unit_price"`
		ExtendedAmount string `json:"extended_amount"`
	} `json:"lines"`
	TotalAmount string `json:"total_amount"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens uint32        `json:"max_tokens"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     uint32 `json:"prompt_tokens"`
		CompletionTokens uint32 `json:"completion_tokens"`
	} `json:"usage"`
}

// Execute drafts a PO. Every path through this function writes exactly one audit row before it
// returns, including the refusal paths — a governance trail that records only the calls that
// happened cannot answer "what did we stop it from doing".
func (a *PODrafter) Execute(ctx context.Context, req domain.NodeRequest) (*domain.NodeResult, error) {
	var trigger commv1.VendorEmailReceived
	if err := proto.Unmarshal(req.TriggerPayload, &trigger); err != nil {
		return nil, fmt.Errorf("%w: trigger payload is not a VendorEmailReceived: %v", domain.ErrTerminal, err)
	}

	prompt := buildPrompt(&trigger)
	decision := aigov.Decision{
		ID:           uuid.NewString(),
		AgentID:      agentID,
		Model:        a.model,
		WorkflowID:   req.WorkflowID,
		NodeID:       req.NodeID,
		PromptSHA256: aigov.HashPrompt(prompt),
		Prompt:       prompt,
		CreatedAt:    time.Now().UTC(),
	}

	// Durable spend, read fresh. Holding this in memory would make the cap per-pod, so N replicas
	// would permit N times the budget while each reported compliance.
	spent, err := a.auditor.SpentMicroUSD(ctx, req.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("read agent spend: %w", err)
	}

	costModel, authErr := a.guard.Authorize(aigov.Request{
		AgentID:             agentID,
		Model:               a.model,
		WorkflowID:          req.WorkflowID,
		NodeID:              req.NodeID,
		Prompt:              prompt,
		MaxCompletionTokens: a.maxToks,
	}, spent)
	if authErr != nil {
		decision.Outcome = aigov.OutcomeRejectedPolicy
		decision.RejectReason = authErr.Error()
		a.record(ctx, decision, req.Attempt)
		return nil, classifyGovernanceError(authErr)
	}

	callCtx, cancel := context.WithTimeout(ctx, a.callTOut)
	defer cancel()

	start := time.Now()
	raw, usage, callErr := a.call(callCtx, prompt)
	decision.LatencyMS = time.Since(start).Milliseconds()
	decision.RawResponse = raw
	decision.PromptTokens = usage.PromptTokens
	decision.CompletionTokens = usage.CompletionTokens
	decision.CostMicroUSD = costModel.Cost(usage.PromptTokens, usage.CompletionTokens)

	if callErr != nil {
		decision.Outcome = aigov.OutcomeError
		decision.RejectReason = callErr.Error()
		a.record(ctx, decision, req.Attempt)
		return nil, callErr
	}

	po, parseErr := a.parseAndBuild(raw, req, decision.ID)
	if parseErr != nil {
		decision.Outcome = aigov.OutcomeRejectedValidation
		decision.RejectReason = parseErr.Error()
		a.record(ctx, decision, req.Attempt)
		// Terminal: the same prompt will produce the same malformed answer, so retrying spends
		// money to reach the identical rejection.
		return nil, parseErr
	}

	needsHuman := a.guard.RequiresHumanApproval(po.TotalMicroUSD())
	po.HumanApproved = false

	decision.Outcome = aigov.OutcomeAccepted
	a.record(ctx, decision, req.Attempt)

	payload, err := marshalEvent(po, req, decision, needsHuman)
	if err != nil {
		return nil, err
	}

	slog.Info("purchase order drafted",
		"workflow_id", req.WorkflowID, "po_number", po.PONumber, "vendor", po.MDMVendorID,
		"total", po.TotalAmount.String(), "currency", po.Currency,
		"decision_id", decision.ID, "cost_micro_usd", decision.CostMicroUSD,
		"latency_ms", decision.LatencyMS, "requires_human_approval", needsHuman)

	return &domain.NodeResult{
		EventType:             "PurchaseOrderDrafted",
		Payload:               payload,
		Effect:                po,
		RequiresHumanApproval: needsHuman,
	}, nil
}

// record persists the audit row. A failure to write the trail is logged loudly but does not mask
// the original outcome — the caller's error is the more useful one to propagate.
func (a *PODrafter) record(ctx context.Context, d aigov.Decision, attempt int) {
	if err := a.auditor.RecordDecision(ctx, d, attempt); err != nil {
		slog.Error("FAILED TO WRITE AGENT AUDIT ROW", "error", err,
			"decision_id", d.ID, "workflow_id", d.WorkflowID, "outcome", d.Outcome)
	}
}

func (a *PODrafter) parseAndBuild(raw string, req domain.NodeRequest, decisionID string) (*domain.PurchaseOrderEffect, error) {
	var dr draftResponse
	if err := json.Unmarshal([]byte(raw), &dr); err != nil {
		return nil, fmt.Errorf("%w: model output is not valid JSON: %v", domain.ErrTerminal, err)
	}

	po := &domain.PurchaseOrderEffect{
		PONumber:    domain.DeterministicPONumber(req.WorkflowID, req.Attempt),
		MDMVendorID: dr.VendorID,
		Currency:    dr.Currency,
		DecisionID:  decisionID,
	}

	for i, l := range dr.Lines {
		qty, err := decimal.NewFromString(l.Quantity)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d quantity %q is not a number", domain.ErrTerminal, i, l.Quantity)
		}
		price, err := decimal.NewFromString(l.UnitPrice)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d unit_price %q is not a number", domain.ErrTerminal, i, l.UnitPrice)
		}
		ext, err := decimal.NewFromString(l.ExtendedAmount)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d extended_amount %q is not a number", domain.ErrTerminal, i, l.ExtendedAmount)
		}
		po.Lines = append(po.Lines, domain.PurchaseOrderLine{
			SKU: l.SKU, Description: l.Description,
			Quantity: qty, UnitPrice: price, ExtendedAmount: ext,
		})
	}

	total, err := decimal.NewFromString(dr.TotalAmount)
	if err != nil {
		return nil, fmt.Errorf("%w: total_amount %q is not a number", domain.ErrTerminal, dr.TotalAmount)
	}
	po.TotalAmount = total

	// The deterministic check the whole design rests on. A model will happily return arithmetic
	// that is confident and wrong; this is where that stops being our problem.
	if err := po.Validate(); err != nil {
		return nil, err
	}
	return po, nil
}

func (a *PODrafter) call(ctx context.Context, prompt string) (string, struct{ PromptTokens, CompletionTokens uint32 }, error) {
	var usage struct{ PromptTokens, CompletionTokens uint32 }

	body, err := json.Marshal(chatRequest{
		Model:     a.model,
		Messages:  []chatMessage{{Role: "user", Content: prompt}},
		MaxTokens: a.maxToks,
	})
	if err != nil {
		return "", usage, fmt.Errorf("%w: marshal request: %v", domain.ErrTerminal, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", usage, fmt.Errorf("%w: build request: %v", domain.ErrTerminal, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		// Network-level failure: the call may succeed on a retry.
		return "", usage, fmt.Errorf("%w: gateway call: %v", domain.ErrTransient, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", usage, fmt.Errorf("%w: read response: %v", domain.ErrTransient, err)
	}

	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		// Overload or a server fault — worth retrying.
		return "", usage, fmt.Errorf("%w: gateway status %d", domain.ErrTransient, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		// 4xx: a bad request or bad credentials. Identical on every retry.
		return "", usage, fmt.Errorf("%w: gateway status %d", domain.ErrTerminal, resp.StatusCode)
	}

	var cr chatResponse
	if err := json.Unmarshal(raw, &cr); err != nil {
		return "", usage, fmt.Errorf("%w: gateway response is not valid JSON: %v", domain.ErrTerminal, err)
	}
	if len(cr.Choices) == 0 {
		return "", usage, fmt.Errorf("%w: gateway returned no choices", domain.ErrTerminal)
	}
	usage.PromptTokens = cr.Usage.PromptTokens
	usage.CompletionTokens = cr.Usage.CompletionTokens
	return cr.Choices[0].Message.Content, usage, nil
}

func marshalEvent(po *domain.PurchaseOrderEffect, req domain.NodeRequest, d aigov.Decision, needsHuman bool) ([]byte, error) {
	now := timestamppb.New(time.Now().UTC())
	ev := &procv1.PurchaseOrderDrafted{
		EventId:           uuid.NewString(),
		TraceParent:       req.TraceParent,
		WorkflowId:        req.WorkflowID,
		OccurredAt:        now,
		PublishedAt:       now,
		SequenceEngineKey: req.SequenceKey,
		PoNumber:          po.PONumber,
		MdmVendorId:       po.MDMVendorID,
		Currency:          po.Currency,
		TotalAmount:       po.TotalAmount.StringFixed(4),
		Provenance: &procv1.AgentProvenance{
			AgentId:          d.AgentID,
			Model:            d.Model,
			PromptSha256:     d.PromptSHA256,
			DecisionId:       d.ID,
			PromptTokens:     d.PromptTokens,
			CompletionTokens: d.CompletionTokens,
			CostMicroUsd:     d.CostMicroUSD,
			HumanApproved:    !needsHuman,
		},
	}
	for _, l := range po.Lines {
		ev.Lines = append(ev.Lines, &procv1.PurchaseOrderLine{
			Sku:            l.SKU,
			Description:    l.Description,
			Quantity:       l.Quantity.StringFixed(4),
			UnitPrice:      l.UnitPrice.StringFixed(4),
			ExtendedAmount: l.ExtendedAmount.StringFixed(4),
		})
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal PurchaseOrderDrafted: %v", domain.ErrTerminal, err)
	}
	return b, nil
}

// classifyGovernanceError maps a refusal onto the orchestrator's retry taxonomy. Rate limiting is
// the only refusal worth retrying; a budget or allowlist refusal is a decision, not a fault, and
// retrying it would burn the consumer's retry budget to reach the identical answer.
func classifyGovernanceError(err error) error {
	if errors.Is(err, aigov.ErrRateLimited) {
		return fmt.Errorf("%w: %v", domain.ErrTransient, err)
	}
	return fmt.Errorf("%w: %v", domain.ErrTerminal, err)
}

func buildPrompt(t *commv1.VendorEmailReceived) string {
	return fmt.Sprintf(`You are a procurement drafting agent. Draft a purchase order from the vendor request below.

Respond with ONLY a JSON object, no prose and no code fence, in exactly this shape:
{"vendor_id":"<string>","currency":"<3-letter ISO code>","lines":[{"sku":"<string>","description":"<string>","quantity":"<decimal string>","unit_price":"<decimal string>","extended_amount":"<decimal string>"}],"total_amount":"<decimal string>"}

Rules:
- extended_amount MUST equal quantity * unit_price for every line.
- total_amount MUST equal the sum of all extended_amount values.
- All amounts are decimal strings. Never use scientific notation.

Vendor request:
  vendor id: %s
  event id:  %s
`, t.GetMdmVendorId(), t.GetEventId())
}
