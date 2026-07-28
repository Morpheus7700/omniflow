// Package aigov holds the governance policy applied to every autonomous agent action.
//
// The distinguishing risk of an agent that spends money is not that the model is wrong — it is that
// nobody can answer, afterwards, what it did and under what authority. So this package is built
// around three rules:
//
//  1. Every call is authorised BEFORE it is made. Spend caps enforced after the fact are not caps.
//  2. Every call produces a Decision record whether it succeeded, was refused, or failed. A
//     governance trail with survivorship bias is not a trail.
//  3. Nothing here performs I/O. The policy is a pure function of (request, spend-so-far), which is
//     what makes it testable and what stops it from silently becoming per-process state.
//
// That last point is deliberate and worth stating, because the obvious implementation is wrong: an
// in-memory spend counter inside the Guard would be per-pod, so N replicas would permit N times the
// configured budget while every one of them reported compliance. Durable spend is read from the
// audit table by the adapter and passed in.
package aigov

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"golang.org/x/time/rate"
)

// Sentinel errors. Callers map these onto their own transient/terminal taxonomy: RateLimited is
// transient (retry later), every other refusal is terminal (retrying changes nothing).
var (
	ErrRateLimited     = errors.New("agent rate limit exceeded")
	ErrBudgetExhausted = errors.New("agent budget exhausted for this workflow")
	ErrModelNotAllowed = errors.New("model not on the allowlist")
)

// Outcome is the terminal disposition of an agent call, recorded on every Decision.
type Outcome string

const (
	OutcomeAccepted           Outcome = "accepted"
	OutcomeRejectedValidation Outcome = "rejected_validation"
	OutcomeRejectedPolicy     Outcome = "rejected_policy"
	OutcomeError              Outcome = "error"
)

// CostModel prices a model in micro-USD (1e-6 USD) per 1000 tokens. Integral units throughout:
// representing money as float64 is a defect waiting for a rounding complaint.
type CostModel struct {
	PromptMicroUSDPer1K     uint64
	CompletionMicroUSDPer1K uint64
}

// Cost returns the micro-USD cost of a call. Rounds up, so the budget can never be overrun by
// accumulated truncation.
func (c CostModel) Cost(promptTokens, completionTokens uint32) uint64 {
	ceilDiv := func(n uint64, d uint64) uint64 { return (n + d - 1) / d }
	return ceilDiv(uint64(promptTokens)*c.PromptMicroUSDPer1K, 1000) +
		ceilDiv(uint64(completionTokens)*c.CompletionMicroUSDPer1K, 1000)
}

// Policy is the governance configuration. Zero values are refusals, not defaults: an unconfigured
// Guard denies every call rather than permitting unbounded spend.
type Policy struct {
	// AllowedModels is an exact-match allowlist. A model that is not named cannot be called, which
	// prevents a config typo or a prompt-injected model override from reaching an unpriced endpoint.
	AllowedModels map[string]CostModel

	// MaxCostPerWorkflowMicroUSD caps cumulative spend for a single workflow across all attempts,
	// including retries. This is the control that bounds a redelivery loop's blast radius.
	MaxCostPerWorkflowMicroUSD uint64

	// MaxEstimatedCallMicroUSD refuses any single call whose worst-case cost exceeds it.
	MaxEstimatedCallMicroUSD uint64

	// AutonomousCeilingMicroUSD is the purchase-order value at or above which human approval is
	// mandatory regardless of agent confidence. Below it, the DAG may proceed unattended.
	AutonomousCeilingMicroUSD uint64
}

// Request describes an intended agent call.
type Request struct {
	AgentID    string
	Model      string
	WorkflowID string
	NodeID     string
	Prompt     string

	// MaxCompletionTokens bounds the response, and with the prompt size gives the worst-case cost
	// used for pre-authorisation.
	MaxCompletionTokens uint32
}

// Guard applies Policy. It is safe for concurrent use.
type Guard struct {
	policy  Policy
	limiter *rate.Limiter
	now     func() time.Time
}

// NewGuard builds a Guard. rps/burst bound call frequency per process — a proactive
// denial-of-wallet ceiling, so a redelivery storm cannot convert into a spend spike faster than an
// operator can react.
func NewGuard(p Policy, rps float64, burst int) *Guard {
	return &Guard{
		policy:  p,
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
		now:     time.Now,
	}
}

// Authorize decides whether a call may proceed. spentMicroUSD is this workflow's cumulative spend,
// read durably by the caller — see the package doc for why it is not held here.
//
// It does not block: a rate-limited call is refused rather than queued, so backpressure surfaces as
// a retryable error the consumer already knows how to handle rather than as a stalled partition.
func (g *Guard) Authorize(req Request, spentMicroUSD uint64) (CostModel, error) {
	cm, ok := g.policy.AllowedModels[req.Model]
	if !ok {
		return CostModel{}, fmt.Errorf("%w: %q", ErrModelNotAllowed, req.Model)
	}

	// Worst case: the prompt as estimated, plus a full-length completion.
	worstCase := cm.Cost(EstimateTokens(req.Prompt), req.MaxCompletionTokens)

	if g.policy.MaxEstimatedCallMicroUSD > 0 && worstCase > g.policy.MaxEstimatedCallMicroUSD {
		return CostModel{}, fmt.Errorf("%w: call would cost up to %d µUSD, per-call ceiling is %d",
			ErrBudgetExhausted, worstCase, g.policy.MaxEstimatedCallMicroUSD)
	}

	if spentMicroUSD+worstCase > g.policy.MaxCostPerWorkflowMicroUSD {
		return CostModel{}, fmt.Errorf("%w: spent %d + worst case %d exceeds workflow cap %d µUSD",
			ErrBudgetExhausted, spentMicroUSD, worstCase, g.policy.MaxCostPerWorkflowMicroUSD)
	}

	// Checked last so a call that would be refused on policy grounds does not consume a token from
	// the rate limiter — otherwise a stream of misconfigured requests starves legitimate ones.
	if !g.limiter.AllowN(g.now(), 1) {
		return CostModel{}, ErrRateLimited
	}

	return cm, nil
}

// RequiresHumanApproval reports whether a purchase order of this value must be seen by a person.
// Comparison is >=, so a ceiling of exactly N means N itself needs approval.
func (g *Guard) RequiresHumanApproval(poValueMicroUSD uint64) bool {
	return poValueMicroUSD >= g.policy.AutonomousCeilingMicroUSD
}

// Decision is the audit record. One is written for EVERY agent call — accepted, refused, or failed.
type Decision struct {
	ID           string
	AgentID      string
	Model        string
	WorkflowID   string
	NodeID       string
	PromptSHA256 string
	Prompt       string
	RawResponse  string

	PromptTokens     uint32
	CompletionTokens uint32
	CostMicroUSD     uint64
	LatencyMS        int64

	Outcome      Outcome
	RejectReason string
	CreatedAt    time.Time
}

// HashPrompt returns the hex SHA-256 of a prompt. The hash travels on the event bus while the
// prompt body stays in the audit table, because a prompt built from vendor correspondence is not
// something to broadcast to every consumer of the topic.
func HashPrompt(prompt string) string {
	sum := sha256.Sum256([]byte(prompt))
	return hex.EncodeToString(sum[:])
}

// EstimateTokens approximates a token count for pre-authorisation.
//
// Four bytes per token is the usual English rule of thumb. It is an ESTIMATE and is used only to
// refuse calls before they are made; the Decision record always carries the provider's reported
// usage, so the audit trail and the budget are reconciled against real numbers rather than this.
// Deliberately biased high (rounds up, minimum 1) so the estimate cannot under-refuse.
func EstimateTokens(s string) uint32 {
	if len(s) == 0 {
		return 1
	}
	n := (len(s) + 3) / 4
	if n > int(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(n)
}
