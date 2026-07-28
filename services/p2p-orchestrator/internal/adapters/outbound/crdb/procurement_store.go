package crdb

import (
	"context"
	"encoding/json"
	"fmt"

	"omniflow/internal/platform/aigov"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"
)

// InsertPurchaseOrder writes the agent's side effect inside the caller's transaction.
//
// ON CONFLICT DO NOTHING against uq_po_per_node is the exactly-once control. Node execution is
// at-least-once — the LLM call happens outside the transaction, so a crash between the call and the
// checkpoint re-runs the node — and rather than trying to prevent the duplicate call (which would
// require consensus with a third party we do not control), the EFFECT is made idempotent. N
// executions of a node yield exactly one purchase order.
//
// Returns false when the row already existed, which is a successful outcome, not a failure.
func (s *Store) InsertPurchaseOrder(
	ctx context.Context,
	tx ports.Transaction,
	workflowID, nodeID string,
	attempt int,
	po *domain.PurchaseOrderEffect,
) (bool, error) {
	ptx, ok := tx.(*txWrapper)
	if !ok {
		return false, fmt.Errorf("%w: InsertPurchaseOrder got a %T, not the store's own transaction", domain.ErrTerminal, tx)
	}

	// Re-validate at the storage boundary. The caller already validated, but this is the last point
	// before money becomes durable and the check is cheap; a future caller that forgets is a bug we
	// would rather catch here than in a vendor's inbox.
	if err := po.Validate(); err != nil {
		return false, err
	}

	lines, err := json.Marshal(po.Lines)
	if err != nil {
		return false, fmt.Errorf("%w: marshal PO lines: %v", domain.ErrTerminal, err)
	}

	tag, err := ptx.tx.Exec(ctx, `
		INSERT INTO purchase_orders (
			workflow_id, node_id, attempt, po_number, mdm_vendor_id,
			currency, total_amount, lines, decision_id, human_approved
		)
		VALUES ($1::UUID, $2, $3, $4, $5, $6, $7::DECIMAL, $8::JSONB, $9::UUID, $10)
		ON CONFLICT ON CONSTRAINT uq_po_per_node DO NOTHING
	`,
		workflowID, nodeID, attempt, po.PONumber, po.MDMVendorID,
		po.Currency, po.TotalAmount.String(), string(lines), po.DecisionID, po.HumanApproved,
	)
	if err != nil {
		return false, classify(err)
	}
	return tag.RowsAffected() == 1, nil
}

// SpentMicroUSD totals this workflow's agent spend across every attempt.
//
// Read on the pool rather than in a transaction, and deliberately so: it is called BEFORE the node
// acquires its lease, so it must not participate in — or block on — the workflow row lock.
func (s *Store) SpentMicroUSD(ctx context.Context, workflowID string) (uint64, error) {
	var spent int64
	err := s.pool.QueryRow(ctx, `
		SELECT coalesce(sum(cost_micro_usd), 0)::INT8
		FROM agent_decisions WHERE workflow_id = $1::UUID
	`, workflowID).Scan(&spent)
	if err != nil {
		return 0, classify(err)
	}
	if spent < 0 {
		return 0, nil
	}
	return uint64(spent), nil
}

// RecordDecision appends to the governance trail.
//
// Written on the pool, OUTSIDE any workflow transaction, and that is the important property: the
// audit row must survive even when the transaction that would have carried it rolls back. A trail
// that disappears whenever the work fails is precisely the trail nobody can use, because the
// interesting question in a review is what the agent tried to do and was stopped from doing.
func (s *Store) RecordDecision(ctx context.Context, d aigov.Decision, attempt int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO agent_decisions (
			id, workflow_id, node_id, attempt, agent_id, model,
			prompt_sha256, prompt, raw_response,
			prompt_tokens, completion_tokens, cost_micro_usd, latency_ms,
			outcome, reject_reason, created_at
		)
		VALUES ($1::UUID, $2::UUID, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (id) DO NOTHING
	`,
		d.ID, d.WorkflowID, d.NodeID, attempt, d.AgentID, d.Model,
		d.PromptSHA256, d.Prompt, d.RawResponse,
		// Clamped, not cast. The columns are INT4/INT8 (signed) while the provider reports unsigned
		// counts, so a hostile or broken gateway returning a value above the signed maximum would
		// otherwise wrap NEGATIVE. A negative cost is worse than a wrong one: SpentMicroUSD sums
		// this column to enforce the budget, so one negative row would credit the workflow spend
		// and let it exceed its cap. Clamping keeps the sum monotonic.
		clampInt32(d.PromptTokens), clampInt32(d.CompletionTokens), clampInt64(d.CostMicroUSD), d.LatencyMS,
		string(d.Outcome), d.RejectReason, d.CreatedAt,
	)
	return classify(err)
}

// clampInt32 narrows a uint32 to the INT4 column width without wrapping.
func clampInt32(v uint32) int32 {
	const maxInt32 = 1<<31 - 1
	if v > maxInt32 {
		return maxInt32
	}
	return int32(v)
}

// clampInt64 narrows a uint64 to the INT8 column width without wrapping. See the note at the call
// site: a wrapped negative cost would corrupt budget enforcement, not merely the audit record.
func clampInt64(v uint64) int64 {
	const maxInt64 = 1<<63 - 1
	if v > maxInt64 {
		return maxInt64
	}
	return int64(v)
}
