package domain

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// NodeRequest is everything a node executor is given. It is built entirely from durable state —
// the workflow row plus its stored trigger payload — so a node can be executed by any pod that
// picks the workflow up, including one that never saw the originating Kafka record.
type NodeRequest struct {
	WorkflowID  string
	EventID     string
	NodeID      string
	Attempt     int
	TraceParent string
	SequenceKey uint64

	// TriggerPayload is the marshalled event that started the workflow, read back from
	// workflows.trigger_payload. Executors decode it themselves.
	TriggerPayload []byte
}

// NodeResult is what an executor produces. The orchestrator writes Effect and the outbox row in one
// transaction, so a node's side effect and its emitted event either both land or neither does.
type NodeResult struct {
	// EventType names the outbox row's event_type column, e.g. "PurchaseOrderDrafted".
	EventType string
	// Payload is the serialised event body, written to orchestrator_outbox.payload.
	Payload []byte
	// Effect, if non-nil, is a side effect to persist in the SAME transaction as the checkpoint.
	Effect *PurchaseOrderEffect
	// RequiresHumanApproval lets a node escalate to the HITL gate dynamically — a PO above the
	// autonomous ceiling must be seen by a person even though the DAG would otherwise proceed.
	RequiresHumanApproval bool
}

// PurchaseOrderLine is one line of a drafted purchase order.
type PurchaseOrderLine struct {
	SKU            string          `json:"sku"`
	Description    string          `json:"description"`
	Quantity       decimal.Decimal `json:"quantity"`
	UnitPrice      decimal.Decimal `json:"unit_price"`
	ExtendedAmount decimal.Decimal `json:"extended_amount"`
}

// PurchaseOrderEffect is the durable artifact a drafting node produces.
type PurchaseOrderEffect struct {
	PONumber      string
	MDMVendorID   string
	Currency      string
	Lines         []PurchaseOrderLine
	TotalAmount   decimal.Decimal
	DecisionID    string
	HumanApproved bool
}

// Validate enforces the invariants an agent is NOT trusted to hold.
//
// A language model asked for structured output will produce plausible arithmetic, and plausible is
// not the same as correct — it will occasionally return a total that does not equal the sum of its
// lines, or a line whose extended amount is not quantity × unit price. Those are the errors that
// turn into real money, so they are recomputed here rather than accepted. This is the deterministic
// check that makes an agent's output safe to persist: the model proposes, the code verifies.
func (po *PurchaseOrderEffect) Validate() error {
	if po.PONumber == "" {
		return fmt.Errorf("%w: po_number is empty", ErrTerminal)
	}
	if po.MDMVendorID == "" {
		return fmt.Errorf("%w: mdm_vendor_id is empty", ErrTerminal)
	}
	if len(po.Currency) != 3 {
		return fmt.Errorf("%w: currency %q is not a 3-letter code", ErrTerminal, po.Currency)
	}
	if len(po.Lines) == 0 {
		return fmt.Errorf("%w: purchase order has no lines", ErrTerminal)
	}
	if len(po.Lines) > 200 {
		return fmt.Errorf("%w: purchase order has %d lines, max 200", ErrTerminal, len(po.Lines))
	}

	sum := decimal.Zero
	for i, l := range po.Lines {
		if l.SKU == "" {
			return fmt.Errorf("%w: line %d has no SKU", ErrTerminal, i)
		}
		if l.Quantity.IsNegative() || l.Quantity.IsZero() {
			return fmt.Errorf("%w: line %d (%s) quantity %s must be positive", ErrTerminal, i, l.SKU, l.Quantity)
		}
		if l.UnitPrice.IsNegative() {
			return fmt.Errorf("%w: line %d (%s) unit price %s is negative", ErrTerminal, i, l.SKU, l.UnitPrice)
		}
		want := l.Quantity.Mul(l.UnitPrice)
		if !l.ExtendedAmount.Equal(want) {
			return fmt.Errorf("%w: line %d (%s) extended amount %s != quantity %s x unit price %s = %s",
				ErrTerminal, i, l.SKU, l.ExtendedAmount, l.Quantity, l.UnitPrice, want)
		}
		sum = sum.Add(want)
	}

	if !po.TotalAmount.Equal(sum) {
		return fmt.Errorf("%w: total %s != sum of lines %s", ErrTerminal, po.TotalAmount, sum)
	}
	if po.TotalAmount.IsNegative() {
		return fmt.Errorf("%w: total %s is negative", ErrTerminal, po.TotalAmount)
	}
	return nil
}

// TotalMicroUSD renders the total in micro-USD for the governance ceiling check. Truncation is
// toward zero, which cannot push a PO under a ceiling by more than a rounding unit.
func (po *PurchaseOrderEffect) TotalMicroUSD() uint64 {
	if po.TotalAmount.IsNegative() {
		return 0
	}
	return uint64(po.TotalAmount.Mul(decimal.NewFromInt(1_000_000)).IntPart())
}

// DeterministicPONumber derives a PO number from the idempotency triple, so a re-executed node
// produces the SAME number rather than a second one. Paired with the uq_po_number constraint this
// makes duplicate issuance impossible rather than merely unlikely.
func DeterministicPONumber(workflowID string, attempt int) string {
	short := workflowID
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("PO-%s-%d", short, attempt)
}
