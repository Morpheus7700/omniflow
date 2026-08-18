package domain

import (
	"errors"
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal {
	v, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return v
}

func validPO() *PurchaseOrderEffect {
	return &PurchaseOrderEffect{
		PONumber:    "PO-abc12345-1",
		MDMVendorID: "VENDOR-9",
		Currency:    "USD",
		Lines: []PurchaseOrderLine{
			{SKU: "SKU-1", Quantity: d("10"), UnitPrice: d("2.50"), ExtendedAmount: d("25.00")},
			{SKU: "SKU-2", Quantity: d("3"), UnitPrice: d("1.25"), ExtendedAmount: d("3.75")},
		},
		TotalAmount: d("28.75"),
		DecisionID:  "11111111-1111-1111-1111-111111111111",
	}
}

func TestValidateAcceptsAWellFormedPO(t *testing.T) {
	if err := validPO().Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

// The core governance test: a model that returns confident, wrong arithmetic must not get a PO
// persisted. These are the errors that turn into real money.
func TestValidateRejectsFabricatedArithmetic(t *testing.T) {
	t.Run("total does not match the sum of lines", func(t *testing.T) {
		po := validPO()
		po.TotalAmount = d("28.00") // understated by 0.75
		err := po.Validate()
		if err == nil {
			t.Fatal("Validate() = nil, want an error for a mismatched total")
		}
		if !errors.Is(err, ErrTerminal) {
			t.Errorf("error must be terminal (retrying will not fix bad arithmetic), got %v", err)
		}
	})

	t.Run("line extended amount is not quantity x unit price", func(t *testing.T) {
		po := validPO()
		po.Lines[0].ExtendedAmount = d("20.00") // should be 25.00
		po.TotalAmount = d("23.75")             // internally consistent with the lie
		if err := po.Validate(); err == nil {
			t.Fatal("Validate() = nil, want an error for a bad line extension")
		}
	})
}

func TestValidateRejectsStructurallyInvalidPOs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PurchaseOrderEffect)
	}{
		{"no lines", func(p *PurchaseOrderEffect) { p.Lines = nil; p.TotalAmount = decimal.Zero }},
		{"empty po number", func(p *PurchaseOrderEffect) { p.PONumber = "" }},
		{"empty vendor", func(p *PurchaseOrderEffect) { p.MDMVendorID = "" }},
		{"bad currency", func(p *PurchaseOrderEffect) { p.Currency = "DOLLARS" }},
		{"empty sku", func(p *PurchaseOrderEffect) { p.Lines[0].SKU = "" }},
		{"zero quantity", func(p *PurchaseOrderEffect) {
			p.Lines[0].Quantity = decimal.Zero
			p.Lines[0].ExtendedAmount = decimal.Zero
			p.TotalAmount = d("3.75")
		}},
		{"negative quantity", func(p *PurchaseOrderEffect) {
			p.Lines[0].Quantity = d("-10")
			p.Lines[0].ExtendedAmount = d("-25.00")
			p.TotalAmount = d("-21.25")
		}},
		{"negative unit price", func(p *PurchaseOrderEffect) {
			p.Lines[0].UnitPrice = d("-2.50")
			p.Lines[0].ExtendedAmount = d("-25.00")
			p.TotalAmount = d("-21.25")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			po := validPO()
			tc.mutate(po)
			if err := po.Validate(); err == nil {
				t.Errorf("Validate() = nil, want an error for %s", tc.name)
			}
		})
	}
}

func TestValidateRejectsTooManyLines(t *testing.T) {
	po := validPO()
	po.Lines = make([]PurchaseOrderLine, 201)
	sum := decimal.Zero
	for i := range po.Lines {
		po.Lines[i] = PurchaseOrderLine{SKU: "S", Quantity: d("1"), UnitPrice: d("1"), ExtendedAmount: d("1")}
		sum = sum.Add(d("1"))
	}
	po.TotalAmount = sum
	if err := po.Validate(); err == nil {
		t.Error("Validate() = nil, want an error for 201 lines")
	}
}

func TestValidateAcceptsHighPrecisionArithmetic(t *testing.T) {
	// decimal, not float64: 0.1 * 3 must be exactly 0.3.
	po := validPO()
	po.Lines = []PurchaseOrderLine{
		{SKU: "SKU-1", Quantity: d("3"), UnitPrice: d("0.1"), ExtendedAmount: d("0.3")},
	}
	po.TotalAmount = d("0.3")
	if err := po.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil (decimal arithmetic must be exact)", err)
	}
}

// An overflow here would wrap a colossal total down to a small number, which would sail under the
// autonomous-approval ceiling — the largest imaginable PO becoming the one that needs no human.
// Saturating fails in the safe direction.
func TestTotalMicroUSDSaturatesInsteadOfWrapping(t *testing.T) {
	po := &PurchaseOrderEffect{TotalAmount: d("99999999999999999999999999")}
	got := po.TotalMicroUSD()
	if got != ^uint64(0) {
		t.Fatalf("TotalMicroUSD() = %d, want saturation to MaxUint64", got)
	}
	// And the point of saturating: it must still be above any plausible ceiling.
	if got < 50_000_000_000 {
		t.Error("saturated value fell under the autonomous ceiling; overflow would skip human approval")
	}
}

func TestTotalMicroUSD(t *testing.T) {
	for _, tc := range []struct {
		total string
		want  uint64
	}{
		{"0", 0},
		{"1", 1_000_000},
		{"28.75", 28_750_000},
		{"0.000001", 1},
	} {
		po := &PurchaseOrderEffect{TotalAmount: d(tc.total)}
		if got := po.TotalMicroUSD(); got != tc.want {
			t.Errorf("TotalMicroUSD(%s) = %d, want %d", tc.total, got, tc.want)
		}
	}
}

// The idempotency guarantee: a re-executed node must derive the SAME number, or the unique
// constraint cannot stop a duplicate PO.
func TestDeterministicPONumberIsStableAcrossRetries(t *testing.T) {
	wf := "abc12345-0000-0000-0000-000000000000"
	first := DeterministicPONumber(wf, 1)
	if second := DeterministicPONumber(wf, 1); first != second {
		t.Errorf("not deterministic: %q vs %q", first, second)
	}
	if other := DeterministicPONumber(wf, 2); other == first {
		t.Error("different attempts must produce different PO numbers")
	}
	if other := DeterministicPONumber("zzz99999-0000-0000-0000-000000000000", 1); other == first {
		t.Error("different workflows must produce different PO numbers")
	}
	if got := DeterministicPONumber("short", 1); got != "PO-short-1" {
		t.Errorf("short workflow id: got %q", got)
	}
}
