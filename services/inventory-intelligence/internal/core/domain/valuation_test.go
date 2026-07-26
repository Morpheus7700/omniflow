package domain

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// ─── Test doubles ────────────────────────────────────────────────────────────────────────────────
//
// ValuationService depends only on the InventoryStore / LedgerAccess ports, so the whole FIFO,
// moving-average and HLC-restatement surface is exercisable in-process. No Docker, no CRDB. What
// these fakes deliberately do NOT model is the single-transaction guarantee itself — that lives in
// SQL and is covered by the integration suite.

type fakeLedger struct {
	lots      []FIFOLot
	movements []InventoryMovementReceived
	processed map[string]bool

	resetCalls int // restatement tripwire: proves whether the rewind path was taken
}

func newFakeLedger() *fakeLedger {
	return &fakeLedger{processed: map[string]bool{}}
}

func (f *fakeLedger) IsAlreadyProcessed(_ context.Context, eventID string) (bool, error) {
	return f.processed[eventID], nil
}

func (f *fakeLedger) MarkProcessed(_ context.Context, eventID string) error {
	f.processed[eventID] = true
	return nil
}

// GetLots mirrors the repository contract: strictly ordered by SequenceEngineKey. That ordering IS
// the FIFO guarantee — a fake that returned insertion order would make these tests pass vacuously.
func (f *fakeLedger) GetLots(_ context.Context, sku, locationID string) ([]FIFOLot, error) {
	var out []FIFOLot
	for _, l := range f.lots {
		if l.SKU == sku && l.LocationID == locationID {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SequenceEngineKey < out[j].SequenceEngineKey })
	return out, nil
}

func (f *fakeLedger) SaveLot(_ context.Context, lot FIFOLot) error {
	f.lots = append(f.lots, lot)
	return nil
}

func (f *fakeLedger) UpdateLotQuantity(_ context.Context, lotID string, remaining decimal.Decimal) error {
	for i := range f.lots {
		if f.lots[i].LotID == lotID {
			f.lots[i].RemainingQty = remaining
			return nil
		}
	}
	return errors.New("lot not found: " + lotID)
}

func (f *fakeLedger) GetSnapshotBefore(_ context.Context, _, _ string, _ uint64) (InventoryFact, error) {
	return InventoryFact{}, nil
}

func (f *fakeLedger) SaveMovementLog(_ context.Context, e InventoryMovementReceived) error {
	f.movements = append(f.movements, e)
	return nil
}

func (f *fakeLedger) GetMovementsFrom(_ context.Context, sku, locationID string, seqKey uint64) ([]InventoryMovementReceived, error) {
	var out []InventoryMovementReceived
	for _, m := range f.movements {
		if m.SKU == sku && m.LocationID == locationID && m.SequenceEngineKey >= seqKey {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SequenceEngineKey < out[j].SequenceEngineKey })
	return out, nil
}

func (f *fakeLedger) ResetLots(_ context.Context, sku, locationID string) error {
	f.resetCalls++
	var kept []FIFOLot
	for _, l := range f.lots {
		if l.SKU != sku || l.LocationID != locationID {
			kept = append(kept, l)
		}
	}
	f.lots = kept
	return nil
}

// fakeStore stands in for the CRDB transaction wrapper. It restores the pre-call lot state when
// processFn fails, modelling the rollback the real single transaction provides — without that, a
// failed movement would leave partially-depleted lots behind and later assertions would be lies.
type fakeStore struct {
	ledger    *fakeLedger
	lastFacts []InventoryFact
}

func (s *fakeStore) ProcessInventoryTransaction(
	ctx context.Context,
	event InventoryMovementReceived,
	processFn func(ctx context.Context, e InventoryMovementReceived, ledger LedgerAccess) ([]InventoryFact, error),
) error {
	snapshot := append([]FIFOLot(nil), s.ledger.lots...)
	facts, err := processFn(ctx, event, s.ledger)
	if err != nil {
		s.ledger.lots = snapshot
		return err
	}
	s.lastFacts = facts
	return nil
}

func newSUT() (*ValuationService, *fakeStore, *fakeLedger) {
	l := newFakeLedger()
	st := &fakeStore{ledger: l}
	return NewValuationService(st), st, l
}

// ─── Helpers ─────────────────────────────────────────────────────────────────────────────────────

// assertDec compares by numeric value. Never compare decimals as strings — "2" vs "2.00" vs "2.0"
// are all equal numerically but differ textually, a trap that has produced false passes here before.
func assertDec(t *testing.T, label string, got, want decimal.Decimal) {
	t.Helper()
	if !got.Equal(want) {
		t.Errorf("%s = %s, want %s", label, got.String(), want.String())
	}
}

func dec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

type movementOpt func(*InventoryMovementReceived)

func withOccurredAt(ts time.Time) movementOpt {
	return func(m *InventoryMovementReceived) { m.OccurredAt = ts }
}

func movement(eventID string, seq uint64, mt MovementType, qty, cost string, opts ...movementOpt) InventoryMovementReceived {
	m := InventoryMovementReceived{
		EventID:           eventID,
		SequenceEngineKey: seq,
		MovementType:      mt,
		SKU:               "SKU-1",
		LocationID:        "LOC-1",
		Quantity:          dec(qty),
		UnitCost:          dec(cost),
		OccurredAt:        time.Now(),
	}
	for _, o := range opts {
		o(&m)
	}
	return m
}

func lastFact(t *testing.T, st *fakeStore) InventoryFact {
	t.Helper()
	if len(st.lastFacts) == 0 {
		t.Fatal("no facts emitted")
	}
	return st.lastFacts[len(st.lastFacts)-1]
}

// ─── Receipts and the snapshot projection ────────────────────────────────────────────────────────

func TestReceiptCreatesLotAndSnapshot(t *testing.T) {
	svc, st, led := newSUT()
	ctx := context.Background()

	if err := svc.Process(ctx, movement("evt-1", 100, MovementTypeReceipt, "10", "2")); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(led.lots) != 1 {
		t.Fatalf("lots = %d, want 1", len(led.lots))
	}
	f := lastFact(t, st)
	assertDec(t, "QtyOnHand", f.QtyOnHand, dec("10"))
	assertDec(t, "FifoTotalValue", f.FifoTotalValue, dec("20"))
	assertDec(t, "MovingAvgCost", f.MovingAvgCost, dec("2"))
	assertDec(t, "QtyDelta", f.QtyDelta, dec("10"))
}

// Moving average is value/qty across surviving lots — NOT a running average of unit costs.
// 10@2 + 10@5 => 70/20 = 3.5, which a naive (2+5)/2 = 3.5 coincidence would hide, so the third
// receipt at a different quantity is what actually pins the formula.
func TestMovingAverageIsValueWeighted(t *testing.T) {
	svc, st, _ := newSUT()
	ctx := context.Background()

	for _, m := range []InventoryMovementReceived{
		movement("evt-1", 100, MovementTypeReceipt, "10", "2"),
		movement("evt-2", 200, MovementTypeReceipt, "10", "5"),
		movement("evt-3", 300, MovementTypeReceipt, "80", "1"),
	} {
		if err := svc.Process(ctx, m); err != nil {
			t.Fatalf("Process %s: %v", m.EventID, err)
		}
	}

	f := lastFact(t, st)
	// on hand 100, value 20 + 50 + 80 = 150 -> 1.5 (an unweighted mean would be 8/3 ≈ 2.67)
	assertDec(t, "QtyOnHand", f.QtyOnHand, dec("100"))
	assertDec(t, "FifoTotalValue", f.FifoTotalValue, dec("150"))
	assertDec(t, "MovingAvgCost", f.MovingAvgCost, dec("1.5"))
}

// ─── FIFO depletion ──────────────────────────────────────────────────────────────────────────────

func TestConsumptionDepletesLotsOldestFirst(t *testing.T) {
	svc, st, led := newSUT()
	ctx := context.Background()

	mustProcess(t, svc, movement("evt-1", 100, MovementTypeReceipt, "10", "2"))
	mustProcess(t, svc, movement("evt-2", 200, MovementTypeReceipt, "10", "5"))
	mustProcess(t, svc, movement("evt-3", 300, MovementTypeConsumption, "15", "0"))

	// 10 @ 2 from the older lot + 5 @ 5 from the newer = COGS 45 over 15 units = 3.00
	f := lastFact(t, st)
	assertDec(t, "FifoUnitCost", f.FifoUnitCost, dec("3"))
	assertDec(t, "QtyDelta", f.QtyDelta, dec("-15"))
	assertDec(t, "QtyOnHand", f.QtyOnHand, dec("5"))
	assertDec(t, "FifoTotalValue", f.FifoTotalValue, dec("25"))

	lots, _ := led.GetLots(ctx, "SKU-1", "LOC-1")
	assertDec(t, "oldest lot remaining", lots[0].RemainingQty, decimal.Zero)
	assertDec(t, "newest lot remaining", lots[1].RemainingQty, dec("5"))
}

// FIFO order must follow SequenceEngineKey, not arrival order. Feeding the higher-seq lot first
// catches an implementation that trusts insertion order.
func TestFIFOFollowsSequenceKeyNotArrivalOrder(t *testing.T) {
	svc, st, _ := newSUT()

	// arrive out of order, but both are "on time" relative to an empty ledger
	mustProcess(t, svc, movement("evt-late-seq", 900, MovementTypeReceipt, "10", "9"))
	// seq 100 is now a late arrival -> restatement path rebuilds in HLC order
	mustProcess(t, svc, movement("evt-early-seq", 100, MovementTypeReceipt, "10", "1"))
	mustProcess(t, svc, movement("evt-consume", 950, MovementTypeConsumption, "10", "0"))

	// The seq-100 lot @1 is oldest in HLC order, so it is consumed first: 10 @ 1 => 1.00
	f := lastFact(t, st)
	assertDec(t, "FifoUnitCost", f.FifoUnitCost, dec("1"))
	assertDec(t, "QtyOnHand", f.QtyOnHand, dec("10"))
	assertDec(t, "FifoTotalValue", f.FifoTotalValue, dec("90"))
}

func TestConsumptionBeyondOnHandIsTerminal(t *testing.T) {
	svc, _, led := newSUT()

	mustProcess(t, svc, movement("evt-1", 100, MovementTypeReceipt, "5", "2"))

	err := svc.Process(context.Background(), movement("evt-2", 200, MovementTypeConsumption, "6", "0"))
	if err == nil {
		t.Fatal("expected an error consuming more than on hand")
	}
	// Must be terminal, not transient: retrying can never make 6 units appear, and a transient
	// classification would spin the consumer until it hit the DLQ by exhaustion instead of directly.
	if !errors.Is(err, ErrTerminal) {
		t.Errorf("error = %v, want ErrTerminal", err)
	}
	// The rolled-back transaction must leave the lot intact.
	lots, _ := led.GetLots(context.Background(), "SKU-1", "LOC-1")
	assertDec(t, "lot survived rollback", lots[0].RemainingQty, dec("5"))
}

// ─── Adjustments ─────────────────────────────────────────────────────────────────────────────────

func TestAdjustmentSignSelectsLotCreationOrDepletion(t *testing.T) {
	t.Run("positive adds a lot", func(t *testing.T) {
		svc, st, led := newSUT()
		mustProcess(t, svc, movement("evt-1", 100, MovementTypeAdjustment, "7", "3"))
		if len(led.lots) != 1 {
			t.Fatalf("lots = %d, want 1", len(led.lots))
		}
		assertDec(t, "QtyOnHand", lastFact(t, st).QtyOnHand, dec("7"))
	})

	t.Run("negative depletes", func(t *testing.T) {
		svc, st, _ := newSUT()
		mustProcess(t, svc, movement("evt-1", 100, MovementTypeReceipt, "10", "4"))
		mustProcess(t, svc, movement("evt-2", 200, MovementTypeAdjustment, "-3", "0"))

		f := lastFact(t, st)
		assertDec(t, "QtyDelta", f.QtyDelta, dec("-3"))
		assertDec(t, "QtyOnHand", f.QtyOnHand, dec("7"))
		assertDec(t, "FifoUnitCost", f.FifoUnitCost, dec("4"))
	})
}

// ─── HLC late arrival ────────────────────────────────────────────────────────────────────────────

// The canonical restatement scenario, matching scripts/failtest_fifo_restatement.sh:
//
//	B(seq 200) receipt 10 @ 5      -> on hand 10
//	C(seq 300) consume 5           -> FIFO cost 5.00 (only B exists yet)
//	A(seq 100) receipt 10 @ 2      -> arrives LATE, predates both
//
// After restatement A is oldest, so C must consume from A instead: FIFO cost flips 5.00 -> 2.00 and
// the snapshot settles at 60.00. This is the single highest-value assertion in the suite.
func TestLateArrivalRestatesForward(t *testing.T) {
	svc, st, led := newSUT()

	mustProcess(t, svc, movement("evt-B", 200, MovementTypeReceipt, "10", "5"))
	mustProcess(t, svc, movement("evt-C", 300, MovementTypeConsumption, "5", "0"))

	beforeRestate := lastFact(t, st)
	assertDec(t, "pre-restatement FifoUnitCost", beforeRestate.FifoUnitCost, dec("5"))

	mustProcess(t, svc, movement("evt-A", 100, MovementTypeReceipt, "10", "2"))

	if led.resetCalls != 1 {
		t.Errorf("ResetLots called %d times, want 1 (restatement did not rewind)", led.resetCalls)
	}

	// Replay emits a fact per movement, in HLC order: A, B, C.
	if len(st.lastFacts) != 3 {
		t.Fatalf("restatement emitted %d facts, want 3", len(st.lastFacts))
	}
	restatedC := lastFact(t, st)
	if restatedC.EventID != "evt-C" {
		t.Errorf("final restated fact = %s, want evt-C", restatedC.EventID)
	}
	assertDec(t, "restated FifoUnitCost", restatedC.FifoUnitCost, dec("2"))
	assertDec(t, "restated QtyOnHand", restatedC.QtyOnHand, dec("15"))
	assertDec(t, "restated FifoTotalValue", restatedC.FifoTotalValue, dec("60"))
}

// Past the bounded lateness horizon the event is downgraded to an adjustment and appended, rather
// than triggering an unbounded historical replay.
func TestBeyondLatenessHorizonAdjustsInsteadOfRestating(t *testing.T) {
	svc, st, led := newSUT()

	mustProcess(t, svc, movement("evt-1", 500, MovementTypeReceipt, "10", "5"))

	stale := movement("evt-stale", 100, MovementTypeReceipt, "4", "1",
		withOccurredAt(time.Now().Add(-40*24*time.Hour)))
	mustProcess(t, svc, stale)

	if led.resetCalls != 0 {
		t.Errorf("ResetLots called %d times, want 0 (should not restate beyond the horizon)", led.resetCalls)
	}
	f := lastFact(t, st)
	if f.MovementType != MovementTypeAdjustment {
		t.Errorf("MovementType = %v, want MovementTypeAdjustment", f.MovementType)
	}
	assertDec(t, "QtyOnHand", f.QtyOnHand, dec("14"))
}

func TestBeyondLatenessHorizonBoundary(t *testing.T) {
	svc, _, _ := newSUT()

	tests := []struct {
		name string
		age  time.Duration
		want bool
	}{
		{"fresh", time.Minute, false},
		{"just inside horizon", 29 * 24 * time.Hour, false},
		{"just outside horizon", 31 * 24 * time.Hour, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := movement("evt", 1, MovementTypeReceipt, "1", "1", withOccurredAt(time.Now().Add(-tc.age)))
			if got := svc.beyondLatenessHorizon(e); got != tc.want {
				t.Errorf("beyondLatenessHorizon(age=%s) = %v, want %v", tc.age, got, tc.want)
			}
		})
	}
}

// ─── Pure helpers ────────────────────────────────────────────────────────────────────────────────

func TestMaxSeq(t *testing.T) {
	tests := []struct {
		name string
		lots []FIFOLot
		want uint64
	}{
		{"empty", nil, 0},
		{"single", []FIFOLot{{SequenceEngineKey: 42}}, 42},
		{"max is not last", []FIFOLot{{SequenceEngineKey: 10}, {SequenceEngineKey: 900}, {SequenceEngineKey: 30}}, 900},
		// HLC keys are near the top of the uint64 range in production; a signed-int regression here
		// would silently invert late-arrival detection.
		{"large keys", []FIFOLot{{SequenceEngineKey: 1 << 62}, {SequenceEngineKey: 1<<63 + 5}}, 1<<63 + 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := maxSeq(tc.lots); got != tc.want {
				t.Errorf("maxSeq() = %d, want %d", got, tc.want)
			}
		})
	}
}

func mustProcess(t *testing.T, svc *ValuationService, m InventoryMovementReceived) {
	t.Helper()
	if err := svc.Process(context.Background(), m); err != nil {
		t.Fatalf("Process %s: %v", m.EventID, err)
	}
}
