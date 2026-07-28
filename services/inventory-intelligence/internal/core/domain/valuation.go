package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// ValuationService implements the core inventory logic
type ValuationService struct {
	store InventoryStore
}

func NewValuationService(store InventoryStore) *ValuationService {
	return &ValuationService{store: store}
}

// Process handles a single incoming movement, enforcing HLC chronological ordering and idempotency
func (s *ValuationService) Process(ctx context.Context, event InventoryMovementReceived) error {
	return s.store.ProcessInventoryTransaction(ctx, event, s.calculateFacts)
}

func (s *ValuationService) calculateFacts(ctx context.Context, e InventoryMovementReceived, l LedgerAccess) ([]InventoryFact, error) {
	// 1. Save the raw event to the append-only movement log for replay
	if err := l.SaveMovementLog(ctx, e); err != nil {
		return nil, err
	}

	lots, err := l.GetLots(ctx, e.SKU, e.LocationID) // persisted cost layers, HLC-ordered
	if err != nil {
		return nil, err
	}

	// Late-arrival: if this predates the newest processed event, restate — don't append.
	if len(lots) > 0 && e.SequenceEngineKey < maxSeq(lots) {
		if s.beyondLatenessHorizon(e) {
			e.MovementType = MovementTypeAdjustment
			return s.applyMovement(ctx, e, l, lots) // NOT calculateFacts -> terminates
		}
		return s.restateSKUFrom(ctx, e, l) // recompute forward
	}

	return s.applyMovement(ctx, e, l, lots)
}

func depleteLots(ctx context.Context, l LedgerAccess, lots []FIFOLot, qty decimal.Decimal, sku string, fact *InventoryFact) error {
	remaining, cogs := qty, decimal.Zero
	for i := range lots { // oldest first = true FIFO
		if !remaining.IsPositive() {
			break
		}
		take := decimal.Min(remaining, lots[i].RemainingQty)
		cogs = cogs.Add(take.Mul(lots[i].UnitCost))
		if err := l.UpdateLotQuantity(ctx, lots[i].LotID, lots[i].RemainingQty.Sub(take)); err != nil {
			return err
		}
		remaining = remaining.Sub(take)
	}
	if remaining.IsPositive() {
		return fmt.Errorf("%w: consumption/adjustment exceeds on-hand %s", ErrTerminal, sku)
	}
	fact.FifoUnitCost = cogs.Div(qty)
	return nil
}

func (s *ValuationService) applyMovement(ctx context.Context, e InventoryMovementReceived, l LedgerAccess, lots []FIFOLot) ([]InventoryFact, error) {
	fact := baseFact(e)
	switch e.MovementType {
	case MovementTypeReceipt:
		if err := l.SaveLot(ctx, FIFOLot{LotID: e.EventID, SKU: e.SKU, LocationID: e.LocationID,
			SequenceEngineKey: e.SequenceEngineKey, RemainingQty: e.Quantity, UnitCost: e.UnitCost}); err != nil {
			return nil, err
		}
	case MovementTypeConsumption:
		if err := depleteLots(ctx, l, lots, e.Quantity, e.SKU, &fact); err != nil {
			return nil, err
		}
		fact.QtyDelta = e.Quantity.Neg()
	case MovementTypeAdjustment:
		if e.Quantity.IsPositive() {
			if err := l.SaveLot(ctx, FIFOLot{LotID: e.EventID, SKU: e.SKU, LocationID: e.LocationID,
				SequenceEngineKey: e.SequenceEngineKey, RemainingQty: e.Quantity, UnitCost: e.UnitCost}); err != nil {
				return nil, err
			}
		} else if e.Quantity.IsNegative() {
			if err := depleteLots(ctx, l, lots, e.Quantity.Abs(), e.SKU, &fact); err != nil {
				return nil, err
			}
			fact.QtyDelta = e.Quantity // already negative
		}
	}

	// Snapshot recomputed from surviving lots (the source of truth), not a running guess.
	onHand, value := decimal.Zero, decimal.Zero
	survivors, _ := l.GetLots(ctx, e.SKU, e.LocationID)
	for _, lt := range survivors {
		onHand = onHand.Add(lt.RemainingQty)
		value = value.Add(lt.RemainingQty.Mul(lt.UnitCost))
	}
	fact.QtyOnHand, fact.FifoTotalValue = onHand, value
	fact.MovingAvgCost = decimal.Zero
	if onHand.IsPositive() {
		fact.MovingAvgCost = value.Div(onHand)
	}
	return []InventoryFact{fact}, nil // + emit the SKU×day snapshot fact row
}

func baseFact(e InventoryMovementReceived) InventoryFact {
	return InventoryFact{
		EventID:           e.EventID,
		TraceParent:       e.TraceParent,
		OccurredAt:        e.OccurredAt,
		CDCEmitTs:         e.CDCEmitTs,
		SequenceEngineKey: e.SequenceEngineKey,
		SKU:               e.SKU,
		MDMVendorID:       e.MDMVendorID,
		LocationID:        e.LocationID,
		MovementType:      e.MovementType,
		QtyDelta:          e.Quantity,
	}
}

func maxSeq(lots []FIFOLot) uint64 {
	var max uint64
	for _, lot := range lots {
		if lot.SequenceEngineKey > max {
			max = lot.SequenceEngineKey
		}
	}
	return max
}

func (s *ValuationService) beyondLatenessHorizon(e InventoryMovementReceived) bool {
	// Assume an arbitrary 30-day bounded lateness horizon.
	return time.Since(e.OccurredAt) > 30*24*time.Hour
}

func (s *ValuationService) restateSKUFrom(ctx context.Context, e InventoryMovementReceived, l LedgerAccess) ([]InventoryFact, error) {
	if err := l.ResetLots(ctx, e.SKU, e.LocationID); err != nil { // rewind
		return nil, err
	}

	all, err := l.GetMovementsFrom(ctx, e.SKU, e.LocationID, 0) // full log, HLC order (incl. e)
	if err != nil {
		return nil, err
	}

	var facts []InventoryFact
	for _, m := range all { // deterministic replay from zero
		lots, _ := l.GetLots(ctx, m.SKU, m.LocationID)
		f, err := s.applyMovement(ctx, m, l, lots)
		if err != nil {
			return nil, err
		}
		facts = append(facts, f...)
	}
	return facts, nil
}
