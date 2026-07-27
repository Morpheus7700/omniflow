package domain

import (
	"context"
	"errors"

	"github.com/shopspring/decimal"
)

var (
	ErrTransient = errors.New("transient error")
	ErrTerminal  = errors.New("terminal error")
)

type EventConsumer interface {
	Start(ctx context.Context)
}

// InventoryStore manages the strict CRDB transaction (operational state + outbox)
type InventoryStore interface {
	// ProcessInventoryTransaction executes the idempotency check, updates the append-only ledger,
	// restates any affected history, and writes the fact rows that the inventory changefeed carries
	// to the viz gateway.
	// Bounded lateness handles historical recalculations within this single transaction.
	ProcessInventoryTransaction(ctx context.Context, event InventoryMovementReceived, processFn func(ctx context.Context, e InventoryMovementReceived, ledger LedgerAccess) ([]InventoryFact, error)) error
}

// LedgerAccess provides transaction-scoped access to operational inventory tables
type LedgerAccess interface {
	IsAlreadyProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error

	// Gets all lots for a SKU ordered strictly by SequenceEngineKey
	GetLots(ctx context.Context, sku string, locationID string) ([]FIFOLot, error)
	SaveLot(ctx context.Context, lot FIFOLot) error
	UpdateLotQuantity(ctx context.Context, lotID string, remaining decimal.Decimal) error

	// Get latest snapshot before a given sequence key for restatement
	GetSnapshotBefore(ctx context.Context, sku string, locationID string, seqKey uint64) (InventoryFact, error)

	// Append-only movement log for HLC restatement replay
	SaveMovementLog(ctx context.Context, e InventoryMovementReceived) error
	GetMovementsFrom(ctx context.Context, sku string, locationID string, seqKey uint64) ([]InventoryMovementReceived, error)
	ResetLots(ctx context.Context, sku string, locationID string) error
}
