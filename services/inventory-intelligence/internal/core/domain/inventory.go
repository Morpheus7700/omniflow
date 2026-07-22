package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type MovementType int

const (
	MovementTypeUnspecified MovementType = 0
	MovementTypeReceipt     MovementType = 1
	MovementTypeConsumption MovementType = 2
	MovementTypeAdjustment  MovementType = 3
)

// InventoryMovementReceived is the core domain event received from Kafka
type InventoryMovementReceived struct {
	EventID           string
	TraceParent       string
	AggregateID       string
	OccurredAt        time.Time
	PublishedAt       time.Time
	CDCEmitTs         time.Time
	SequenceEngineKey uint64
	MovementType      MovementType
	SKU               string
	MDMVendorID       string
	LocationID        string
	Quantity          decimal.Decimal
	UnitCost          decimal.Decimal
}

// InventoryFact represents a precomputed movement and snapshot intended for the outbox
type InventoryFact struct {
	EventID           string
	TraceParent       string
	OccurredAt        time.Time
	CDCEmitTs         time.Time
	SequenceEngineKey uint64

	SKU         string
	MDMVendorID string
	LocationID  string

	// Deltas
	MovementType MovementType
	QtyDelta     decimal.Decimal

	// Running metrics (snapshot values as of this movement)
	QtyOnHand      decimal.Decimal
	FifoUnitCost   decimal.Decimal
	FifoTotalValue decimal.Decimal
	MovingAvgCost  decimal.Decimal
}

// FIFOLot represents a specific cost bucket for FIFO calculation
type FIFOLot struct {
	LotID             string
	SKU               string
	LocationID        string
	SequenceEngineKey uint64
	RemainingQty      decimal.Decimal
	UnitCost          decimal.Decimal
}
