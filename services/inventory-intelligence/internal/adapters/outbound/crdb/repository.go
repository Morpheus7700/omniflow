package crdb

import (
	"context"
	"errors"
	"fmt"
	"net"

	"omniflow/services/inventory-intelligence/internal/core/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func wrapError(err error) error {
	if err == nil {
		return nil
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return fmt.Errorf("%w: %v", domain.ErrTransient, err)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 40001 = serialization_failure
		// 08* = connection exceptions
		if pgErr.Code == "40001" || pgErr.Code == "08000" || pgErr.Code == "08003" || pgErr.Code == "08006" {
			return fmt.Errorf("%w: %v", domain.ErrTransient, err)
		}
	}

	return err
}

func (r *Repository) ProcessInventoryTransaction(
	ctx context.Context,
	event domain.InventoryMovementReceived,
	processFn func(ctx context.Context, e domain.InventoryMovementReceived, ledger domain.LedgerAccess) ([]domain.InventoryFact, error),
) error {
	// Single CRDB Transaction
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return wrapError(err)
	}
	defer tx.Rollback(ctx)

	ledger := &TxLedger{tx: tx}

	// 1. Check idempotency
	processed, err := ledger.IsAlreadyProcessed(ctx, event.EventID)
	if err != nil {
		return wrapError(err)
	}
	if processed {
		// Silent deduplication for idempotency
		return nil
	}

	// 2. Mark processed
	if err := ledger.MarkProcessed(ctx, event.EventID); err != nil {
		return wrapError(err)
	}

	// 3. Process the domain logic (bounded lateness / FIFO / moving avg recalculation)
	facts, err := processFn(ctx, event, ledger)
	if err != nil {
		if errors.Is(err, domain.ErrTerminal) {
			return err // Return as-is if domain marked it terminal
		}
		return wrapError(err)
	}

	// 4. Write to outbox (fact_inventory_movement, fact_inventory_snapshot)
	for _, fact := range facts {
		_, err := tx.Exec(ctx,
			`INSERT INTO fact_inventory_movement 
			(event_id, trace_parent, occurred_at, cdc_emit_ts, sequence_engine_key, sku, mdm_vendor_id, location_id, movement_type, qty_delta, fifo_unit_cost, fifo_total_value, moving_avg_cost) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (event_id) DO UPDATE SET
				trace_parent = excluded.trace_parent,
				occurred_at = excluded.occurred_at,
				cdc_emit_ts = excluded.cdc_emit_ts,
				sequence_engine_key = excluded.sequence_engine_key,
				sku = excluded.sku,
				mdm_vendor_id = excluded.mdm_vendor_id,
				location_id = excluded.location_id,
				movement_type = excluded.movement_type,
				qty_delta = excluded.qty_delta,
				fifo_unit_cost = excluded.fifo_unit_cost,
				fifo_total_value = excluded.fifo_total_value,
				moving_avg_cost = excluded.moving_avg_cost`,
			fact.EventID, fact.TraceParent, fact.OccurredAt, fact.CDCEmitTs, fact.SequenceEngineKey, fact.SKU, fact.MDMVendorID, fact.LocationID, int64(fact.MovementType), fact.QtyDelta, fact.FifoUnitCost, fact.FifoTotalValue, fact.MovingAvgCost,
		)
		if err != nil {
			return wrapError(err)
		}

		snapshotDate := fact.OccurredAt.Format("2006-01-02")
		snapshotID := fact.SKU + "_" + snapshotDate
		_, err = tx.Exec(ctx,
			`UPSERT INTO fact_inventory_snapshot 
			(snapshot_id, snapshot_date, sku, mdm_vendor_id, location_id, qty_on_hand, fifo_total_value, moving_avg_cost, last_updated_at, sequence_engine_key) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			snapshotID, snapshotDate, fact.SKU, fact.MDMVendorID, fact.LocationID, fact.QtyOnHand, fact.FifoTotalValue, fact.MovingAvgCost, fact.OccurredAt, fact.SequenceEngineKey,
		)
		if err != nil {
			return wrapError(err)
		}
	}

	return wrapError(tx.Commit(ctx))
}

// TxLedger implements domain.LedgerAccess backed by an active pgx.Tx
type TxLedger struct {
	tx pgx.Tx
}

func (l *TxLedger) IsAlreadyProcessed(ctx context.Context, eventID string) (bool, error) {
	var processed bool
	err := l.tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)", eventID).Scan(&processed)
	return processed, err
}

func (l *TxLedger) MarkProcessed(ctx context.Context, eventID string) error {
	_, err := l.tx.Exec(ctx, "INSERT INTO processed_events (event_id, processed_at) VALUES ($1, NOW()) ON CONFLICT DO NOTHING", eventID)
	return err
}

func (l *TxLedger) GetLots(ctx context.Context, sku string, locationID string) ([]domain.FIFOLot, error) {
	rows, err := l.tx.Query(ctx, `
		SELECT lot_id, sku, location_id, sequence_engine_key, remaining_qty, unit_cost
		FROM inventory_lots
		WHERE sku = $1 AND location_id = $2 AND remaining_qty > 0
		ORDER BY sequence_engine_key ASC`, sku, locationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lots []domain.FIFOLot
	for rows.Next() {
		var lot domain.FIFOLot
		if err := rows.Scan(&lot.LotID, &lot.SKU, &lot.LocationID, &lot.SequenceEngineKey, &lot.RemainingQty, &lot.UnitCost); err != nil {
			return nil, err
		}
		lots = append(lots, lot)
	}
	return lots, rows.Err()
}

func (l *TxLedger) SaveLot(ctx context.Context, lot domain.FIFOLot) error {
	_, err := l.tx.Exec(ctx, `
		INSERT INTO inventory_lots (lot_id, sku, location_id, sequence_engine_key, remaining_qty, unit_cost)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		lot.LotID, lot.SKU, lot.LocationID, lot.SequenceEngineKey, lot.RemainingQty, lot.UnitCost)
	return err
}

func (l *TxLedger) UpdateLotQuantity(ctx context.Context, lotID string, remaining decimal.Decimal) error {
	_, err := l.tx.Exec(ctx, "UPDATE inventory_lots SET remaining_qty = $1 WHERE lot_id = $2", remaining, lotID)
	return err
}

func (l *TxLedger) GetSnapshotBefore(ctx context.Context, sku string, locationID string, seqKey uint64) (domain.InventoryFact, error) {
	var fact domain.InventoryFact
	err := l.tx.QueryRow(ctx, `
		SELECT sku, location_id, sequence_engine_key, qty_on_hand, fifo_total_value, moving_avg_cost
		FROM fact_inventory_snapshot
		WHERE sku = $1 AND location_id = $2 AND sequence_engine_key < $3
		ORDER BY sequence_engine_key DESC LIMIT 1`, sku, locationID, seqKey).Scan(
		&fact.SKU, &fact.LocationID, &fact.SequenceEngineKey, &fact.QtyOnHand, &fact.FifoTotalValue, &fact.MovingAvgCost,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.InventoryFact{SKU: sku, LocationID: locationID}, nil
	}
	return fact, err
}

func (l *TxLedger) SaveMovementLog(ctx context.Context, e domain.InventoryMovementReceived) error {
	_, err := l.tx.Exec(ctx, `
		INSERT INTO inventory_movements_log 
		(event_id, trace_parent, aggregate_id, occurred_at, published_at, cdc_emit_ts, sequence_engine_key, movement_type, sku, mdm_vendor_id, location_id, quantity, unit_cost)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		e.EventID, e.TraceParent, e.AggregateID, e.OccurredAt, e.PublishedAt, e.CDCEmitTs, e.SequenceEngineKey, int64(e.MovementType), e.SKU, e.MDMVendorID, e.LocationID, e.Quantity, e.UnitCost)
	return err
}

func (l *TxLedger) ResetLots(ctx context.Context, sku string, locationID string) error {
	_, err := l.tx.Exec(ctx, "DELETE FROM inventory_lots WHERE sku = $1 AND location_id = $2", sku, locationID)
	return err
}

func (l *TxLedger) GetMovementsFrom(ctx context.Context, sku string, locationID string, seqKey uint64) ([]domain.InventoryMovementReceived, error) {
	rows, err := l.tx.Query(ctx, `
		SELECT event_id, trace_parent, aggregate_id, occurred_at, published_at, cdc_emit_ts, sequence_engine_key, movement_type, sku, mdm_vendor_id, location_id, quantity, unit_cost
		FROM inventory_movements_log
		WHERE sku = $1 AND location_id = $2 AND sequence_engine_key >= $3
		ORDER BY sequence_engine_key ASC`, sku, locationID, seqKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mvmts []domain.InventoryMovementReceived
	for rows.Next() {
		var e domain.InventoryMovementReceived
		var mType int64
		if err := rows.Scan(
			&e.EventID, &e.TraceParent, &e.AggregateID, &e.OccurredAt, &e.PublishedAt, &e.CDCEmitTs, &e.SequenceEngineKey, &mType, &e.SKU, &e.MDMVendorID, &e.LocationID, &e.Quantity, &e.UnitCost,
		); err != nil {
			return nil, err
		}
		e.MovementType = domain.MovementType(mType)
		mvmts = append(mvmts, e)
	}
	return mvmts, rows.Err()
}
