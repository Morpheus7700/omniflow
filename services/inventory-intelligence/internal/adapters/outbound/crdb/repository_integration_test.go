//go:build integration

// Integration tests for the CockroachDB repository adapter. Run with:
//
//	go test -tags=integration ./...
//
// They are tag-gated so the default `go test ./...` stays fast and Docker-free. What is verified
// here CANNOT be verified with fakes, because the guarantee is the SQL and the transaction itself:
// single-transaction atomicity, the exactly-once idempotency guard, lot ordering, and DECIMAL
// fidelity across the wire. The domain-level FIFO arithmetic is covered by valuation_test.go.
package crdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"omniflow/services/inventory-intelligence/internal/core/domain"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Pinned to the exact image and start command the compose stack runs, so these tests exercise the
// dialect and single-node topology we actually deploy against.
//
// The container is driven directly rather than through testcontainers' cockroachdb helper module:
// that module depends on pgx and would drag the production pgx version forward as a side effect of
// adding tests. Keeping the data-path dependency untouched is worth twenty lines of setup.
const crdbImage = "cockroachdb/cockroach:v24.3.3"

// ─── Harness ─────────────────────────────────────────────────────────────────────────────────────

// repoRoot walks up from the test's working directory to the directory holding go.mod, so the real
// shipped schema file can be applied rather than a hand-copied duplicate that could drift.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repo root (no go.mod found walking up)")
		}
		dir = parent
	}
}

// newTestDB boots a single-node CockroachDB, applies infrastructure/crdb_schema.sql, and returns a
// pool. The container is shared per-test for isolation; each test gets a clean database.
func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, crdbImage,
		testcontainers.WithCmd("start-single-node", "--insecure", "--store=type=mem,size=100%"),
		testcontainers.WithExposedPorts("26257/tcp", "8080/tcp"),
		// /health returns 200 only once the node has finished initialising and can serve SQL.
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/health").WithPort("8080/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("start cockroachdb container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "26257/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}

	adminDSN := fmt.Sprintf("postgres://root@%s:%s/defaultdb?sslmode=disable", host, port.Port())
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS omniflow"); err != nil {
		admin.Close()
		t.Fatalf("create database: %v", err)
	}
	admin.Close()

	dsn := fmt.Sprintf("postgres://root@%s:%s/omniflow?sslmode=disable", host, port.Port())
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	t.Cleanup(pool.Close)

	schema, err := os.ReadFile(filepath.Join(repoRoot(t), "infrastructure", "crdb_schema.sql"))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return pool
}

func dec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func movement(eventID string, seq uint64, mt domain.MovementType, qty, cost string) domain.InventoryMovementReceived {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return domain.InventoryMovementReceived{
		EventID:           eventID,
		TraceParent:       "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		AggregateID:       "AGG-1",
		OccurredAt:        now,
		PublishedAt:       now,
		CDCEmitTs:         now,
		SequenceEngineKey: seq,
		MovementType:      mt,
		SKU:               "SKU-1",
		MDMVendorID:       "VENDOR-1",
		LocationID:        "LOC-1",
		Quantity:          dec(qty),
		UnitCost:          dec(cost),
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// scanDecimal reads a DECIMAL column back as a decimal.Decimal for numeric comparison. Never
// compare these as strings — 2, 2.0 and 2.00 are all equal numerically but differ textually.
func scanDecimal(t *testing.T, pool *pgxpool.Pool, query string, args ...any) decimal.Decimal {
	t.Helper()
	var d decimal.Decimal
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&d); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return d
}

// ─── Exactly-once ────────────────────────────────────────────────────────────────────────────────

// At-least-once delivery means redelivery is normal, not exceptional. The idempotency guard must
// make the second attempt a silent no-op — not an error, and not a duplicate ledger row.
func TestProcessInventoryTransactionIsExactlyOnce(t *testing.T) {
	pool := newTestDB(t)
	repo := NewRepository(pool)
	svc := domain.NewValuationService(repo)
	ctx := context.Background()

	evt := movement("evt-dup", 100, domain.MovementTypeReceipt, "10", "2")

	if err := svc.Process(ctx, evt); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	// Redelivery of the identical event.
	if err := svc.Process(ctx, evt); err != nil {
		t.Fatalf("redelivery must be a silent no-op, got: %v", err)
	}

	if got := countRows(t, pool, "inventory_lots"); got != 1 {
		t.Errorf("inventory_lots = %d rows, want 1 — redelivery duplicated the ledger", got)
	}
	if got := countRows(t, pool, "inventory_movements_log"); got != 1 {
		t.Errorf("inventory_movements_log = %d rows, want 1", got)
	}
	if got := countRows(t, pool, "fact_inventory_movement"); got != 1 {
		t.Errorf("fact_inventory_movement = %d rows, want 1", got)
	}
	if got := countRows(t, pool, "processed_events"); got != 1 {
		t.Errorf("processed_events = %d rows, want 1", got)
	}

	// On-hand must still reflect a single receipt.
	onHand := scanDecimal(t, pool, "SELECT remaining_qty FROM inventory_lots WHERE lot_id = $1", "evt-dup")
	if !onHand.Equal(dec("10")) {
		t.Errorf("remaining_qty = %s, want 10", onHand)
	}
}

// ─── Atomicity ───────────────────────────────────────────────────────────────────────────────────

// The movement log write, the lot mutations and the outbox rows must commit or roll back together.
// A partial commit would leave the ledger and the projection permanently disagreeing.
func TestProcessInventoryTransactionRollsBackEntirely(t *testing.T) {
	pool := newTestDB(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	sentinel := errors.New("boom")

	// processFn writes to the movement log and the lots table, then fails. Nothing may survive.
	err := repo.ProcessInventoryTransaction(ctx, movement("evt-fail", 100, domain.MovementTypeReceipt, "5", "1"),
		func(ctx context.Context, e domain.InventoryMovementReceived, l domain.LedgerAccess) ([]domain.InventoryFact, error) {
			if err := l.SaveMovementLog(ctx, e); err != nil {
				return nil, err
			}
			if err := l.SaveLot(ctx, domain.FIFOLot{
				LotID: e.EventID, SKU: e.SKU, LocationID: e.LocationID,
				SequenceEngineKey: e.SequenceEngineKey, RemainingQty: e.Quantity, UnitCost: e.UnitCost,
			}); err != nil {
				return nil, err
			}
			return nil, sentinel
		})

	if err == nil {
		t.Fatal("expected the processFn error to propagate")
	}

	for _, table := range []string{"inventory_lots", "inventory_movements_log", "fact_inventory_movement", "fact_inventory_snapshot"} {
		if got := countRows(t, pool, table); got != 0 {
			t.Errorf("%s = %d rows after rollback, want 0", table, got)
		}
	}
	// Crucially the idempotency marker must roll back too — otherwise a failed event could never
	// be retried, silently vanishing.
	if got := countRows(t, pool, "processed_events"); got != 0 {
		t.Errorf("processed_events = %d rows after rollback, want 0 — the event could never be retried", got)
	}
}

// A terminal domain error must surface unwrapped so the consumer routes it straight to the DLQ.
func TestTerminalDomainErrorPropagatesAndRollsBack(t *testing.T) {
	pool := newTestDB(t)
	repo := NewRepository(pool)
	svc := domain.NewValuationService(repo)
	ctx := context.Background()

	if err := svc.Process(ctx, movement("evt-1", 100, domain.MovementTypeReceipt, "5", "2")); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	err := svc.Process(ctx, movement("evt-2", 200, domain.MovementTypeConsumption, "99", "0"))
	if !errors.Is(err, domain.ErrTerminal) {
		t.Fatalf("error = %v, want domain.ErrTerminal", err)
	}

	// The over-consumption attempt must not have disturbed the surviving lot.
	remaining := scanDecimal(t, pool, "SELECT remaining_qty FROM inventory_lots WHERE lot_id = $1", "evt-1")
	if !remaining.Equal(dec("5")) {
		t.Errorf("remaining_qty = %s, want 5 — the failed transaction leaked", remaining)
	}
	if got := countRows(t, pool, "processed_events"); got != 1 {
		t.Errorf("processed_events = %d, want 1 (only the successful receipt)", got)
	}
}

// ─── Ledger reads ────────────────────────────────────────────────────────────────────────────────

// GetLots must order by sequence_engine_key (the FIFO guarantee) and exclude exhausted lots.
func TestGetLotsOrdersBySequenceKeyAndSkipsExhausted(t *testing.T) {
	pool := newTestDB(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	// Insert out of sequence order on purpose, including one already-exhausted lot.
	seed := []struct {
		id   string
		seq  uint64
		qty  string
		cost string
	}{
		{"lot-c", 300, "5", "7"},
		{"lot-a", 100, "10", "2"},
		{"lot-exhausted", 200, "0", "9"},
		{"lot-b", 250, "3", "5"},
	}
	for _, s := range seed {
		if _, err := pool.Exec(ctx,
			`INSERT INTO inventory_lots (lot_id, sku, location_id, sequence_engine_key, remaining_qty, unit_cost)
			 VALUES ($1, 'SKU-1', 'LOC-1', $2, $3, $4)`, s.id, s.seq, dec(s.qty), dec(s.cost)); err != nil {
			t.Fatalf("seed %s: %v", s.id, err)
		}
	}

	var got []domain.FIFOLot
	err := repo.ProcessInventoryTransaction(ctx, movement("evt-probe", 999, domain.MovementTypeReceipt, "1", "1"),
		func(ctx context.Context, e domain.InventoryMovementReceived, l domain.LedgerAccess) ([]domain.InventoryFact, error) {
			var err error
			got, err = l.GetLots(ctx, "SKU-1", "LOC-1")
			return nil, err
		})
	if err != nil {
		t.Fatalf("ProcessInventoryTransaction: %v", err)
	}

	wantIDs := []string{"lot-a", "lot-b", "lot-c"} // seq 100, 250, 300; the 0-qty lot is excluded
	if len(got) != len(wantIDs) {
		t.Fatalf("GetLots returned %d lots, want %d (%v)", len(got), len(wantIDs), wantIDs)
	}
	for i, want := range wantIDs {
		if got[i].LotID != want {
			t.Errorf("lot[%d] = %s, want %s (ordering must follow sequence_engine_key)", i, got[i].LotID, want)
		}
	}
	// A DECIMAL that round-trips through CRDB must keep its exact value.
	if !got[0].UnitCost.Equal(dec("2")) {
		t.Errorf("unit_cost = %s, want 2", got[0].UnitCost)
	}
}

func TestGetSnapshotBefore(t *testing.T) {
	pool := newTestDB(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	for _, s := range []struct {
		id  string
		seq uint64
		qty string
	}{
		{"snap-1", 100, "10"},
		{"snap-2", 200, "25"},
		{"snap-3", 300, "40"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO fact_inventory_snapshot
			 (snapshot_id, snapshot_date, sku, mdm_vendor_id, location_id, qty_on_hand, fifo_total_value, moving_avg_cost, last_updated_at, sequence_engine_key)
			 VALUES ($1, '2026-07-24', 'SKU-1', 'VENDOR-1', 'LOC-1', $2, 0, 0, now(), $3)`,
			s.id, dec(s.qty), s.seq); err != nil {
			t.Fatalf("seed %s: %v", s.id, err)
		}
	}

	run := func(seqKey uint64) domain.InventoryFact {
		t.Helper()
		var fact domain.InventoryFact
		err := repo.ProcessInventoryTransaction(ctx, movement("evt-"+decimal.NewFromInt(int64(seqKey)).String(), seqKey, domain.MovementTypeReceipt, "1", "1"),
			func(ctx context.Context, e domain.InventoryMovementReceived, l domain.LedgerAccess) ([]domain.InventoryFact, error) {
				var err error
				fact, err = l.GetSnapshotBefore(ctx, "SKU-1", "LOC-1", seqKey)
				return nil, err
			})
		if err != nil {
			t.Fatalf("ProcessInventoryTransaction: %v", err)
		}
		return fact
	}

	// Strictly before: seq 300 must see snap-2, not snap-3.
	if got := run(300); !got.QtyOnHand.Equal(dec("25")) {
		t.Errorf("GetSnapshotBefore(300).QtyOnHand = %s, want 25 (must be strictly before)", got.QtyOnHand)
	}
	if got := run(1000); !got.QtyOnHand.Equal(dec("40")) {
		t.Errorf("GetSnapshotBefore(1000).QtyOnHand = %s, want 40 (latest)", got.QtyOnHand)
	}
	// No prior snapshot is not an error — it yields an empty fact for the SKU.
	got := run(50)
	if !got.QtyOnHand.Equal(decimal.Zero) {
		t.Errorf("GetSnapshotBefore(50).QtyOnHand = %s, want 0", got.QtyOnHand)
	}
	if got.SKU != "SKU-1" || got.LocationID != "LOC-1" {
		t.Errorf("empty snapshot = %+v, want it to carry SKU/location", got)
	}
}

// ─── End-to-end FIFO against real SQL ────────────────────────────────────────────────────────────

// The same scenario scripts/failtest_fifo_restatement.sh proves against the full compose stack,
// executed here in seconds against real CockroachDB:
//
//	B(seq 200) receipt 10 @ 5  ->  C(seq 300) consume 5 @ 5.00  ->  late A(seq 100) receipt 10 @ 2
//	                                        => C restates to 2.00, snapshot value 60.00
//
// valuation_test.go pins the arithmetic; this pins that the arithmetic survives a real round trip
// through DECIMAL columns, INT8 HLC keys and the movement-log replay query.
func TestLateArrivalRestatementAgainstRealDatabase(t *testing.T) {
	pool := newTestDB(t)
	svc := domain.NewValuationService(NewRepository(pool))
	ctx := context.Background()

	mustProcess := func(m domain.InventoryMovementReceived) {
		t.Helper()
		if err := svc.Process(ctx, m); err != nil {
			t.Fatalf("Process %s: %v", m.EventID, err)
		}
	}

	mustProcess(movement("evt-B", 200, domain.MovementTypeReceipt, "10", "5"))
	mustProcess(movement("evt-C", 300, domain.MovementTypeConsumption, "5", "0"))

	before := scanDecimal(t, pool, "SELECT fifo_unit_cost FROM fact_inventory_movement WHERE event_id = $1", "evt-C")
	if !before.Equal(dec("5.00")) {
		t.Fatalf("pre-restatement fifo_unit_cost = %s, want 5.00", before)
	}

	// The late arrival predates both, forcing a restatement.
	mustProcess(movement("evt-A", 100, domain.MovementTypeReceipt, "10", "2"))

	after := scanDecimal(t, pool, "SELECT fifo_unit_cost FROM fact_inventory_movement WHERE event_id = $1", "evt-C")
	if !after.Equal(dec("2.00")) {
		t.Errorf("restated fifo_unit_cost = %s, want 2.00 — the late arrival did not restate history", after)
	}

	// Snapshot settles at (10-5)@2 + 10@5 = 60.00 over 15 units.
	value := scanDecimal(t, pool,
		"SELECT fifo_total_value FROM fact_inventory_snapshot WHERE sku = $1 AND location_id = $2", "SKU-1", "LOC-1")
	if !value.Equal(dec("60.00")) {
		t.Errorf("snapshot fifo_total_value = %s, want 60.00", value)
	}
	qty := scanDecimal(t, pool,
		"SELECT qty_on_hand FROM fact_inventory_snapshot WHERE sku = $1 AND location_id = $2", "SKU-1", "LOC-1")
	if !qty.Equal(dec("15")) {
		t.Errorf("snapshot qty_on_hand = %s, want 15", qty)
	}
}

// HLC sequence keys are 19-digit values. They travel as uint64 in Go and INT8 in CockroachDB, and
// must survive the round trip exactly — a silent truncation would corrupt both late-arrival
// detection and the ordering the WebGL timeline renders on.
func TestSequenceEngineKeyRoundTripsExactly(t *testing.T) {
	pool := newTestDB(t)
	svc := domain.NewValuationService(NewRepository(pool))
	ctx := context.Background()

	const hlcKey uint64 = 1784933243325766766

	if err := svc.Process(ctx, movement("evt-hlc", hlcKey, domain.MovementTypeReceipt, "1", "1")); err != nil {
		t.Fatalf("Process: %v", err)
	}

	var got uint64
	if err := pool.QueryRow(ctx,
		"SELECT sequence_engine_key FROM fact_inventory_movement WHERE event_id = $1", "evt-hlc").Scan(&got); err != nil {
		t.Fatalf("read back key: %v", err)
	}
	if got != hlcKey {
		t.Errorf("sequence_engine_key = %d, want %d", got, hlcKey)
	}
}
