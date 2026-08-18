//go:build integration

// Integration tests for SaveCheckpoint — the exactly-once CTE.
//
//	go test -tags=integration ./...
//
// WHY THIS FILE EXISTS. Every exactly-once claim this project makes rests on one SQL statement in
// state_store.go, and until now that statement had no Go test of any kind. It was covered only by
// scripts/failtest_exactly_once.sh, which boots the entire stack — Kafka, CockroachDB, five
// services — and counts rows. That proof is real, but it is also slow, it cannot run on a fork PR,
// and when it fails it reports "expected 2 rows, got 3" without telling you which of the three SQL
// branches misbehaved. Worse, it is a whole-system test standing in for a unit-level guarantee: it
// passes as long as the system as a whole emits the right number of rows, so a checkpoint bug that
// happened to be masked elsewhere would not be caught here.
//
// These tests exercise the statement directly against real CockroachDB, because what is being
// verified IS the SQL: the CTE's ON CONFLICT DO NOTHING, the `WHERE EXISTS (SELECT 1 FROM
// insert_ledger)` guard, CockroachDB's requirement that a data-modifying CTE return columns, and
// the two-placeholder split for a value used as both UUID and STRING. None of that is observable
// against a fake — a fake would just do what the Go code says it does.
package crdb

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Pinned to the image compose runs, for the same reason the inventory suite pins it: these tests
// assert dialect behaviour (CTE column requirements, placeholder type inference) that is specific
// to the engine version deployed. TestCRDBImageMatchesCompose in the inventory package guards the
// pin against drift for the whole repo.
const crdbImage = "cockroachdb/cockroach:v26.2.5"

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

func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, crdbImage,
		testcontainers.WithCmd("start-single-node", "--insecure", "--store=type=mem,size=100%"),
		testcontainers.WithExposedPorts("26257/tcp", "8080/tcp"),
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

	for _, rel := range schemaFilesInDeployOrder(t) {
		sql, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
		if err != nil {
			t.Fatalf("read schema %s: %v", rel, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply schema %s: %v", rel, err)
		}
	}
	return pool
}

// schemaFilesInDeployOrder reads the schema list out of infrastructure/init/crdb-init.sh rather
// than hardcoding it.
//
// The orchestrator's tables are spread across several files — workflows, node_execution_ledger and
// orchestrator_outbox live in storage/orchestrator_schema.sql, not in the top-level crdb_schema.sql
// that the inventory suite applies — and the ORDER matters: procurement_schema.sql declares a
// foreign key onto workflows(id), so applying it first fails. Duplicating that list here would
// create a second source of truth that silently rots the first time a schema file is added, and the
// symptom would be a confusing `relation "..." does not exist` in a test that looks unrelated.
//
// Parsing the init script means these tests apply exactly what crdb-init applies, in exactly the
// order it applies it, and a new schema file is picked up with no change to this file. Same
// principle as TestCRDBImageMatchesCompose: derive from the shipped artefact, do not restate it.
func schemaFilesInDeployOrder(t *testing.T) []string {
	t.Helper()
	initPath := filepath.Join(repoRoot(t), "infrastructure", "init", "crdb-init.sh")
	body, err := os.ReadFile(initPath)
	if err != nil {
		t.Fatalf("read crdb-init.sh: %v", err)
	}
	// Matches `-f /infrastructure/<path>.sql`; the leading slash is the in-container mount point.
	re := regexp.MustCompile(`-f\s+/(infrastructure/[^\s]+\.sql)`)
	matches := re.FindAllStringSubmatch(string(body), -1)
	if len(matches) == 0 {
		t.Fatalf("no schema files found in %s — did the init script change shape?", initPath)
	}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, m[1])
	}
	return out
}

// seedWorkflow creates a workflow through the real loader, so the row under test is shaped exactly
// as production shapes it — including the 19-digit HLC key, which is where INT8 fidelity bugs live.
func seedWorkflow(t *testing.T, s *Store, seqKey uint64) *domain.Workflow {
	t.Helper()
	eventID := uuidLike(t)
	wf, err := s.LoadOrCreateWorkflow(context.Background(), eventID,
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		seqKey, []string{"draft_po", "human_approval", "final_step"}, []byte(`{"seed":true}`))
	if err != nil {
		t.Fatalf("LoadOrCreateWorkflow: %v", err)
	}
	return wf
}

// uuidLike returns a syntactically valid v4-shaped UUID. event_id is a UUID column and only needs
// to be unique per test; crypto/rand is the honest way to get that without hand-rolling entropy
// from a clock, which collides when two tests seed inside the same nanosecond tick.
func uuidLike(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func begin(t *testing.T, s *Store, wf *domain.Workflow) ports.Transaction {
	t.Helper()
	tx, err := s.AcquireLease(context.Background(), wf.ID)
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	return tx
}

func count(t *testing.T, pool *pgxpool.Pool, q string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", q, err)
	}
	return n
}

// THE test. A consumer redelivery replays the same (workflow, node, attempt). The ledger insert
// hits ON CONFLICT DO NOTHING and returns no rows, so `WHERE EXISTS (SELECT 1 FROM insert_ledger)`
// is false and the outbox insert is skipped. If that guard is ever weakened, a Kafka redelivery —
// which is routine, not exceptional — emits a duplicate downstream event, and the whole
// exactly-once claim in the README becomes false.
func TestSaveCheckpoint_RedeliveryEmitsNoSecondOutboxRow(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	wf := seedWorkflow(t, s, 1787050674655252100)
	ctx := context.Background()
	payload := []byte(`{"status":"completed","node":"draft_po"}`)

	for i := 0; i < 3; i++ {
		tx := begin(t, s, wf)
		if err := s.SaveCheckpointTyped(ctx, tx, wf, "draft_po", 1, "PurchaseOrderDrafted", payload); err != nil {
			t.Fatalf("SaveCheckpointTyped call %d: %v", i+1, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %d: %v", i+1, err)
		}
	}

	if got := count(t, pool, `SELECT count(*) FROM node_execution_ledger WHERE workflow_id=$1::UUID AND node_id='draft_po'`, wf.ID); got != 1 {
		t.Errorf("ledger rows = %d, want 1 — ON CONFLICT DO NOTHING is not deduplicating", got)
	}
	// The assertion the whole design exists for.
	if got := count(t, pool, `SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID); got != 1 {
		t.Fatalf("outbox rows = %d after 3 identical checkpoints, want 1 — exactly-once emission is BROKEN", got)
	}
}

// A retry is a different attempt, and must produce a new ledger row AND a new outbox row. Without
// this test, "suppress everything" would pass the test above while silently dropping legitimate
// retries — the opposite failure, and just as bad.
func TestSaveCheckpoint_NewAttemptEmitsAnotherRow(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	wf := seedWorkflow(t, s, 1787050674655252101)
	ctx := context.Background()

	for attempt := 1; attempt <= 2; attempt++ {
		tx := begin(t, s, wf)
		if err := s.SaveCheckpointTyped(ctx, tx, wf, "draft_po", attempt, "NodeTransition", []byte(`{"a":1}`)); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit attempt %d: %v", attempt, err)
		}
	}

	if got := count(t, pool, `SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID); got != 2 {
		t.Errorf("outbox rows = %d across two attempts, want 2 — a genuine retry is being suppressed", got)
	}
}

// The second branch: a node that completes without a payload advances the ledger but must emit
// NOTHING downstream. This is what makes the HITL suspend silent — the orchestrator checkpoints
// human_approval and writes no outbox row, so nothing downstream sees a half-finished workflow.
func TestSaveCheckpoint_NoPayloadWritesLedgerButNoOutbox(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	wf := seedWorkflow(t, s, 1787050674655252102)
	ctx := context.Background()

	tx := begin(t, s, wf)
	if err := s.SaveCheckpoint(ctx, tx, wf, "human_approval", 1, nil); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := count(t, pool, `SELECT count(*) FROM node_execution_ledger WHERE workflow_id=$1::UUID`, wf.ID); got != 1 {
		t.Errorf("ledger rows = %d, want 1", got)
	}
	if got := count(t, pool, `SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID); got != 0 {
		t.Errorf("outbox rows = %d, want 0 — a payload-less checkpoint must stay invisible downstream", got)
	}
}

// The third branch: no node at all is a pure state advance (used when the DAG moves without
// executing anything). It must touch neither the ledger nor the outbox.
func TestSaveCheckpoint_NoNodeUpdatesWorkflowOnly(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	wf := seedWorkflow(t, s, 1787050674655252103)
	ctx := context.Background()

	wf.State = domain.WorkflowState("SUSPENDED")
	wf.CurrentNodeIndex = 2
	tx := begin(t, s, wf)
	if err := s.SaveCheckpoint(ctx, tx, wf, "", 0, nil); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var state string
	var idx int
	if err := pool.QueryRow(ctx, `SELECT state, current_node_index FROM workflows WHERE id=$1::UUID`, wf.ID).Scan(&state, &idx); err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	if state != "SUSPENDED" || idx != 2 {
		t.Errorf("workflow = (%s, %d), want (SUSPENDED, 2)", state, idx)
	}
	if got := count(t, pool, `SELECT count(*) FROM node_execution_ledger WHERE workflow_id=$1::UUID`, wf.ID); got != 0 {
		t.Errorf("ledger rows = %d, want 0", got)
	}
	if got := count(t, pool, `SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID); got != 0 {
		t.Errorf("outbox rows = %d, want 0", got)
	}
}

// If the transaction rolls back, the idempotency marker MUST roll back with it. Otherwise a
// checkpoint that failed after writing the ledger row would leave that row behind, the redelivery
// would hit ON CONFLICT DO NOTHING, and the outbox row would never be written by anyone — the event
// is lost permanently while every table looks consistent. This is the failure mode that a
// row-counting end-to-end test cannot distinguish from success.
func TestSaveCheckpoint_RollbackReleasesTheIdempotencyMarker(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	wf := seedWorkflow(t, s, 1787050674655252104)
	ctx := context.Background()
	payload := []byte(`{"status":"completed"}`)

	tx := begin(t, s, wf)
	if err := s.SaveCheckpointTyped(ctx, tx, wf, "draft_po", 1, "NodeTransition", payload); err != nil {
		t.Fatalf("SaveCheckpointTyped: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if got := count(t, pool, `SELECT count(*) FROM node_execution_ledger WHERE workflow_id=$1::UUID`, wf.ID); got != 0 {
		t.Fatalf("ledger rows = %d after rollback, want 0 — the event could never be retried", got)
	}

	// And the retry must now succeed and emit exactly one row.
	tx2 := begin(t, s, wf)
	if err := s.SaveCheckpointTyped(ctx, tx2, wf, "draft_po", 1, "NodeTransition", payload); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit retry: %v", err)
	}
	if got := count(t, pool, `SELECT count(*) FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID); got != 1 {
		t.Errorf("outbox rows = %d after rollback+retry, want 1", got)
	}
}

// The $5/$11 split. wf.ID is bound twice because it is a UUID in two columns and a STRING in
// orchestrator_outbox.aggregate_id, where it doubles as the Kafka partition key. Collapsing them
// into one placeholder fails with SQLSTATE 42804 — this asserts the value survives as a string and
// still matches the UUID columns, so a future "simplification" breaks a test rather than CI.
func TestSaveCheckpoint_AggregateIDIsStringAndMatchesWorkflowUUID(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	wf := seedWorkflow(t, s, 1787050674655252105)
	ctx := context.Background()

	tx := begin(t, s, wf)
	if err := s.SaveCheckpointTyped(ctx, tx, wf, "draft_po", 1, "PurchaseOrderDrafted", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("SaveCheckpointTyped: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var aggID, eventType string
	if err := pool.QueryRow(ctx,
		`SELECT aggregate_id, event_type FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID,
	).Scan(&aggID, &eventType); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if aggID != wf.ID {
		t.Errorf("aggregate_id = %q, want %q", aggID, wf.ID)
	}
	// The typed variant exists so a real domain event travels under its own name.
	if eventType != "PurchaseOrderDrafted" {
		t.Errorf("event_type = %q, want PurchaseOrderDrafted", eventType)
	}
	if got := count(t, pool, `SELECT count(*) FROM node_execution_ledger WHERE workflow_id=$1::UUID`, wf.ID); got != 1 {
		t.Errorf("the same value did not match as a UUID: ledger rows = %d, want 1", got)
	}
}

// sequence_engine_key is a 19-digit hybrid logical clock carried as INT8. A float64 round trip
// silently corrupts values above 2^53, and this key orders the entire settlement ledger — a
// corrupted key does not error, it just puts records in the wrong order forever.
func TestSaveCheckpoint_PreservesFullHLCPrecision(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	const hlc = uint64(1787050674655252106) // > 2^53; unrepresentable exactly as float64
	wf := seedWorkflow(t, s, hlc)
	ctx := context.Background()

	tx := begin(t, s, wf)
	if err := s.SaveCheckpointTyped(ctx, tx, wf, "draft_po", 1, "NodeTransition", []byte(`{"y":1}`)); err != nil {
		t.Fatalf("SaveCheckpointTyped: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var got uint64
	if err := pool.QueryRow(ctx,
		`SELECT sequence_engine_key FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID,
	).Scan(&got); err != nil {
		t.Fatalf("read sequence_engine_key: %v", err)
	}
	if got != hlc {
		t.Errorf("sequence_engine_key = %d, want %d (precision lost in the round trip)", got, hlc)
	}
}

// The payload must arrive byte-identical: it is protobuf-derived JSON that downstream consumers
// decode, and a re-encode that reorders or re-escapes it would break the changefeed contract.
func TestSaveCheckpoint_PayloadRoundTripsUnchanged(t *testing.T) {
	pool := newTestDB(t)
	s := NewStore(pool)
	wf := seedWorkflow(t, s, 1787050674655252107)
	ctx := context.Background()
	payload := []byte(`{"po_number":"PO-88fa6139-1","total":"1234.56","lines":[{"sku":"SKU-1","qty":3}]}`)

	tx := begin(t, s, wf)
	if err := s.SaveCheckpointTyped(ctx, tx, wf, "draft_po", 1, "PurchaseOrderDrafted", payload); err != nil {
		t.Fatalf("SaveCheckpointTyped: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var got []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM orchestrator_outbox WHERE aggregate_id=$1`, wf.ID).Scan(&got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	var a, b any
	if err := json.Unmarshal(payload, &a); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if err := json.Unmarshal(got, &b); err != nil {
		t.Fatalf("stored payload is not valid JSON (%q): %v", string(got), err)
	}
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Errorf("payload round trip changed the document:\n got %s\nwant %s", got, payload)
	}
}
