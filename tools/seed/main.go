// Command seed drives ONE synthetic procurement event through the live OmniFlow stack end to end and
// exits 0 on success. It is the "make it real" harness: no simulated logs, a real message on the wire.
//
// The golden path it proves:
//
//	seed → commbot_outbox (real CRDB row)
//	     → CRDB changefeed → omniflow.orchestration.v1 (Kafka)
//	     → p2p-orchestrator (DAG topo-sort) → workflow SUSPENDS at human_approval
//	seed → omniflow.p2p.approval.v1 (Kafka) → orchestrator RESUMES → final_step
//	     → orchestrator_outbox (exactly-once CTE) → CRDB changefeed → omniflow.p2p.completed.v1
//	     → viz-gateway → SSE /api/stream carries a ProjectionEvent with our sequence_engine_key.
//
// SEED_MODE selects the entry seam:
//
//   - "outbox" (default): INSERT a *classified* event straight into commbot_outbox. This exercises
//     the entire changefeed/Kafka/orchestrator/HITL/viz spine WITHOUT requiring CommBot's egress LLM
//   - object-store fetch, which needs public HTTPS endpoints that do not exist inside CI. Robust,
//     zero external deps. This is the default the E2E asserts on.
//   - "email": produce a raw VendorEmailReceived to omniflow.communication.v1 and let the REAL
//     CommBot classify it (via the mock-llm) and write its own outbox row. Proves the extra hop but
//     needs SUBJECT_URI/BODY_URI reachable over HTTPS from a host on CommBot's OBJECT_STORE_HOSTS
//     allowlist (e.g. raw.githubusercontent.com at the tested commit).
//
// Whichever seam is used, the workflow key (event_id) is identical to the approval key, so the
// orchestrator resumes the same workflow.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	commv1 "omniflow/contracts/communication/v1"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	inventoryv1 "omniflow/contracts/inventory/v1"
)

const (
	orchestrationInputTopic = "omniflow.communication.v1" // CommBot inbound (email mode)
	approvalTopic           = "omniflow.p2p.approval.v1"  // HITL resume signal
	commbotOutboxEventType  = "VendorEmailClassified"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("seed: %v", err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	action := env("SEED_ACTION", "full")
	mode := env("SEED_MODE", "outbox")
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	dsn := env("CRDB_DSN", "postgres://root@localhost:26257/omniflow?sslmode=disable")

	if action == "approve-only" {
		eventID, err := eventIDFromEnv()
		if err != nil {
			return err
		}
		if eventID == "" {
			return errors.New("SEED_EVENT_ID is required for approve-only")
		}
		seqKeyStr := os.Getenv("SEED_SEQUENCE_ENGINE_KEY")
		if seqKeyStr == "" {
			return errors.New("SEED_SEQUENCE_ENGINE_KEY is required for approve-only")
		}
		var seqKey uint64
		if _, err := fmt.Sscanf(seqKeyStr, "%d", &seqKey); err != nil {
			return fmt.Errorf("parse SEED_SEQUENCE_ENGINE_KEY: %w", err)
		}
		traceParent := env("SEED_TRACE_PARENT", newTraceParent())

		log.Printf("approving existing workflow event_id=%s sequence_engine_key=%d", eventID, seqKey)
		if err := produceApproval(ctx, brokers, eventID, traceParent, seqKey); err != nil {
			return fmt.Errorf("produce approval: %w", err)
		}
		return nil
	}

	eventID, err := eventIDFromEnv()
	if err != nil {
		return err
	}
	if eventID == "" {
		eventID, err = uuidV4()
		if err != nil {
			return fmt.Errorf("generate event id: %w", err)
		}
	}

	var seqKey uint64
	if s := os.Getenv("SEED_INV_SEQ"); s != "" {
		if _, err := fmt.Sscanf(s, "%d", &seqKey); err != nil {
			return fmt.Errorf("parse SEED_INV_SEQ: %w", err)
		}
	} else {
		seqKey = uint64(time.Now().UnixNano()) // monotonic HLC-ish key, unique per run
	}

	traceParent := env("SEED_TRACE_PARENT", newTraceParent())
	aggregateID := env("SEED_VENDOR_ID", "VENDOR-ACME-001")

	log.Printf("seeding mode=%s event_id=%s sequence_engine_key=%d", logSafe(mode), eventID, seqKey)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect crdb: %w", err)
	}
	defer conn.Close(ctx)

	switch mode {
	case "outbox":
		if err := seedOutbox(ctx, conn, eventID, traceParent, aggregateID, seqKey); err != nil {
			return err
		}
	case "email":
		if err := seedEmail(ctx, brokers, eventID, traceParent, aggregateID, seqKey); err != nil {
			return err
		}
	case "inventory":
		if err := seedInventory(ctx, brokers, eventID, traceParent, aggregateID, seqKey); err != nil {
			return err
		}
		fmt.Printf("SEED_EVENT_ID=%s\n", eventID)
		fmt.Printf("SEED_SEQUENCE_ENGINE_KEY=%d\n", seqKey)
		return nil
	default:
		return fmt.Errorf("unknown SEED_MODE %q (want outbox|email|inventory)", mode)
	}

	// Wait for the orchestrator to create + suspend the workflow at human_approval before we approve,
	// otherwise the approval races ahead of the changefeed and burns retries. Polling the workflow row
	// is deterministic and also *proves* stages 2-3 ran for real.
	if err := waitForSuspended(ctx, conn, eventID); err != nil {
		return fmt.Errorf("workflow never suspended (upstream stages did not complete): %w", err)
	}
	log.Printf("workflow %s reached SUSPENDED — releasing human approval", eventID)

	// Machine-readable markers for the E2E script to assert the same key on the SSE stream.
	fmt.Printf("SEED_EVENT_ID=%s\n", eventID)
	fmt.Printf("SEED_SEQUENCE_ENGINE_KEY=%d\n", seqKey)

	if action == "suspend-only" {
		log.Printf("SEED_ACTION=suspend-only: exiting without approving")
		return nil
	}

	if err := produceApproval(ctx, brokers, eventID, traceParent, seqKey); err != nil {
		return fmt.Errorf("produce approval: %w", err)
	}

	return nil
}

// seedOutbox inserts a classified event directly into CommBot's outbox table. The commbot_outbox
// changefeed then emits it to omniflow.orchestration.v1 exactly as a real CommBot write would.
func seedOutbox(ctx context.Context, conn *pgx.Conn, eventID, traceParent, aggregateID string, seqKey uint64) error {
	payload, err := proto.Marshal(buildVendorEmail(eventID, traceParent, aggregateID, seqKey, true))
	if err != nil {
		return fmt.Errorf("marshal classified event: %w", err)
	}
	_, err = conn.Exec(ctx, `
		INSERT INTO commbot_outbox (aggregate_id, event_id, event_type, trace_parent, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5, now())
	`, aggregateID, eventID, commbotOutboxEventType, traceParent, payload)
	if err != nil {
		return fmt.Errorf("insert commbot_outbox: %w", err)
	}
	log.Printf("inserted commbot_outbox row (aggregate_id=%s) — changefeed will emit to %s", logSafe(aggregateID), "omniflow.orchestration.v1")
	return nil
}

// seedEmail produces a raw (unclassified) vendor email to CommBot's inbound topic and lets the real
// CommBot classify + write its own outbox row.
func seedEmail(ctx context.Context, brokers []string, eventID, traceParent, aggregateID string, seqKey uint64) error {
	email := buildVendorEmail(eventID, traceParent, aggregateID, seqKey, false)
	// email mode needs fetchable, allowlisted HTTPS URIs (CommBot's zero-trust quarantine boundary).
	email.SecureSubjectUri = env("SUBJECT_URI", email.SecureSubjectUri)
	email.SecureBodyUri = env("BODY_URI", email.SecureBodyUri)

	value, err := proto.Marshal(email)
	if err != nil {
		return fmt.Errorf("marshal vendor email: %w", err)
	}
	return produce(ctx, brokers, orchestrationInputTopic, aggregateID, value)
}

func produceApproval(ctx context.Context, brokers []string, eventID, traceParent string, seqKey uint64) error {
	approval := &commv1.HumanApprovalEvent{
		EventId:           eventID,
		TraceParent:       traceParent,
		SequenceEngineKey: seqKey,
		ApprovedBy:        "e2e-seeder",
	}
	value, err := proto.Marshal(approval)
	if err != nil {
		return fmt.Errorf("marshal approval: %w", err)
	}
	return produce(ctx, brokers, approvalTopic, eventID, value)
}

func produce(ctx context.Context, brokers []string, topic, key string, value []byte) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	log.Printf("produced to %s (key=%s, %d bytes)", logSafe(topic), logSafe(key), len(value))
	return nil
}

// waitForSuspended polls the workflows table until the orchestrator has created and suspended the
// workflow for this event, confirming the upstream changefeed + DAG stages actually ran.
func waitForSuspended(ctx context.Context, conn *pgx.Conn, eventID string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		var state string
		err := conn.QueryRow(ctx, `SELECT state FROM workflows WHERE event_id = $1`, eventID).Scan(&state)
		switch {
		case err == nil && state == "SUSPENDED":
			return nil
		case err == nil:
			log.Printf("workflow state=%s, waiting for SUSPENDED…", logSafe(state))
		case errors.Is(err, pgx.ErrNoRows):
			log.Printf("workflow not created yet, waiting…")
		default:
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after 60s (last state check for event_id=%s)", eventID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

// buildVendorEmail constructs a contract-valid VendorEmailReceived. When classified is true it is the
// post-CommBot shape (CLASSIFIED + a concrete intent); otherwise the raw ingested shape.
func buildVendorEmail(eventID, traceParent, aggregateID string, seqKey uint64, classified bool) *commv1.VendorEmailReceived {
	now := timestamppb.Now()
	e := &commv1.VendorEmailReceived{
		EventId:           eventID,
		TraceParent:       traceParent,
		AggregateId:       aggregateID,
		OccurredAt:        now,
		PublishedAt:       now,
		SequenceEngineKey: seqKey,
		VendorEmail:       env("SEED_VENDOR_EMAIL", "billing@acme-supply.example"),
		MdmVendorId:       aggregateID,
		// Valid HTTPS URIs so protovalidate's uri rule passes. In outbox mode they are never fetched;
		// in email mode override via SUBJECT_URI/BODY_URI with allowlisted, reachable content.
		SecureSubjectUri: "https://storage.googleapis.com/omniflow-quarantine/subject.txt",
		SecureBodyUri:    "https://storage.googleapis.com/omniflow-quarantine/body.txt",
	}
	if classified {
		e.VisualizationStage = commv1.VendorEmailReceived_VISUALIZATION_STAGE_CLASSIFIED
		e.IntentClassification = commv1.VendorEmailReceived_INTENT_INVOICE_SUBMISSION
	} else {
		e.VisualizationStage = commv1.VendorEmailReceived_VISUALIZATION_STAGE_INGESTED
		e.IntentClassification = commv1.VendorEmailReceived_INTENT_UNSPECIFIED
	}
	return e
}

// newTraceParent builds a W3C traceparent that satisfies the contract regex
// ^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$.
func newTraceParent() string {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	_, _ = rand.Read(traceID)
	_, _ = rand.Read(spanID)
	return fmt.Sprintf("00-%x-%x-01", traceID, spanID)
}

// uuidV4 returns a random RFC-4122 v4 UUID string (no external dependency).
func uuidV4() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// uuidRe matches the canonical 8-4-4-4-12 form produced by uuidV4.
var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// eventIDFromEnv reads SEED_EVENT_ID and rejects anything that is not a UUID.
//
// This is a boundary check, not decoration. The seeder writes two machine-readable markers to
// stdout, and scripts/e2e.sh recovers the key it asserts on with
//
//	grep '^SEED_SEQUENCE_ENGINE_KEY=' "$SEED_LOG" | cut -d= -f2
//
// The event id is printed one line above that marker. An id containing a newline would let the
// caller fabricate a SEED_SEQUENCE_ENGINE_KEY line of their choosing, and the end-to-end proof
// would then assert on a key nothing ever produced — a green run proving nothing. Constraining the
// id to a UUID removes the whole class rather than escaping one character of it.
func eventIDFromEnv() (string, error) {
	v := os.Getenv("SEED_EVENT_ID")
	if v == "" {
		return "", nil
	}
	if !uuidRe.MatchString(v) {
		return "", fmt.Errorf("SEED_EVENT_ID must be a UUID, got %q", logSafe(v))
	}
	return v, nil
}

// logSafe escapes the characters that could forge a new log record. Every input to this tool comes
// from the environment, and its stdout is parsed by scripts/e2e.sh (see eventIDFromEnv). Values
// that cannot be constrained to a fixed shape the way the event id can are escaped here instead.
func logSafe(s string) string {
	return strings.NewReplacer("\r", `\r`, "\n", `\n`).Replace(s)
}

func seedInventory(ctx context.Context, brokers []string, eventID, traceParent, aggregateID string, seqKey uint64) error {
	now := timestamppb.Now()

	movTypeStr := env("SEED_INV_MOVEMENT_TYPE", "receipt")
	var movType inventoryv1.InventoryMovementReceived_MovementType
	switch movTypeStr {
	case "receipt":
		movType = inventoryv1.InventoryMovementReceived_MOVEMENT_TYPE_RECEIPT
	case "consumption":
		movType = inventoryv1.InventoryMovementReceived_MOVEMENT_TYPE_CONSUMPTION
	case "adjustment":
		movType = inventoryv1.InventoryMovementReceived_MOVEMENT_TYPE_ADJUSTMENT
	default:
		return fmt.Errorf("unknown movement type: %s", movTypeStr)
	}

	sku := env("SEED_INV_SKU", "SKU-TEST-001")
	qty := env("SEED_INV_QTY", "10")
	unitCost := env("SEED_INV_UNIT_COST", "5.00")
	loc := env("SEED_INV_LOCATION", "LOC-1")
	vendor := env("SEED_INV_VENDOR", "VENDOR-ACME-001")

	movement := &inventoryv1.InventoryMovementReceived{
		EventId:           eventID,
		TraceParent:       traceParent,
		AggregateId:       aggregateID,
		OccurredAt:        now,
		PublishedAt:       now,
		SequenceEngineKey: seqKey,
		MovementType:      movType,
		Sku:               sku,
		MdmVendorId:       vendor,
		LocationId:        loc,
		Quantity:          qty,
		UnitCost:          unitCost,
	}

	// SEED_INV_INVALID produces a SEMANTIC poison pill: wire-valid protobuf that proto.Unmarshal
	// accepts and protovalidate then rejects. This is the only way to exercise the validator's
	// terminal-error path.
	//
	// It matters because the wire-level poison pill (a byte sequence that fails proto.Unmarshal)
	// returns at consumer.go's unmarshal guard and NEVER reaches validator.Validate — so without
	// this, "a protovalidate failure is terminal and goes to the DLQ" is an invariant nothing in
	// CI actually proves. event_id carries (buf.validate.field).string.uuid, so a non-UUID is
	// rejected by exactly one rule while every other field stays well-formed.
	if env("SEED_INV_INVALID", "") != "" {
		movement.EventId = "definitely-not-a-uuid"
	}

	value, err := proto.Marshal(movement)
	if err != nil {
		return fmt.Errorf("marshal inventory movement: %w", err)
	}

	return produce(ctx, brokers, "omniflow.inventory.movement.v1", sku, value)
}
