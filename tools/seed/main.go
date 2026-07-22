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
	"strings"
	"time"

	commv1 "omniflow/contracts/communication/v1"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		eventID := os.Getenv("SEED_EVENT_ID")
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

	eventID, err := uuidV4()
	if err != nil {
		return fmt.Errorf("generate event id: %w", err)
	}
	seqKey := uint64(time.Now().UnixNano()) // monotonic HLC-ish key, unique per run
	traceParent := newTraceParent()
	aggregateID := env("SEED_VENDOR_ID", "VENDOR-ACME-001")

	log.Printf("seeding mode=%s event_id=%s sequence_engine_key=%d", mode, eventID, seqKey)

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
	default:
		return fmt.Errorf("unknown SEED_MODE %q (want outbox|email)", mode)
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
	log.Printf("inserted commbot_outbox row (aggregate_id=%s) — changefeed will emit to %s", aggregateID, "omniflow.orchestration.v1")
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
	log.Printf("produced to %s (key=%s, %d bytes)", topic, key, len(value))
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
			log.Printf("workflow state=%s, waiting for SUSPENDED…", state)
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
