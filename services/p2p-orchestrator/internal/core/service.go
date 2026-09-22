package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1 "omniflow/contracts/communication/v1"
	"omniflow/internal/platform/metrics"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// Config is the operational shape of the orchestrator — who this pod is and how long a workflow
// may wait at the human gate. Both used to be literals in the code path.
type Config struct {
	// OwnerPod identifies this process on the workflow row while it is parked at the human gate.
	// A literal "orchestrator-pod-local" told an operator nothing; the hostname tells them which
	// replica parked it.
	OwnerPod string
	// HITLLeaseTTL is how long a workflow may sit SUSPENDED awaiting approval before the sweep
	// fails it. A workflow that waits forever is a purchase order nobody will ever see again.
	HITLLeaseTTL time.Duration
}

// DefaultHITLLeaseTTL is the approval window when none is configured.
const DefaultHITLLeaseTTL = 24 * time.Hour

// failedLedgerNode is the node_execution_ledger key under which a workflow's failure is recorded.
// It shares the ledger's uniqueness with real nodes, so the WorkflowFailed outbox event is
// emitted exactly once no matter how many consumers or sweeps observe the same failure.
const failedLedgerNode = "__failed"

type OrchestratorService struct {
	store  ports.Checkpointer
	tracer trace.Tracer
	dag    *domain.DAG
	cfg    Config

	// executors maps a node id to the thing that does its work. A node with no entry is a
	// pass-through checkpoint, which is what lets a partially implemented DAG still run end to end.
	// This map is the seam the agent fleet fans out along.
	executors map[string]ports.NodeExecutor
}

func NewOrchestratorService(s ports.Checkpointer, d *domain.DAG, executors map[string]ports.NodeExecutor, cfg Config) *OrchestratorService {
	if executors == nil {
		executors = map[string]ports.NodeExecutor{}
	}
	if cfg.OwnerPod == "" {
		cfg.OwnerPod = "orchestrator"
	}
	if cfg.HITLLeaseTTL <= 0 {
		cfg.HITLLeaseTTL = DefaultHITLLeaseTTL
	}
	return &OrchestratorService{
		store:     s,
		tracer:    otel.Tracer("p2p-orchestrator"),
		dag:       d,
		cfg:       cfg,
		executors: executors,
	}
}

// approvalPayload is the outbox event a human approval emits. It carries the audit facts the
// approval message brought with it: WHO approved, and the trace of the approval itself (distinct
// from the workflow's originating trace). Marshalled, never concatenated — approved_by is
// external input and a quote in it must not be able to change the event's shape.
type approvalPayload struct {
	Status              string `json:"status"`
	Node                string `json:"node"`
	ApprovedBy          string `json:"approved_by"`
	ApprovalTraceParent string `json:"approval_trace_parent,omitempty"`
	ApprovedAt          string `json:"approved_at"`
}

// failurePayload is the WorkflowFailed outbox event.
type failurePayload struct {
	Status   string `json:"status"`
	Node     string `json:"node,omitempty"`
	Reason   string `json:"reason"`
	FailedAt string `json:"failed_at"`
}

// executeNode runs a node's executor, if it has one. Nodes without an executor return nil, which
// the caller treats as a plain checkpoint.
func (s *OrchestratorService) executeNode(ctx context.Context, wf *domain.Workflow, nodeID string, attempt int) (*domain.NodeResult, error) {
	exec, ok := s.executors[nodeID]
	if !ok {
		return nil, nil
	}

	ctx, span := s.tracer.Start(ctx, "ExecuteNode")
	defer span.End()
	span.SetAttributes(
		attribute.String("workflow.id", wf.ID),
		attribute.String("node.id", nodeID),
		attribute.Int("node.attempt", attempt),
	)

	result, err := exec.Execute(ctx, domain.NodeRequest{
		WorkflowID:     wf.ID,
		EventID:        wf.EventID,
		NodeID:         nodeID,
		Attempt:        attempt,
		TraceParent:    wf.TraceParent,
		SequenceKey:    wf.SequenceEngineKey,
		TriggerPayload: wf.TriggerPayload,
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return result, nil
}

func (s *OrchestratorService) ProcessEvent(ctx context.Context, payload []byte, isApproval bool) error {
	ctx, span := s.tracer.Start(ctx, "ProcessEvent")
	defer span.End()

	if isApproval {
		return s.handleApproval(ctx, payload)
	}
	return s.handleWorkflowTrigger(ctx, payload)
}

func (s *OrchestratorService) handleWorkflowTrigger(ctx context.Context, payload []byte) error {
	var event v1.VendorEmailReceived
	if err := proto.Unmarshal(payload, &event); err != nil {
		return domain.ErrTerminal
	}

	sortedNodes, err := s.dag.TopoSort()
	if err != nil {
		return err
	}

	// The trigger payload is persisted on the workflow row so any pod can resume a node later.
	wf, err := s.store.LoadOrCreateWorkflow(ctx, event.EventId, event.TraceParent, event.SequenceEngineKey, sortedNodes, payload)
	if err != nil {
		return err
	}

	return s.drainWorkflow(ctx, wf)
}

func (s *OrchestratorService) handleApproval(ctx context.Context, payload []byte) error {
	var event v1.HumanApprovalEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return domain.ErrTerminal
	}

	wf, err := s.store.LoadWorkflowByEventID(ctx, event.EventId)
	if err != nil {
		return err
	}

	tx, err := s.store.AcquireLease(ctx, wf.ID)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	nodeID := wf.NextNode()

	if wf.State != domain.StateSuspended || nodeID != "human_approval" {
		slog.Warn("stray approval ignored",
			"workflow_id", wf.ID, "event_id", wf.EventID, "state", wf.State, "next_node", nodeID)
		return nil
	}

	attempt := 1
	executed, err := s.store.CheckIdempotency(ctx, wf.ID, nodeID, attempt)
	if err != nil {
		slog.Error("idempotency check failed on approval",
			"workflow_id", wf.ID, "event_id", wf.EventID, "error", err)
		return err
	}
	if executed {
		// The duplicate-approval suppression path. Silence here made it impossible to tell
		// suppression from a dropped message; this is the line that proves exactly-once held.
		slog.Info("duplicate approval suppressed",
			"workflow_id", wf.ID, "event_id", wf.EventID, "approved_by", event.ApprovedBy)
		return nil
	}

	slog.Info("approval accepted, resuming workflow",
		"workflow_id", wf.ID, "event_id", wf.EventID, "approved_by", event.ApprovedBy)

	wf.CurrentNodeIndex++
	wf.State = domain.StateRunning
	wf.OwnerPod = "" // Release the human-in-the-loop durable lease
	wf.LeaseExpiresAt = time.Time{}

	outboxPayload, err := json.Marshal(approvalPayload{
		Status:              "approved",
		Node:                nodeID,
		ApprovedBy:          event.ApprovedBy,
		ApprovalTraceParent: event.TraceParent,
		ApprovedAt:          time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("%w: marshal approval payload: %w", domain.ErrTerminal, err)
	}

	if err := s.store.SaveCheckpointTyped(ctx, tx, wf, nodeID, attempt, "HumanApproved", outboxPayload); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return s.drainWorkflow(ctx, wf)
}

func (s *OrchestratorService) drainWorkflow(ctx context.Context, wf *domain.Workflow) error {
	// Every branch below is logged. This path previously emitted NOTHING — not a node transition, not
	// a lease acquisition, not a completion — which is why a workflow that stalled mid-DAG was
	// indistinguishable from one that was merely slow, and why a self-deadlock here went undiagnosed
	// through several CI runs. Workflow id and event id are on every line so a single record can be
	// followed end to end across services by either key.
	log := slog.With("workflow_id", wf.ID, "event_id", wf.EventID, "seq_key", wf.SequenceEngineKey)

	for {
		if wf.State == domain.StateCompleted || wf.State == domain.StateFailed || wf.State == domain.StateSuspended {
			log.Info("drain stopped", "state", wf.State, "node_index", wf.CurrentNodeIndex)
			return nil
		}

		nodeID := wf.NextNode()
		if nodeID == "" {
			tx, err := s.store.AcquireLease(ctx, wf.ID)
			if err != nil {
				log.Warn("lease unavailable while completing", "error", err)
				return err
			}

			wf.State = domain.StateCompleted
			if err := s.store.SaveCheckpoint(ctx, tx, wf, "", 0, nil); err != nil {
				if rbErr := tx.Rollback(ctx); rbErr != nil {
					log.Warn("rollback after failed completion checkpoint", "error", rbErr)
				}
				log.Error("checkpoint failed while completing", "error", err)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				log.Error("commit failed while completing", "error", err)
				return err
			}
			metrics.WorkflowTransitions.WithLabelValues(string(domain.StateCompleted)).Inc()
			log.Info("workflow completed", "nodes_executed", wf.CurrentNodeIndex)
			return nil
		}

		tx, err := s.store.AcquireLease(ctx, wf.ID)
		if err != nil {
			log.Warn("lease unavailable", "node", nodeID, "error", err)
			return err
		}

		attempt := 1
		executed, err := s.store.CheckIdempotency(ctx, wf.ID, nodeID, attempt)
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				log.Warn("rollback after failed idempotency check", "node", nodeID, "error", rbErr)
			}
			log.Error("idempotency check failed", "node", nodeID, "error", err)
			return err
		}
		if executed {
			// Redelivery of an already-executed node. Expected under at-least-once, so it is info,
			// not a warning — but it must be visible, because a flood of these means offsets are
			// not advancing.
			wf.CurrentNodeIndex++
			if err := s.store.SaveCheckpoint(ctx, tx, wf, nodeID, attempt, nil); err != nil {
				if rbErr := tx.Rollback(ctx); rbErr != nil {
					log.Warn("rollback after failed replay checkpoint", "node", nodeID, "error", rbErr)
				}
				log.Error("checkpoint failed on replayed node", "node", nodeID, "error", err)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				log.Error("commit failed on replayed node", "node", nodeID, "error", err)
				return err
			}
			log.Info("node already executed, advancing", "node", nodeID, "attempt", attempt)
			continue
		}

		if nodeID == "human_approval" {
			wf.State = domain.StateSuspended
			wf.OwnerPod = s.cfg.OwnerPod
			wf.LeaseExpiresAt = time.Now().Add(s.cfg.HITLLeaseTTL)

			if err := s.store.SaveCheckpoint(ctx, tx, wf, "", 0, nil); err != nil {
				if rbErr := tx.Rollback(ctx); rbErr != nil {
					log.Warn("rollback after failed suspend checkpoint", "error", rbErr)
				}
				log.Error("checkpoint failed while suspending", "error", err)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				log.Error("commit failed while suspending", "error", err)
				return err
			}
			metrics.WorkflowTransitions.WithLabelValues(string(domain.StateSuspended)).Inc()
			log.Info("suspended awaiting human approval", "lease_expires_at", wf.LeaseExpiresAt)
			return nil
		}

		// Fast DB tx explicit exit: release the FOR UPDATE lock before slow I/O. An agent call can
		// take seconds; holding a row lock across it would serialise every other worker behind a
		// third party we do not control.
		if err := tx.Rollback(ctx); err != nil {
			log.Warn("rollback before node execution failed", "node", nodeID, "error", err)
		}

		// ---- NODE EXECUTION (outside the transaction, therefore at-least-once) ----
		//
		// A crash between this call and the checkpoint below re-runs the node on redelivery. We do
		// not attempt to prevent the duplicate execution — that would require consensus with an
		// external service — so every side effect a node returns is persisted under a deterministic
		// idempotency key instead. At-least-once execution, exactly-once effect.
		result, execErr := s.executeNode(ctx, wf, nodeID, attempt)
		if execErr != nil {
			log.Error("node execution failed", "node", nodeID, "attempt", attempt, "error", execErr)
			return execErr
		}

		// Re-acquire to checkpoint the node completion
		tx, err = s.store.AcquireLease(ctx, wf.ID)
		if err != nil {
			log.Warn("lease unavailable when checkpointing node completion", "node", nodeID, "error", err)
			return err
		}

		// Re-fetch under the lock to confirm the index hasn't advanced.
		// MUST use the Tx variant: AcquireLease above holds `FOR UPDATE NOWAIT` on this very row, so
		// reading it on a separate pool connection deadlocks against our own lock and the workflow
		// never completes.
		latestWf, err := s.store.LoadWorkflowByEventIDTx(ctx, tx, wf.EventID)
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				log.Warn("rollback after failed re-fetch under lease", "node", nodeID, "error", rbErr)
			}
			log.Error("re-fetch under lease failed", "node", nodeID, "error", err)
			return err
		}
		if latestWf.CurrentNodeIndex != wf.CurrentNodeIndex {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				log.Warn("rollback while yielding to another worker", "node", nodeID, "error", rbErr)
			}
			// Another pod advanced this workflow. Benign, but no longer silent: repeated occurrences
			// indicate two pods contending for the same workflow.
			log.Info("yielding, another worker advanced this workflow",
				"node", nodeID, "our_index", wf.CurrentNodeIndex, "observed_index", latestWf.CurrentNodeIndex)
			return nil
		}

		wf.CurrentNodeIndex++

		eventType := "NodeTransition"
		outboxPayload := []byte(`{"status":"completed","node":"` + nodeID + `"}`)
		if result != nil && len(result.Payload) > 0 {
			eventType = result.EventType
			outboxPayload = result.Payload
		}

		// The side effect, the idempotency ledger row, and the outbox event land in ONE transaction.
		// That is what makes the emitted event and the durable artifact impossible to disagree.
		if result != nil && result.Effect != nil {
			inserted, err := s.store.InsertPurchaseOrder(ctx, tx, wf.ID, nodeID, attempt, result.Effect)
			if err != nil {
				if rbErr := tx.Rollback(ctx); rbErr != nil {
					log.Warn("rollback after failed purchase order insert", "node", nodeID, "error", rbErr)
				}
				log.Error("persisting purchase order failed", "node", nodeID, "error", err)
				return err
			}
			if !inserted {
				// A prior execution of this node already produced the PO. Expected after a crash
				// between the agent call and this checkpoint — the exactly-once effect working, not
				// a fault — but it must be visible, because a flood means nodes are re-running.
				log.Info("purchase order already existed for this node, not re-issued",
					"node", nodeID, "attempt", attempt, "po_number", result.Effect.PONumber)
			}
		}

		if err := s.store.SaveCheckpointTyped(ctx, tx, wf, nodeID, attempt, eventType, outboxPayload); err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				log.Warn("rollback after failed completion checkpoint", "node", nodeID, "error", rbErr)
			}
			log.Error("checkpoint failed on node completion", "node", nodeID, "error", err)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			log.Error("commit failed on node completion", "node", nodeID, "error", err)
			return err
		}
		log.Info("node completed", "node", nodeID, "attempt", attempt, "node_index", wf.CurrentNodeIndex)
	}
}

// FailWorkflow moves the workflow identified by eventID to FAILED and emits one WorkflowFailed
// outbox event, exactly once. It is the consumer's last act before dead-lettering a record and
// the sweep's verdict on an approval that never came.
//
// Before this existed a terminal node error left the row RUNNING forever: the DLQ record was the
// only evidence, and nothing downstream — not the dashboard, not a replay — could tell a failed
// workflow from a slow one. StateFailed was defined, checked in drainWorkflow, and never assigned.
//
// Already-terminal workflows are left alone (a duplicate dead-letter or a sweep racing a late
// approval must not flip COMPLETED to FAILED). Lease contention is returned as transient so the
// caller — a sweep tick or a consumer about to dead-letter anyway — simply tries later.
func (s *OrchestratorService) FailWorkflow(ctx context.Context, eventID, nodeID, reason string) error {
	ctx, span := s.tracer.Start(ctx, "FailWorkflow")
	defer span.End()

	wf, err := s.store.LoadWorkflowByEventID(ctx, eventID)
	if err != nil {
		return err
	}
	log := slog.With("workflow_id", wf.ID, "event_id", wf.EventID, "seq_key", wf.SequenceEngineKey)

	tx, err := s.store.AcquireLease(ctx, wf.ID)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Re-read under the lock: the pool read above may predate a concurrent completion.
	wf, err = s.store.LoadWorkflowByEventIDTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if wf.State == domain.StateCompleted || wf.State == domain.StateFailed {
		log.Info("fail requested on a terminal workflow, ignored", "state", wf.State, "reason", reason)
		return nil
	}

	wf.State = domain.StateFailed
	wf.OwnerPod = ""
	wf.LeaseExpiresAt = time.Time{}

	payload, err := json.Marshal(failurePayload{
		Status:   "failed",
		Node:     nodeID,
		Reason:   reason,
		FailedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("%w: marshal failure payload: %w", domain.ErrTerminal, err)
	}
	if err := s.store.SaveCheckpointTyped(ctx, tx, wf, failedLedgerNode, 1, "WorkflowFailed", payload); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	metrics.WorkflowTransitions.WithLabelValues(string(domain.StateFailed)).Inc()
	log.Error("workflow failed", "node", nodeID, "reason", reason)
	return nil
}

// ReapExpiredApprovals fails every workflow that has sat SUSPENDED past its HITL lease. Called on
// a timer from main; safe to run on every replica concurrently, because FailWorkflow takes the
// row lock NOWAIT and a loser simply skips the row until the next tick.
//
// Returns the number of workflows it failed. Errors on individual workflows are logged and do not
// stop the sweep — one contended row must not shield the rest.
func (s *OrchestratorService) ReapExpiredApprovals(ctx context.Context, limit int) (int, error) {
	ctx, span := s.tracer.Start(ctx, "ReapExpiredApprovals")
	defer span.End()

	expired, err := s.store.ListExpiredSuspended(ctx, time.Now(), limit)
	if err != nil {
		return 0, err
	}
	failed := 0
	for _, eventID := range expired {
		reason := fmt.Sprintf("human approval not received within %s", s.cfg.HITLLeaseTTL)
		if err := s.FailWorkflow(ctx, eventID, "human_approval", reason); err != nil {
			if errors.Is(err, domain.ErrTransient) {
				slog.Info("expired approval skipped this sweep", "event_id", eventID, "error", err)
			} else {
				slog.Error("failing expired approval", "event_id", eventID, "error", err)
			}
			continue
		}
		failed++
		metrics.ApprovalsExpired.Inc()
	}
	return failed, nil
}
