package core

import (
	"context"
	"log/slog"
	"time"

	v1 "omniflow/contracts/communication/v1"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type OrchestratorService struct {
	store  ports.Checkpointer
	tracer trace.Tracer
	dag    *domain.DAG
}

func NewOrchestratorService(s ports.Checkpointer, d *domain.DAG) *OrchestratorService {
	return &OrchestratorService{
		store:  s,
		tracer: otel.Tracer("p2p-orchestrator"),
		dag:    d,
	}
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

	wf, err := s.store.LoadOrCreateWorkflow(ctx, event.EventId, event.TraceParent, event.SequenceEngineKey, sortedNodes)
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

	outboxPayload := []byte(`{"status":"approved","node":"` + nodeID + `"}`)

	if err := s.store.SaveCheckpoint(ctx, tx, wf, nodeID, attempt, outboxPayload); err != nil {
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
				tx.Rollback(ctx)
				log.Error("checkpoint failed while completing", "error", err)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				log.Error("commit failed while completing", "error", err)
				return err
			}
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
			tx.Rollback(ctx)
			log.Error("idempotency check failed", "node", nodeID, "error", err)
			return err
		}
		if executed {
			// Redelivery of an already-executed node. Expected under at-least-once, so it is info,
			// not a warning — but it must be visible, because a flood of these means offsets are
			// not advancing.
			wf.CurrentNodeIndex++
			if err := s.store.SaveCheckpoint(ctx, tx, wf, nodeID, attempt, nil); err != nil {
				tx.Rollback(ctx)
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
			wf.OwnerPod = "orchestrator-pod-local"
			wf.LeaseExpiresAt = time.Now().Add(24 * time.Hour)

			if err := s.store.SaveCheckpoint(ctx, tx, wf, "", 0, nil); err != nil {
				tx.Rollback(ctx)
				log.Error("checkpoint failed while suspending", "error", err)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				log.Error("commit failed while suspending", "error", err)
				return err
			}
			log.Info("suspended awaiting human approval", "lease_expires_at", wf.LeaseExpiresAt)
			return nil
		}

		// Fast DB tx explicit exit: release FOR UPDATE lock for slow I/O
		tx.Rollback(ctx)

		// [EXTERNAL I/O EXECUTION HAPPENS HERE OUTSIDE THE TRANSACTION]

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
			tx.Rollback(ctx)
			log.Error("re-fetch under lease failed", "node", nodeID, "error", err)
			return err
		}
		if latestWf.CurrentNodeIndex != wf.CurrentNodeIndex {
			tx.Rollback(ctx)
			// Another pod advanced this workflow. Benign, but no longer silent: repeated occurrences
			// indicate two pods contending for the same workflow.
			log.Info("yielding, another worker advanced this workflow",
				"node", nodeID, "our_index", wf.CurrentNodeIndex, "observed_index", latestWf.CurrentNodeIndex)
			return nil
		}

		wf.CurrentNodeIndex++
		outboxPayload := []byte(`{"status":"completed","node":"` + nodeID + `"}`)

		if err := s.store.SaveCheckpoint(ctx, tx, wf, nodeID, attempt, outboxPayload); err != nil {
			tx.Rollback(ctx)
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
