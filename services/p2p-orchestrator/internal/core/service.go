package core

import (
	"context"
	"log/slog"
	"time"

	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"
	v1 "omniflow/contracts/communication/v1"

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
		slog.Warn("Stray human approval event or workflow not suspended", "wf_id", wf.ID)
		return nil
	}

	attempt := 1
	executed, err := s.store.CheckIdempotency(ctx, wf.ID, nodeID, attempt)
	if err != nil {
		return err
	}
	if executed {
		return nil
	}

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
	for {
		if wf.State == domain.StateCompleted || wf.State == domain.StateFailed || wf.State == domain.StateSuspended {
			return nil
		}

		nodeID := wf.NextNode()
		if nodeID == "" {
			tx, err := s.store.AcquireLease(ctx, wf.ID)
			if err != nil {
				return err
			}

			wf.State = domain.StateCompleted
			if err := s.store.SaveCheckpoint(ctx, tx, wf, "", 0, nil); err != nil {
				tx.Rollback(ctx)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		}

		tx, err := s.store.AcquireLease(ctx, wf.ID)
		if err != nil {
			return err
		}

		attempt := 1
		executed, err := s.store.CheckIdempotency(ctx, wf.ID, nodeID, attempt)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		if executed {
			wf.CurrentNodeIndex++
			if err := s.store.SaveCheckpoint(ctx, tx, wf, nodeID, attempt, nil); err != nil {
				tx.Rollback(ctx)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			continue
		}

		if nodeID == "human_approval" {
			wf.State = domain.StateSuspended
			wf.OwnerPod = "orchestrator-pod-local"
			wf.LeaseExpiresAt = time.Now().Add(24 * time.Hour)
			
			if err := s.store.SaveCheckpoint(ctx, tx, wf, "", 0, nil); err != nil {
				tx.Rollback(ctx)
				return err
			}
			return tx.Commit(ctx)
		}

		// Fast DB tx explicit exit: release FOR UPDATE lock for slow I/O
		tx.Rollback(ctx)

		// [EXTERNAL I/O EXECUTION HAPPENS HERE OUTSIDE THE TRANSACTION]

		// Re-acquire to checkpoint the node completion
		tx, err = s.store.AcquireLease(ctx, wf.ID)
		if err != nil {
			return err
		}
		
		// Re-fetch under the lock to confirm the index hasn't advanced
		latestWf, err := s.store.LoadWorkflowByEventID(ctx, wf.EventID)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		if latestWf.CurrentNodeIndex != wf.CurrentNodeIndex {
			tx.Rollback(ctx)
			return nil // quiet abort, another worker advanced it
		}

		wf.CurrentNodeIndex++
		outboxPayload := []byte(`{"status":"completed","node":"` + nodeID + `"}`)
		
		if err := s.store.SaveCheckpoint(ctx, tx, wf, nodeID, attempt, outboxPayload); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
}
