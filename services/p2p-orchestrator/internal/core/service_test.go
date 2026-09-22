package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "omniflow/contracts/communication/v1"
	"omniflow/services/p2p-orchestrator/internal/core/domain"
	"omniflow/services/p2p-orchestrator/internal/core/ports"

	"google.golang.org/protobuf/proto"
)

// ---- fake checkpointer -----------------------------------------------------------------------
//
// In-memory, single-process stand-in for the CockroachDB store. It models what the service
// depends on — the ledger's uniqueness, the outbox rows a checkpoint emits, and lease contention —
// and nothing else. The real SQL (the exactly-once CTE, placeholder typing) is covered by the
// integration suite; this is for the orchestration logic above it, which had no test at all.

type outboxRow struct {
	nodeID    string
	eventType string
	payload   []byte
}

type fakeStore struct {
	mu        sync.Mutex
	workflows map[string]*domain.Workflow // by event id
	ledger    map[string]bool             // "wfID/node/attempt"
	outbox    []outboxRow
	contended map[string]bool // workflow ids whose lease is held elsewhere
	nextID    int
}

type fakeTx struct {
	done bool
}

func (t *fakeTx) Commit(context.Context) error   { t.done = true; return nil }
func (t *fakeTx) Rollback(context.Context) error { t.done = true; return nil }

func newFakeStore() *fakeStore {
	return &fakeStore{workflows: map[string]*domain.Workflow{}, ledger: map[string]bool{}, contended: map[string]bool{}}
}

func (f *fakeStore) LoadOrCreateWorkflow(_ context.Context, eventID, traceParent string, seqKey uint64, sortedNodes []string, trigger []byte) (*domain.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if wf, ok := f.workflows[eventID]; ok {
		return copyWF(wf), nil
	}
	f.nextID++
	wf := &domain.Workflow{
		ID: fmt.Sprintf("wf-%d", f.nextID), EventID: eventID, TraceParent: traceParent,
		SequenceEngineKey: seqKey, State: domain.StatePending, SortedNodes: sortedNodes, TriggerPayload: trigger,
	}
	f.workflows[eventID] = wf
	return copyWF(wf), nil
}

func (f *fakeStore) LoadWorkflowByEventID(_ context.Context, eventID string) (*domain.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	wf, ok := f.workflows[eventID]
	if !ok {
		return nil, fmt.Errorf("%w: workflow not found (yet)", domain.ErrTransient)
	}
	return copyWF(wf), nil
}

func (f *fakeStore) LoadWorkflowByEventIDTx(ctx context.Context, _ ports.Transaction, eventID string) (*domain.Workflow, error) {
	return f.LoadWorkflowByEventID(ctx, eventID)
}

func (f *fakeStore) AcquireLease(_ context.Context, workflowID string) (ports.Transaction, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.contended[workflowID] {
		return nil, fmt.Errorf("%w: lease held by another worker (55P03)", domain.ErrTransient)
	}
	return &fakeTx{}, nil
}

func (f *fakeStore) ListExpiredSuspended(_ context.Context, now time.Time, limit int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, wf := range f.workflows {
		if wf.State == domain.StateSuspended && !wf.LeaseExpiresAt.IsZero() && wf.LeaseExpiresAt.Before(now) {
			out = append(out, wf.EventID)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeStore) CheckIdempotency(_ context.Context, workflowID, nodeID string, attempt int) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ledger[ledgerKey(workflowID, nodeID, attempt)], nil
}

func (f *fakeStore) SaveCheckpoint(ctx context.Context, tx ports.Transaction, wf *domain.Workflow, nodeID string, attempt int, payload []byte) error {
	return f.SaveCheckpointTyped(ctx, tx, wf, nodeID, attempt, "NodeTransition", payload)
}

// SaveCheckpointTyped mirrors the CTE's contract: the workflow row is updated; if a node is named
// its ledger row is inserted (ON CONFLICT DO NOTHING); an outbox row is emitted only when a
// payload is present AND the ledger insert was new.
func (f *fakeStore) SaveCheckpointTyped(_ context.Context, _ ports.Transaction, wf *domain.Workflow, nodeID string, attempt int, eventType string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored := f.workflows[wf.EventID]
	*stored = *copyWF(wf)
	if nodeID == "" {
		return nil
	}
	k := ledgerKey(wf.ID, nodeID, attempt)
	inserted := !f.ledger[k]
	f.ledger[k] = true
	if inserted && len(payload) > 0 {
		f.outbox = append(f.outbox, outboxRow{nodeID: nodeID, eventType: eventType, payload: payload})
	}
	return nil
}

func (f *fakeStore) InsertPurchaseOrder(context.Context, ports.Transaction, string, string, int, *domain.PurchaseOrderEffect) (bool, error) {
	return true, nil
}

func (f *fakeStore) state(eventID string) *domain.Workflow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return copyWF(f.workflows[eventID])
}

func (f *fakeStore) outboxTypes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.outbox {
		out = append(out, r.eventType)
	}
	return out
}

func ledgerKey(wf, node string, attempt int) string {
	return fmt.Sprintf("%s/%s/%d", wf, node, attempt)
}

func copyWF(w *domain.Workflow) *domain.Workflow {
	c := *w
	c.SortedNodes = append([]string(nil), w.SortedNodes...)
	return &c
}

// ---- helpers ---------------------------------------------------------------------------------

func testDAG() *domain.DAG {
	return &domain.DAG{Nodes: map[string]*domain.Node{
		"draft_po":       {ID: "draft_po", Dependencies: []string{}},
		"human_approval": {ID: "human_approval", Dependencies: []string{"draft_po"}},
		"final_step":     {ID: "final_step", Dependencies: []string{"human_approval"}},
	}}
}

const (
	testEventID = "5f05b9d0-e173-43cc-b52a-627671f55508"
	testTrace   = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

func triggerBytes(t *testing.T) []byte {
	t.Helper()
	b, err := proto.Marshal(&v1.VendorEmailReceived{EventId: testEventID, TraceParent: testTrace, SequenceEngineKey: 1790023750749228000})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func approvalBytes(t *testing.T, approvedBy string) []byte {
	t.Helper()
	b, err := proto.Marshal(&v1.HumanApprovalEvent{EventId: testEventID, TraceParent: testTrace, SequenceEngineKey: 1790023750749228000, ApprovedBy: approvedBy})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newService(store *fakeStore, execs map[string]ports.NodeExecutor, cfg Config) *OrchestratorService {
	return NewOrchestratorService(store, testDAG(), execs, cfg)
}

// ---- tests -----------------------------------------------------------------------------------

func TestTriggerRunsToTheHumanGateAndParksWithLease(t *testing.T) {
	store := newFakeStore()
	svc := newService(store, nil, Config{OwnerPod: "pod-a", HITLLeaseTTL: 2 * time.Hour})
	before := time.Now()

	if err := svc.ProcessEvent(context.Background(), triggerBytes(t), false); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}

	wf := store.state(testEventID)
	if wf.State != domain.StateSuspended {
		t.Fatalf("state = %s, want SUSPENDED", wf.State)
	}
	if wf.NextNode() != "human_approval" {
		t.Fatalf("parked at %q, want human_approval", wf.NextNode())
	}
	if wf.OwnerPod != "pod-a" {
		t.Fatalf("owner_pod = %q, want the configured pod, not a literal", wf.OwnerPod)
	}
	if lease := wf.LeaseExpiresAt.Sub(before); lease < 119*time.Minute || lease > 121*time.Minute {
		t.Fatalf("lease expires in %v, want ~2h (the configured TTL)", lease)
	}
	if got := store.outboxTypes(); len(got) != 1 || got[0] != "NodeTransition" {
		t.Fatalf("outbox after draft_po = %v, want one NodeTransition", got)
	}
}

func TestApprovalResumesEmitsAuditedEventAndCompletes(t *testing.T) {
	store := newFakeStore()
	svc := newService(store, nil, Config{})
	ctx := context.Background()
	if err := svc.ProcessEvent(ctx, triggerBytes(t), false); err != nil {
		t.Fatal(err)
	}

	// approved_by carries a quote and a brace: a concatenated payload would have broken here.
	approver := `alice "the auditor" {finance}`
	if err := svc.ProcessEvent(ctx, approvalBytes(t, approver), true); err != nil {
		t.Fatalf("approval: %v", err)
	}

	wf := store.state(testEventID)
	if wf.State != domain.StateCompleted {
		t.Fatalf("state = %s, want COMPLETED after approval", wf.State)
	}
	if wf.OwnerPod != "" || !wf.LeaseExpiresAt.IsZero() {
		t.Fatalf("lease not released after approval: owner=%q expires=%v", wf.OwnerPod, wf.LeaseExpiresAt)
	}
	want := []string{"NodeTransition", "HumanApproved", "NodeTransition"} // draft_po, approval, final_step
	if got := store.outboxTypes(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("outbox = %v, want %v", got, want)
	}
	var p approvalPayload
	if err := json.Unmarshal(store.outbox[1].payload, &p); err != nil {
		t.Fatalf("HumanApproved payload is not JSON: %v — %s", err, store.outbox[1].payload)
	}
	if p.ApprovedBy != approver || p.Node != "human_approval" || p.Status != "approved" || p.ApprovalTraceParent != testTrace {
		t.Fatalf("HumanApproved payload = %+v", p)
	}
}

func TestDuplicateApprovalIsSuppressed(t *testing.T) {
	store := newFakeStore()
	svc := newService(store, nil, Config{})
	ctx := context.Background()
	if err := svc.ProcessEvent(ctx, triggerBytes(t), false); err != nil {
		t.Fatal(err)
	}
	if err := svc.ProcessEvent(ctx, approvalBytes(t, "a"), true); err != nil {
		t.Fatal(err)
	}
	outboxBefore := len(store.outboxTypes())

	if err := svc.ProcessEvent(ctx, approvalBytes(t, "a"), true); err != nil {
		t.Fatalf("duplicate approval must be a no-op, got %v", err)
	}
	if got := len(store.outboxTypes()); got != outboxBefore {
		t.Fatalf("duplicate approval emitted rows: outbox %d -> %d", outboxBefore, got)
	}
	if store.state(testEventID).State != domain.StateCompleted {
		t.Fatal("duplicate approval changed the terminal state")
	}
}

func TestStrayApprovalBeforeTheGateIsIgnored(t *testing.T) {
	store := newFakeStore()
	svc := newService(store, nil, Config{})
	// A workflow that exists but is not parked at the gate.
	if _, err := store.LoadOrCreateWorkflow(context.Background(), testEventID, testTrace, 1, []string{"draft_po", "human_approval", "final_step"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := svc.ProcessEvent(context.Background(), approvalBytes(t, "a"), true); err != nil {
		t.Fatalf("stray approval must be ignored, got %v", err)
	}
	if st := store.state(testEventID).State; st != domain.StatePending {
		t.Fatalf("stray approval moved the workflow to %s", st)
	}
	if n := len(store.outboxTypes()); n != 0 {
		t.Fatalf("stray approval emitted %d outbox rows", n)
	}
}

func TestApprovalForUnknownWorkflowIsTransient(t *testing.T) {
	// The changefeed may deliver the trigger after the approval; retrying is the right answer.
	svc := newService(newFakeStore(), nil, Config{})
	err := svc.ProcessEvent(context.Background(), approvalBytes(t, "a"), true)
	if !errors.Is(err, domain.ErrTransient) {
		t.Fatalf("err = %v, want ErrTransient", err)
	}
}

func TestFailWorkflowIsTerminalAndExactlyOnce(t *testing.T) {
	store := newFakeStore()
	svc := newService(store, nil, Config{})
	ctx := context.Background()
	if err := svc.ProcessEvent(ctx, triggerBytes(t), false); err != nil {
		t.Fatal(err)
	}

	if err := svc.FailWorkflow(ctx, testEventID, "draft_po", "model refused"); err != nil {
		t.Fatalf("FailWorkflow: %v", err)
	}
	wf := store.state(testEventID)
	if wf.State != domain.StateFailed {
		t.Fatalf("state = %s, want FAILED", wf.State)
	}
	if wf.OwnerPod != "" || !wf.LeaseExpiresAt.IsZero() {
		t.Fatal("lease not cleared on failure")
	}
	types := store.outboxTypes()
	if types[len(types)-1] != "WorkflowFailed" {
		t.Fatalf("last outbox row = %v, want WorkflowFailed", types)
	}
	var p failurePayload
	if err := json.Unmarshal(store.outbox[len(store.outbox)-1].payload, &p); err != nil || p.Reason != "model refused" || p.Node != "draft_po" {
		t.Fatalf("WorkflowFailed payload = %+v (%v)", p, err)
	}

	// Second failure (a duplicate dead-letter) must not emit again or change anything.
	if err := svc.FailWorkflow(ctx, testEventID, "draft_po", "again"); err != nil {
		t.Fatalf("second FailWorkflow: %v", err)
	}
	if got := store.outboxTypes(); len(got) != len(types) {
		t.Fatalf("second FailWorkflow emitted: %v", got)
	}

	// And a late approval for a failed workflow is a stray, not a resurrection.
	if err := svc.ProcessEvent(ctx, approvalBytes(t, "late"), true); err != nil {
		t.Fatal(err)
	}
	if store.state(testEventID).State != domain.StateFailed {
		t.Fatal("a late approval resurrected a FAILED workflow")
	}
}

func TestFailWorkflowNeverFlipsCompleted(t *testing.T) {
	store := newFakeStore()
	svc := newService(store, nil, Config{})
	ctx := context.Background()
	if err := svc.ProcessEvent(ctx, triggerBytes(t), false); err != nil {
		t.Fatal(err)
	}
	if err := svc.ProcessEvent(ctx, approvalBytes(t, "a"), true); err != nil {
		t.Fatal(err)
	}
	if err := svc.FailWorkflow(ctx, testEventID, "", "dead-lettered after completion"); err != nil {
		t.Fatal(err)
	}
	if st := store.state(testEventID).State; st != domain.StateCompleted {
		t.Fatalf("FailWorkflow flipped COMPLETED to %s", st)
	}
}

func TestReapExpiredApprovalsFailsTimedOutAndSkipsContended(t *testing.T) {
	store := newFakeStore()
	svc := newService(store, nil, Config{HITLLeaseTTL: time.Hour})
	ctx := context.Background()

	park := func(eventID string, expired bool) string {
		wf, err := store.LoadOrCreateWorkflow(ctx, eventID, testTrace, 1, []string{"draft_po", "human_approval", "final_step"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		wf.State = domain.StateSuspended
		wf.CurrentNodeIndex = 1
		wf.LeaseExpiresAt = time.Now().Add(time.Hour)
		if expired {
			wf.LeaseExpiresAt = time.Now().Add(-time.Minute)
		}
		if err := store.SaveCheckpoint(ctx, &fakeTx{}, wf, "", 0, nil); err != nil {
			t.Fatal(err)
		}
		return wf.ID
	}
	park("00000000-0000-0000-0000-000000000001", true)
	park("00000000-0000-0000-0000-000000000002", false)
	contendedID := park("00000000-0000-0000-0000-000000000003", true)
	store.contended[contendedID] = true

	n, err := svc.ReapExpiredApprovals(ctx, 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want 1 (one expired + unlocked; one fresh; one expired but locked elsewhere)", n)
	}
	if st := store.state("00000000-0000-0000-0000-000000000001").State; st != domain.StateFailed {
		t.Fatalf("expired workflow is %s, want FAILED", st)
	}
	if st := store.state("00000000-0000-0000-0000-000000000002").State; st != domain.StateSuspended {
		t.Fatalf("fresh workflow is %s, want still SUSPENDED", st)
	}
	if st := store.state("00000000-0000-0000-0000-000000000003").State; st != domain.StateSuspended {
		t.Fatalf("contended workflow is %s, want untouched this sweep", st)
	}
	// Next sweep, lock released: it goes too.
	delete(store.contended, contendedID)
	if n, _ := svc.ReapExpiredApprovals(ctx, 100); n != 1 {
		t.Fatalf("second sweep reaped %d, want 1", n)
	}
}

type failingExecutor struct{ err error }

func (f failingExecutor) Execute(context.Context, domain.NodeRequest) (*domain.NodeResult, error) {
	return nil, f.err
}

func TestNodeExecutionErrorIsReturnedNotSwallowed(t *testing.T) {
	store := newFakeStore()
	boom := fmt.Errorf("%w: model returned garbage", domain.ErrTerminal)
	svc := newService(store, map[string]ports.NodeExecutor{"draft_po": failingExecutor{boom}}, Config{})

	err := svc.ProcessEvent(context.Background(), triggerBytes(t), false)
	if !errors.Is(err, domain.ErrTerminal) {
		t.Fatalf("err = %v, want the executor's terminal error surfaced to the consumer", err)
	}
	if st := store.state(testEventID).State; st != domain.StatePending {
		t.Fatalf("a failed node checkpointed the workflow as %s", st)
	}
	if n := len(store.outboxTypes()); n != 0 {
		t.Fatalf("a failed node emitted %d outbox rows", n)
	}
}
