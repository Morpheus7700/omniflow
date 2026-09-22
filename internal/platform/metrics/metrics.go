// Package metrics holds the process-wide Prometheus instruments. They are registered on the
// default registry, which internal/platform/telemetry serves at /metrics on every service's health
// port. Names carry the omniflow_ prefix and follow the Prometheus naming guide (unit suffix,
// _total for counters).
//
// Cardinality is deliberately low: service, topic and outcome only. Never label by event id,
// workflow id or partition — an unbounded label set is a memory leak with a dashboard on top.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	OutcomeOK      = "ok"      // processed and offset committed
	OutcomeDLQ     = "dlq"     // dead-lettered (and offset committed after confirmation)
	OutcomeSkipped = "skipped" // resolved-timestamp / tombstone / duplicate — nothing to do
)

var (
	// ConsumerRecords counts every record a consumer finished with, by outcome. A rising dlq
	// series is the first alarm this system has ever had.
	ConsumerRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "omniflow_consumer_records_total",
		Help: "Kafka records processed, by final outcome.",
	}, []string{"service", "topic", "outcome"})

	// ConsumerRetries counts in-place retries of transient failures. Retries without a matching
	// rise in dlq means the backoff is absorbing blips; retries WITH it means it is not enough.
	ConsumerRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "omniflow_consumer_retries_total",
		Help: "In-place retries of transient processing failures.",
	}, []string{"service", "topic"})

	// WorkflowTransitions counts orchestrator state changes. SUSPENDED without a later COMPLETED
	// or FAILED is a purchase order waiting on a human.
	WorkflowTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "omniflow_workflow_transitions_total",
		Help: "Workflow state transitions checkpointed by the orchestrator.",
	}, []string{"state"})

	// ApprovalsExpired counts workflows the HITL sweep failed for want of an approval.
	ApprovalsExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "omniflow_approvals_expired_total",
		Help: "Workflows failed by the sweep because no approval arrived within the lease.",
	})

	// AgentSpend accumulates recorded model spend, so a budget dashboard needs no database read.
	AgentSpend = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "omniflow_agent_spend_micro_usd_total",
		Help: "Model spend recorded by the drafting agent, in micro-USD.",
	}, []string{"model", "outcome"})
)
