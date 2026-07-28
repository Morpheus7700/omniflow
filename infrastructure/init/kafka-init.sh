#!/usr/bin/env bash
set -eo pipefail

# kafka-init creates every topic the system produces to or consumes from, BEFORE any service starts.
#
# WHY THIS EXISTS (do not delete and fall back on broker auto-create):
#   `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` is set on the broker, but that flag only takes effect
#   when a CLIENT asks for creation in its Metadata request. franz-go does NOT ask unless you pass
#   `kgo.AllowAutoTopicCreation()`, and no service here passes it. So for every Go client the broker
#   setting is inert. This was a real CI failure — the seeder died with
#     "produce to omniflow.p2p.approval.v1: UNKNOWN_TOPIC_OR_PARTITION"
#   while omniflow.orchestration.v1 worked fine, because the latter is created by CockroachDB's
#   changefeed sink (a Java client that DOES request auto-creation) and the former only by franz-go.
#
#   The subtler reason: the DLQ topics. Every consumer withholds its offset commit until DLQ delivery
#   is confirmed (that ordering is load-bearing and LOCKED). With no .dlq topic in existence the first
#   poison message wedges its partition forever. The happy-path E2E never exercises it, so nothing
#   would have caught this until production.
#
# This runs in the apache/kafka:3.8.0 JVM image, which ships the CLI scripts AND the JRE to run them
# (the kafka-native image does not — see the note on the kafka service in docker-compose.yml).
# Readiness is already gated by compose (`depends_on: { kafka: service_healthy }`), so there are no
# wait loops here — same reasoning as crdb-init.sh.

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
TOPICS_CLI="/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP}"

# Single broker: replication factor MUST be 1. RF>1 fails outright with the same
# "not enough replicas" class of error that forced KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1.
# One partition keeps per-key ordering trivially total, which the HLC sequence_engine_key
# assertions in the E2E and failtest scripts rely on.
RF=1
PARTITIONS=1

# Every topic in the system. Keep this list in sync with:
#   producers : tools/seed/main.go, the 3 changefeeds in crdb-init.sh, the 3 consumers' DLQ paths
#   consumers : the kgo.ConsumeTopics(...) calls in each service's cmd/main.go
TOPICS=(
  # CommBot ingress (seed `email` mode) + its dead-letter sink
  "omniflow.communication.v1"
  "omniflow.communication.v1.dlq"

  # commbot_outbox changefeed -> p2p-orchestrator, + the orchestrator's dead-letter sink
  "omniflow.orchestration.v1"
  "omniflow.orchestration.v1.dlq"

  # Human-in-the-loop approval: produced by the seeder, consumed by p2p-orchestrator to resume the DAG.
  # Its own dead-letter sink: the orchestrator consumes two topics carrying two different wire
  # formats (JSON changefeed envelope here, bare protobuf there), so they cannot share a DLQ —
  # a re-drive tool would have no way to know how to decode a given record.
  "omniflow.p2p.approval.v1"
  "omniflow.p2p.approval.v1.dlq"

  # orchestrator_outbox changefeed -> viz-gateway
  "omniflow.p2p.completed.v1"

  # External inventory movement ingress (raw protobuf) + its dead-letter sink
  "omniflow.inventory.movement.v1"
  "omniflow.inventory.movement.v1.dlq"

  # fact_inventory_* changefeed (topic_prefix=omniflow.inventory.) -> viz-gateway
  "omniflow.inventory.fact_inventory_movement"
  "omniflow.inventory.fact_inventory_snapshot"
)

echo "Creating ${#TOPICS[@]} topics on ${BOOTSTRAP} (rf=${RF}, partitions=${PARTITIONS})..."
for topic in "${TOPICS[@]}"; do
  # --if-not-exists makes this idempotent, so a re-run (or a restarted stack) is a no-op rather
  # than a hard failure under `set -e`.
  $TOPICS_CLI --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$RF"
  echo "  ok: $topic"
done

# Print the realized topology so CI logs show what actually exists, not just what we asked for.
echo "Realized topic list:"
$TOPICS_CLI --list

echo "kafka-init complete."
# Reaching here under `set -e` means every topic was created or already present. Exit 0 explicitly
# so the container's status can't inherit a stray non-zero from the last command.
exit 0
