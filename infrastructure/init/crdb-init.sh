#!/usr/bin/env bash
set -eo pipefail

# crdb-init runs in a SEPARATE cockroachdb/cockroach container (the CLI binary only — there is no
# node in this container, and no Kafka tooling). Therefore:
#   * every `cockroach sql` MUST target the cockroachdb service over the network (--host), otherwise
#     it defaults to localhost:26257 inside THIS container and hangs forever.
#   * readiness of CockroachDB *and* Kafka is already gated by compose:
#     `depends_on: { cockroachdb: service_healthy, kafka: service_healthy }`, so no in-script wait
#     loops are needed (the previous Kafka wait shelled out to /opt/kafka/... which does not exist
#     in this image and looped forever).
CRDB="cockroach sql --insecure --host=cockroachdb:26257"

# License is OPTIONAL. Under CockroachDB v24.3+ licensing, a single-node cluster
# (`start-single-node`) needs no license key, so changefeeds are attempted regardless. If a key IS
# supplied (CRDB_LICENSE), we apply it; if not, we proceed license-free. Should changefeed creation
# below fail with a licensing error, crdb-init exits non-zero and CI fails LOUDLY (never a silent skip)
# — the signal to add a free CRDB_LICENSE and re-run.
echo "Setting cluster parameters..."
$CRDB -e "SET CLUSTER SETTING kv.rangefeed.enabled = true;"
if [ -n "${CRDB_LICENSE:-}" ]; then
    echo "CRDB_LICENSE supplied — applying enterprise license (org=${CRDB_ORG})."
    $CRDB -e "SET CLUSTER SETTING cluster.organization = '${CRDB_ORG}';"
    $CRDB -e "SET CLUSTER SETTING enterprise.license = '${CRDB_LICENSE}';"
else
    echo "No CRDB_LICENSE — proceeding license-free (single-node, v24.3+). Changefeed creation is the test."
fi

echo "Creating omniflow database if not exists..."
$CRDB -e "CREATE DATABASE IF NOT EXISTS omniflow;"

echo "Applying pure DDL schema files..."
$CRDB -d omniflow -f /infrastructure/crdb_schema.sql
$CRDB -d omniflow -f /infrastructure/storage/orchestrator_schema.sql
$CRDB -d omniflow -f /infrastructure/storage/commbot_outbox_schema.sql

# Idempotently create a changefeed only if no running/pending job already targets it.
# SHOW CHANGEFEED JOBS exposes `description` (the full CREATE statement) + `status`; there is NO
# `statement` column — matching on `description` is what actually works.
create_changefeed() {
    local target="$1"
    local job_match="$2"
    local create_stmt="$3"

    echo "Checking changefeed for: $target"
    local job_status
    job_status=$($CRDB -d omniflow --format=csv -e \
        "SELECT status FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%${job_match}%' ORDER BY created DESC LIMIT 1" \
        | tail -n 1)

    if [[ "$job_status" == "running" ]] || [[ "$job_status" == "pending" ]]; then
        echo "Changefeed for $target is already '$job_status'. Skipping creation."
    else
        echo "Creating changefeed for $target..."
        $CRDB -d omniflow -e "$create_stmt"
    fi
}

create_changefeed "fact_inventory tables" "omniflow.inventory." "
CREATE CHANGEFEED FOR TABLE fact_inventory_movement, fact_inventory_snapshot
  INTO 'kafka://kafka:29092?topic_prefix=omniflow.inventory.&tls_enabled=false'
  WITH updated, resolved='1s', mvcc_timestamp, format='json',
       min_checkpoint_frequency='1s',
       protect_data_from_gc_on_pause;
"

create_changefeed "orchestrator_outbox" "omniflow.p2p.completed.v1" "
CREATE CHANGEFEED FOR TABLE orchestrator_outbox
  INTO 'kafka://kafka:29092?topic_name=omniflow.p2p.completed.v1&tls_enabled=false'
  -- key_column + unordered dropped: CRDB rejects unordered together with resolved, and the viz
  -- rendering gate depends on the resolved watermark, so resolved wins. Custom keying is moot on this
  -- single-broker/single-partition dev Kafka (ordering is preserved regardless of message key).
  WITH
    format                   = 'json',
    updated,
    mvcc_timestamp,
    resolved                 = '1s',
    min_checkpoint_frequency = '1s',
    protect_data_from_gc_on_pause;
"

create_changefeed "commbot_outbox" "omniflow.orchestration.v1" "
CREATE CHANGEFEED FOR TABLE commbot_outbox
  INTO 'kafka://kafka:29092?topic_name=omniflow.orchestration.v1&tls_enabled=false'
  -- key_column + unordered dropped (see orchestrator_outbox above): incompatible with resolved.
  WITH
    format                    = 'json',
    updated,
    mvcc_timestamp,
    resolved                  = '1s',
    min_checkpoint_frequency  = '1s',
    protect_data_from_gc_on_pause;
"

echo "crdb-init complete."
