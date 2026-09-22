#!/usr/bin/env bash
# scripts/lib.sh — the shared harness every boot proof is built on. Source it; never execute it.
#
# Each proof has the same skeleton: boot the stack, gate on kafka-init and crdb-init, resolve the
# CockroachDB container once, drive events, assert against SQL, and tear down with logs on failure.
# That skeleton used to be copied into six scripts, and it drifted — one script lacked the Windows
# path fix, one still grepped logs for readiness, one dumped logs for two services only. Every hard-won
# rule below (see docs/kb/05-gotchas.md) now lives in exactly one place.
#
# Contract for a proof script:
#   export COMPOSE_PROJECT_NAME="omniflow-<proof>"     # isolates it from any other compose project
#   source "$(dirname "$0")/lib.sh"
#   boot_stack                                           # up + init gates + CRDB id + services healthy
#   ...assertions using crdb / wait_sql / wait_state...
# Teardown is automatic (EXIT trap). Exit non-zero and the full compose logs land in .proof-logs/.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo "scripts/lib.sh is a library — source it from a proof script" >&2
    exit 64
fi

set -euo pipefail

# Git Bash / MSYS2 on Windows rewrites arguments that look like absolute POSIX paths into Windows
# paths before the process ever sees them, so `docker compose exec -T kafka /opt/kafka/bin/...`
# arrives inside the container as `C:/Program Files/Git/opt/kafka/bin/...` and exits 127. The path
# is meaningful in the CONTAINER, not on the host, so the rewrite is always wrong here. On Linux and
# macOS the variable is ignored, which is why CI never saw the bug.
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

: "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME before sourcing scripts/lib.sh}"
export COMPOSE_PROJECT_NAME

# Full container logs on failure go here, one file per proof, so CI can upload them as an artifact.
# `--tail` on stdout was the only record before, and a container that logged more than that before
# dying was unrecoverable.
PROOF_LOG_DIR="${PROOF_LOG_DIR:-$ROOT/.proof-logs}"

# CRDB_LICENSE is OPTIONAL: single-node CockroachDB v24.3+ runs Kafka-sink changefeeds license-free.
# If a key is set it flows through to crdb-init; if not, changefeed creation is itself the test.
# There is no silent skip — a licensing failure is a hard non-zero exit.

log()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
fail() { printf '\n\033[1;31m✗ %s\033[0m\n' "$*"; }

SSE_PID=""    # so the EXIT trap is safe under set -u before any stream is opened
CRDB_CID=""   # resolved once by boot_stack; every query goes through it
_TMPFILES=()

tmpfile() {
    local f
    f="$(mktemp)"
    _TMPFILES+=("$f")
    printf '%s' "$f"
}

# Every CockroachDB query goes through crdb(), and BOTH the per-call `timeout` and the pre-resolved
# container id matter:
#   1. `docker compose exec` blocked indefinitely — a job twice sat at its 25-minute cap (which GitHub
#      reports as "cancelled", NOT "failure"), leaving docker/tail/tr as orphans.
#   2. A deadline loop did not rescue it: a deadline is re-checked only BETWEEN iterations, so one
#      stuck call pins the loop forever. Each call needs its own timeout.
#   3. With timeouts in place the hang became measurable — every compose-exec call burned its full
#      20s while cockroachdb stayed healthy. The fault was compose's project/state resolution, so use
#      plain `docker exec` against an id resolved once.
# `tail -n +2` drops the CSV header FIRST: a zero-row result otherwise returns the header text (the
# literal "state"), which reads as data. Header-first, then last row: no rows yields "".
# Returns the last CSV cell, whitespace/CR stripped; empty string on timeout or error.
crdb() {
    [[ -n "$CRDB_CID" ]] || return 0
    timeout 20s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv -e "$1" 2>/dev/null \
      | tail -n +2 | tail -n 1 | tr -d '[:space:]' || true
}

# Called only on failure. crdb() swallows stderr to keep polls quiet, which makes an erroring query
# indistinguishable from one returning no rows. These probes separate the possibilities: container
# state, bare exec plumbing, whether the SQL layer answers at all, open transactions (a long-lived one
# blocks reads of its row), a different table, and finally the failing query with stderr visible.
crdb_diag() {
    echo "---- docker compose ps ----"
    timeout 30s docker compose ps || echo "(compose ps failed/timed out)"
    echo "---- bare exec plumbing check ----"
    timeout 15s docker exec "$CRDB_CID" echo alive || echo "(docker exec failed/timed out: $?)"
    echo "---- SELECT 1 (is the SQL layer alive at all?) ----"
    timeout 15s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv -e "SELECT 1;" || echo "(exit $?)"
    echo "---- open transactions ----"
    timeout 15s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv \
        -e "SELECT id, application_name, start, num_stmts FROM [SHOW CLUSTER TRANSACTIONS];" || echo "(exit $?)"
    echo "---- a DIFFERENT table (is the block row-specific?) ----"
    timeout 15s docker exec "$CRDB_CID" \
        cockroach sql --insecure -d omniflow --format=csv \
        -e "SELECT count(*) FROM orchestrator_outbox AS OF SYSTEM TIME '-30s';" || echo "(exit $?)"
    if [[ -n "${SEED_EVENT_ID:-}" ]]; then
        echo "---- AOST query (the form the polls use) with stderr visible ----"
        timeout 20s docker exec "$CRDB_CID" \
            cockroach sql --insecure -d omniflow --format=csv \
            -e "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${SEED_EVENT_ID}';" || echo "(exit $?)"
    fi
    if [[ $# -gt 0 ]]; then
        echo "---- failing query with stderr visible ----"
        timeout 20s docker exec "$CRDB_CID" \
            cockroach sql --insecure -d omniflow --format=csv -e "$1" || echo "(exit $?)"
    fi
}

_harness_cleanup() {
    local code=$?
    [[ -n "$SSE_PID" ]] && kill "$SSE_PID" 2>/dev/null || true
    if [[ "$code" -ne 0 ]]; then
        fail "${COMPOSE_PROJECT_NAME} failed (exit $code)"
        mkdir -p "$PROOF_LOG_DIR"
        local logfile="$PROOF_LOG_DIR/${COMPOSE_PROJECT_NAME}.log"
        docker compose logs --no-color > "$logfile" 2>&1 || true
        echo "===== last 200 lines of container logs (full file: ${logfile#"$ROOT"/}) ====="
        tail -n 200 "$logfile" || true
        echo "===== end container logs ====="
    fi
    log "Tearing down ${COMPOSE_PROJECT_NAME}"
    docker compose down -v --remove-orphans || true
    if (( ${#_TMPFILES[@]} )); then rm -f "${_TMPFILES[@]}"; fi
    exit "$code"
}
trap _harness_cleanup EXIT

# One-shot init containers (kafka-init, crdb-init) have restart:no and exit 0 only on full success.
# `docker compose wait` adopts the container's exit code as its OWN status, so under `set -e` a
# non-zero init would abort here before we could report it. Block for the stop, then read the true
# exit code with docker inspect and branch on that. Log-grepping for a completion line is not used:
# it is fragile (the line is "crdb-init complete.", not "Init complete") and it cost a round once.
wait_init() {
    local svc="$1" cid code
    log "Waiting for ${svc} to complete"
    docker compose wait "$svc" >/dev/null 2>&1 || true
    cid="$(docker compose ps -aq "$svc")"
    code="$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null || echo unknown)"
    echo "${svc} exit code: ${code}"
    if [[ "$code" != "0" ]]; then
        fail "${svc} exited non-zero (${code})"
        docker compose logs --no-color "$svc" || true
        exit 1
    fi
}

# Block until each named service's compose healthcheck reports healthy. The images are distroless,
# so the healthcheck is the binary's own `-probe` against its /readyz — 503 until the pgx pool is
# live AND the Kafka consumers are wired. A service with no healthcheck is a hard failure, not a
# pass: `depends_on: service_healthy` silently means `service_started` for such a service.
wait_healthy() {
    local svc cid status deadline
    for svc in "$@"; do
        deadline=$(( SECONDS + ${HEALTHY_TIMEOUT:-120} ))
        while :; do
            cid="$(docker compose ps -q "$svc")"
            status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo unknown)"
            case "$status" in
                healthy) echo "  ${svc}: healthy"; break ;;
                none|unknown)
                    fail "${svc} reports no healthcheck — the compose healthcheck is missing or the image has no /svc"
                    exit 1 ;;
            esac
            if (( SECONDS >= deadline )); then
                fail "${svc} never became healthy (last status: ${status})"
                docker compose logs --no-color --tail 50 "$svc" || true
                exit 1
            fi
            sleep 2
        done
    done
}

# Boot everything, gate on both init jobs (kafka-init first: crdb-init depends on it, so a topic
# failure would otherwise surface as a confusing crdb-init stall), resolve the CockroachDB container
# id once, and wait for the four app services to be READY — not merely started. Before the services
# had real healthchecks this was a `sleep 5` in every script, which hid the difference between "slow
# to connect" and "never came up".
boot_stack() {
    log "Building + starting the stack (${COMPOSE_PROJECT_NAME})"
    docker compose up -d --build
    wait_init kafka-init
    wait_init crdb-init
    CRDB_CID="$(docker compose ps -q cockroachdb)"
    if [[ -z "$CRDB_CID" ]]; then
        fail "could not resolve the cockroachdb container id"
        docker compose ps || true
        exit 1
    fi
    echo "cockroachdb container: ${CRDB_CID:0:12}"
    log "Waiting for the app services to report READY"
    wait_healthy commbot inventory-intelligence p2p-orchestrator viz-gateway
}

# Poll a crdb() query until its single cell equals $2, for at most $3 seconds (default 60).
# Sets SQL_LAST to the last observed value so the caller can report it. Returns 1 on timeout.
# A plain deadline loop, deliberately NOT `timeout N bash -c '…'`: smuggling a quoted SQL literal
# through a single-quoted bash -c needs '"'"' escaping that once silently never expired and ran a
# job to its 25-minute cap. Here the SQL's single quotes sit harmlessly inside double quotes.
wait_sql() {
    local query="$1" want="$2" deadline
    deadline=$(( SECONDS + ${3:-60} ))
    SQL_LAST=""
    while (( SECONDS < deadline )); do
        SQL_LAST="$(crdb "$query")"
        [[ "$SQL_LAST" == "$want" ]] && return 0
        sleep 2
    done
    return 1
}

# Poll a workflow (by event_id) until it reaches state $2. Reads are historical
# (AS OF SYSTEM TIME '-30s') to avoid blocking on the orchestrator's write intents, so a row created
# moments ago is not visible yet — polling absorbs that lag; a one-shot read raced it.
# Always poll for the terminal condition, never a proxy: an SSE event is emitted by the FIRST
# payload-bearing checkpoint and proves the workflow RESUMED, not that it FINISHED.
wait_state() {
    local event_id="$1" want="$2" secs="${3:-150}"
    if wait_sql "SELECT state FROM workflows AS OF SYSTEM TIME '-30s' WHERE event_id='${event_id}';" "$want" "$secs"; then
        return 0
    fi
    fail "workflow ${event_id} never reached ${want} (last state: '${SQL_LAST}')"
    crdb_diag "SELECT event_id, state FROM workflows WHERE event_id='${event_id}';"
    return 1
}

# Extract SEED_EVENT_ID / SEED_SEQUENCE_ENGINE_KEY from seeder output and export both. The
# approve-only seed REQUIRES the sequence key (it produces the HumanApprovalEvent carrying the same
# HLC key and refuses to guess it), so a missing export fails here, not at the approval step.
seed_keys() {
    SEED_EVENT_ID="$(printf '%s\n' "$1" | grep '^SEED_EVENT_ID=' | cut -d= -f2)"
    SEED_SEQUENCE_ENGINE_KEY="$(printf '%s\n' "$1" | grep '^SEED_SEQUENCE_ENGINE_KEY=' | cut -d= -f2)"
    export SEED_EVENT_ID SEED_SEQUENCE_ENGINE_KEY
    if [[ -z "$SEED_EVENT_ID" || -z "$SEED_SEQUENCE_ENGINE_KEY" ]]; then
        fail "could not extract seed keys (event_id='${SEED_EVENT_ID}' seq_key='${SEED_SEQUENCE_ENGINE_KEY}')"
        exit 1
    fi
}

# Open the SSE stream into file $1 BEFORE triggering the event. The gateway broadcasts live to
# connected clients only — there is no backlog — so a late connect misses the projection.
# --max-time must comfortably exceed seed duration + assertion window: the seeder alone may poll 60s
# for SUSPENDED, so a tight ceiling kills the capture mid-assertion and reports "never reached the
# stream" for a stream that was simply no longer being read. The EXIT trap kills it regardless.
open_sse() {
    local out="$1"
    log "Opening SSE stream: ${SSE_URL:-http://localhost:8081/api/stream}"
    curl -sN --max-time "${SSE_MAX_TIME:-180}" "${SSE_URL:-http://localhost:8081/api/stream}" > "$out" &
    SSE_PID=$!
    sleep 3  # allow the client to register with the broker
}

# Wait up to $3 seconds (default 40) for $2 to appear in SSE capture file $1.
# 40s, not 20s: after approval the DAG still has nodes to execute before the outbox row exists, CI
# runners are slow, and a real run measured 17s — 20s windows are one second from a permanent flake.
wait_sse() {
    local out="$1" needle="$2" deadline
    deadline=$(( SECONDS + ${3:-40} ))
    while (( SECONDS < deadline )); do
        if grep -q -- "$needle" "$out"; then return 0; fi
        sleep 1
    done
    fail "${needle} never reached the SSE stream"
    echo "---- captured SSE output ----"; cat "$out" || true
    return 1
}
