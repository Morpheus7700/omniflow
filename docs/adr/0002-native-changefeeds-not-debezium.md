# ADR 0002 — CockroachDB native changefeeds, not Debezium

- **Status:** Accepted
- **Date:** 2026-07 (recorded here 2026-09-22)

## Context

Every service must publish events without a dual write: writing the business row to the database and
the event to Kafka in two steps means a crash between them either loses the event or invents one.
The transactional outbox pattern fixes the write side — business row and outbox row in one
transaction — and then something has to move outbox rows to Kafka.

The default answer in 2026 is Debezium. The blueprint this project started from named it explicitly.

## Decision

CockroachDB's **native JSON changefeed** is the only bridge from the database to Kafka. No Debezium,
no connector runtime, no Kafka Connect cluster.

## Consequences

**Why it is not Debezium.** Debezium's CockroachDB story is a Postgres-protocol connector pointed at
a database that speaks the Postgres wire protocol but is not Postgres: it has no logical replication
slots, no `pg_replication_slots`, and no WAL in the shape the connector expects. Using it means
either an unsupported path or an extra always-on distributed system (Kafka Connect: workers,
offsets topic, status topic, its own failure modes) to do what the database already does natively
with `CREATE CHANGEFEED`.

**What we get.** Exactly-once *delivery semantics into Kafka* handled by the database; a `resolved`
watermark we can render (the settlement line is literally a changefeed guarantee drawn on screen);
one fewer deployable component; and a spine whose failure modes are CockroachDB's, which we already
have to understand.

**What it costs.**

- Vendor coupling. The CDC layer is CockroachDB-specific; moving databases means rewriting it.
- The emitted shape is CockroachDB's, not a schema we chose: `{"after": {…columns…}}`, BYTES as a
  `\x`-hex string, and `resolved` messages interleaved with rows. Every consumer has to know that,
  and getting it wrong is the repo's single most recurring bug (see `docs/kb/05-gotchas.md`).
- Changefeed options are the database's: `topic_prefix` and `topic_name` are sink-URI parameters, not
  `WITH` options, and `unordered` cannot coexist with `resolved` — both discovered the hard way.
- On a multi-node cluster this needs an Enterprise license. Single-node (what compose runs) does not.

## Guarded by

`infrastructure/init/crdb-init.sh` creates the three changefeeds and fails loudly; `scripts/e2e.sh`
asserts at least three are *running* before it seeds; `services/viz-gateway/internal/kafka/consumer_test.go`
pins the envelope decode, including that a top-level payload is dropped rather than projected empty.
