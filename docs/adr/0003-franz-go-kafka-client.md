# ADR 0003 — franz-go as the Kafka client

- **Status:** Accepted
- **Date:** 2026-07 (recorded here 2026-09-22)

## Context

The services started on `confluent-kafka-go`, which wraps `librdkafka` through cgo.

## Decision

**franz-go** (pure Go) everywhere, and every binary builds with `CGO_ENABLED=0`.

## Consequences

**What it buys.** Static binaries, which is what lets the runtime image be
`gcr.io/distroless/static:nonroot` — no shell, no libc, no package manager, nothing to patch but our
own code. Cross-compilation is a `GOARCH` setting rather than a toolchain problem, so the multi-arch
release build costs seconds instead of running the whole compile under QEMU. No C toolchain in CI.

**What it costs.** A smaller ecosystem than librdkafka's, and behaviour that differs in places where
librdkafka's defaults are the folklore everyone repeats. One of those cost a full CI round:

> **franz-go does not set `allow_auto_topic_creation` in its Metadata request** unless you pass
> `kgo.AllowAutoTopicCreation()`. The broker's `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` is therefore
> **inert for every Go client here**. The tell that isolated it: `omniflow.orchestration.v1` existed
> (created by CockroachDB's *Java* changefeed sink, which does request creation) while the topics
> only franz-go touched did not.

The response was not to pass the option but to create all ten topics explicitly in
`infrastructure/init/kafka-init.sh`, because deterministic topology beats implicit creation with
broker-default partitions — and because the `.dlq` topics are the ones that matter: every consumer
withholds its offset commit until dead-letter delivery is confirmed, so a missing `.dlq` wedges that
partition permanently on the first poison message while every happy-path job stays green.

`kgo.DisableAutoCommit()` is mandatory in every consumer, and `MarkCommitRecords` is a **no-op**
unless the client was built with `AutoCommitMarks` — one service committed zero offsets for its
entire lifetime because of that, invisible because its idempotency table absorbed the replays.

## Guarded by

`scripts/failtest_dlq_poison.sh` (both a wire-level and a semantic poison pill reach the DLQ and the
partition keeps flowing), `internal/platform/kafkaconf` tests, and the `build` job's
`CGO_ENABLED=0 go build ./...` on both modules.
