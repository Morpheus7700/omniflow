# ADR 0004 — `sequence_engine_key` is a STRING past the database boundary

- **Status:** Accepted
- **Date:** 2026-07 (recorded here 2026-09-22)

## Context

Ordering across services is established by a hybrid logical clock. The key is a 64-bit integer —
`1790023750749228000` is a real one from a CI run: 19 digits.

JavaScript's `Number` is a float64 with 53 bits of integer precision. `JSON.parse` on that value
yields `1790023750749227800`. Silently. Two adjacent events collapse onto the same key, sort order
becomes arbitrary, and the settlement line — which is *defined* as a comparison against this key —
starts lying while every service reports success.

## Decision

The key is carried as a **decimal string** in every payload that crosses the database boundary: the
SSE frame, the replay response, the TypeScript type. It is parsed to `BigInt` in the browser for
comparison and sorting, never to `Number`.

## Consequences

- The gateway decodes changefeed JSON with `json.Decoder.UseNumber()`, so the value never becomes a
  float64 even in transit through Go. A default decode corrupts it before it is re-serialised.
- The frontend sorts and compares with `BigInt`, and `store/index.ts` rejects an event whose key will
  not parse rather than letting it reach a `BigInt()` call inside render.
- The column stays `INT8` in CockroachDB — this is a wire-format decision, not a storage one.
- It reads as over-engineering until you see the failure, which is why this ADR exists: the bug
  produces no error anywhere, only a dashboard that is subtly wrong.

## Guarded by

`services/viz-gateway/internal/kafka/consumer_test.go` (19-digit fidelity through the real SSE
broker), `state_store_integration_test.go` (fidelity through real SQL), and
`frontend/src/store/index.test.ts`, which asserts two keys differing in the last digit stay distinct
and correctly ordered.
