# ADR 0001 — Server-Sent Events, not WebSocket, for the browser transport

- **Status:** Accepted
- **Date:** 2026-07 (recorded here 2026-09-22)

## Context

The dashboard needs live projection events. The obvious reach is a WebSocket; the repo's KB asserted
"SSE by deliberate ADR" for months while `docs/adr/` contained only a WebSocket *upgrade spec* — the
decision itself was written down nowhere, which is how a locked decision quietly becomes folklore.

What the browser actually does with this stream: it receives ledger rows and settlement watermarks,
in order, and renders them. It sends nothing. Replay is a separate `GET` with a bounded row count.

## Decision

Server-Sent Events over HTTP, one endpoint (`GET /api/stream`), with replay as an ordinary `GET`.

## Consequences

**What this buys.**

- The stream is plain HTTP: ordinary proxies, ordinary logging, ordinary `curl`. The end-to-end proof
  asserts on the real wire with `curl -N` and no client library — that is not a convenience, it is
  what makes the proof trustworthy.
- Reconnection is the browser's job. `EventSource` retries on its own and replays `Last-Event-ID`, so
  the gap-recovery story is a header the gateway honours (it queries the replay repository from the
  key after that id) rather than a protocol to design. A WebSocket would put reconnect, backoff and
  resume in our code.
- No framing, no ping/pong, no subprotocol negotiation, no upgrade handshake to get wrong.

**What it costs, honestly.**

- The channel is one-way. Anything the client wants to *say* needs a second mechanism — today that is
  the replay `GET`, and the form in the rail is a URL, not a message. A feature that needs genuine
  bidirectional traffic (presence, server-pushed filters negotiated per client, collaborative cursors)
  would make this the wrong choice.
- HTTP/1.1 limits a browser to six connections per origin; SSE holds one open. Irrelevant for one
  dashboard tab, not irrelevant if the same origin ever serves many long-lived streams.
- Binary payloads would need base64. We send JSON.

**Revisit when** the client needs to send anything the URL cannot carry. The spec for that upgrade is
kept at `docs/specs/websocket-upgrade.md`, deliberately outside `docs/adr/` so it cannot be mistaken
for a decision.

## Guarded by

`services/viz-gateway/internal/api/sse_cap_test.go`, `sse_resume_test.go` (resume, eviction, retry
advertisement), `frontend/src/hooks/useEventBuffer.test.tsx` (no-gap/no-dup handover), and
`scripts/e2e.sh`, which asserts a seeded key reaches `/api/stream` on the real stack.
