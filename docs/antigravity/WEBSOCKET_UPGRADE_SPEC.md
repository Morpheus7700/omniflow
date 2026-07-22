# WebSocket Upgrade Spec (viz-gateway ⇄ frontend)

Status: **SPEC — do not build until E2E (Prompt 4) is green.** SSE is correct and works today; we do
not destabilize a proving pipeline. This becomes an Antigravity builder prompt only after the SSE
E2E gate passes in CI.

## Why WebSocket at all (the honest case)
SSE is the right transport for a pure server→client firehose: auto-reconnect, proxy-friendly, simple.
WebSocket earns its cost ONLY by enabling **client→server interactivity** that an MD-grade dashboard
wants and SSE can't do without a side REST channel:
1. **Live time-travel scrub** — drag a timeline; client pushes `{from_seq,to_seq}` and the server
   streams that window (today replay is a separate REST call in `internal/api/replay.go`).
2. **Server-side filtering** — subscribe to a vendor/SKU/stage subset; server sends only matches
   (less bandwidth, less client-side work on big graphs).
3. **Presence / multi-viewer** — show who else is watching and their cursor/focus (a boardroom demo
   with the presenter + remote viewers).
If we don't want (1)–(3), we KEEP SSE. This spec is only worth executing for those features.

## Design decision (locked for this spec)
**Single WebSocket** carrying a typed, bidirectional envelope — replaces the SSE `/api/stream` AND
folds in the replay REST endpoint. Rationale: one connection, one ordering domain, no SSE/REST
seam to keep consistent. Keep the REST replay endpoint temporarily for backward-compat, delete once
the frontend is migrated.

## Server (viz-gateway, own module — go 1.25)
- Library: `github.com/coder/websocket` (pure Go, no CGO — consistent with the franz-go choice).
  NOT gorilla (archived) unless a blocker surfaces.
- New `internal/api/ws.go`: `WSBroker` replacing `SSEBroker` responsibilities. Per-connection
  goroutine with a bounded send channel; on backpressure, apply the SAME policy as SSE today
  (2s deadline then drop the slow client — never block the fan-out; document it).
- **Message envelope (JSON, seqkey as STRING always):**
  - server→client: `{type:"projection", event:ProjectionEvent}`, `{type:"watermark", seq}`,
    `{type:"presence", viewers}`, `{type:"error", msg}`.
  - client→server: `{type:"replay", from_seq, to_seq}`, `{type:"filter", predicate}`,
    `{type:"resume", after_seq}` (reconnect continuity).
- **Ordering invariant preserved:** still gate rendering on the HLC `resolved` watermark; still order
  by `sequence_engine_key`; `resume.after_seq` lets a reconnecting client fetch the gap from the
  workflows/fact log (reuse `ReplayRepository.GetMovements`) then resubscribe live — the same
  subscribe-then-snapshot no-gap/no-dup discipline `useEventBuffer` already implements.
- Heartbeat: server ping every 15s; drop on 2 missed pongs. (SSE got this free; WS must do it.)
- Keep the franz-go consumer + confirmed-commit path untouched; only the fan-out layer changes.

## Frontend (`frontend/`, Next.js + R3F)
- Replace `EventSource` in `src/hooks/useEventBuffer.ts` with a `WebSocket` wrapper hook
  (`useSocket.ts`) that: dials `NEXT_PUBLIC_WS_URL`, auto-reconnects with backoff, on reopen sends
  `{type:"resume", after_seq:<last-rendered seqkey>}`, and preserves the buffer-then-snapshot flip so
  no event is dropped or doubled across a reconnect.
- HUD gains real controls: a scrub slider (emits `replay`), filter chips (emit `filter`), a presence
  badge. Numbers stay on the flat HUD, never in 3D (existing ADR).
- `docker-compose.yml`: swap the frontend build arg `NEXT_PUBLIC_SSE_URL` → `NEXT_PUBLIC_WS_URL`
  (`ws://localhost:8081/api/ws`).

## Verification (same bar as everything else)
Extend `.github/workflows/e2e.yml`: after seeding, open the WS from a tiny Go client, assert a
`projection` frame with the seeded seqkey arrives, then send a `replay` and assert the window streams
back. A real gate — not a screenshot claim. Optionally a Playwright screenshot of the moving node for
the PR (see [[frontend-visual-verify]]).

## Sequencing
E2E green on SSE (Prompt 4) → this spec → build → re-audit → migrate frontend → delete SSE + REST
replay seam. Do not start before the pipeline is proven.
