---
paths:
  - "services/*/internal/**"
  - "services/viz-gateway/**"
  - "internal/platform/**"
  - "tools/**"
---

# Error Handling (Go)

- Never discard an error. No bare `_ = doThing()`, no empty `if err != nil {}`. If ignoring is
  genuinely correct, say why in a comment on that line.
- Wrap with context using `%w`, never `%v`: `fmt.Errorf("load dag %s: %w", id, err)`. The message
  names the operation and its key; it does not repeat the wrapped text.
- Classify database and network failures through `internal/platform/errclass` — the one SQLSTATE
  taxonomy for the root module. Do not hand-roll a second SQLSTATE switch. `errclass.Classify`
  returns `Transient` (retry with backoff), `Terminal` (route to DLQ now), or `Unknown`.
  `Unknown` is treated as terminal-and-alert on purpose: an unnamed error has an unbounded retry
  cost and retrying it blocks the partition. Don't quietly fold it into `Transient`.
- `services/viz-gateway` is a separate module and cannot import `internal/platform/errclass`.
  Don't add an import that won't compile; keep its classification local.
- **Consumer ordering is load-bearing.** Confirm the DLQ produce *before* committing the offset,
  and keep manual commits. Never switch to auto-commit, and never fire-and-forget a produce — a
  commit that outruns its DLQ write loses the message silently.
- Sentinel errors compare with `errors.Is`; typed errors extract with `errors.As`. Never match on
  `err.Error()` string contents.
- Log the error once, at the boundary that decides what to do about it. Wrapping and logging at
  every level produces the same failure five times in the log and hides which layer handled it.
- Never log secrets, connection strings, or payload bodies. Log the key and the `errclass.Class`.
- HTTP/SSE surfaces return a consistent shape and correct status codes (400 validation, 404 not
  found, 500 unexpected). Never return a raw database error or stack trace to a browser.
