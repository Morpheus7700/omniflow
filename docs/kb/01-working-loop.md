# 01 · The Working Loop

Claude is the lead engineer: it edits the code, opens PRs, and audits its own work against CI. The
human (Aniket) is the overseer and decision-maker.

Earlier rounds ran a two-agent loop — Antigravity (Gemini) as builder, Claude as auditing Sentinel,
with the human pasting prompts between them. That is retired. The rules it produced are kept below
because they were learned the hard way and still apply to a single agent auditing itself.

## How a change lands

`master` is branch-protected: **9 required checks, `strict: true`, `enforce_admins: true`**. There
are no direct pushes, including for the repo owner. So:

1. Branch off an up-to-date `master`.
2. Build the change; verify locally as far as the machine allows (build, vet, `go test ./...`,
   `gofmt -l`, `bash -n`).
3. Open a PR. The PR body states **which test proves the change is safe** — see
   [[03-locked-constraints]].
4. All 9 checks must pass. `strict: true` means the branch must also be up to date with `master`, so
   a merge landing first forces an update and a re-run.
5. Self-merge (squash). Approvals are not required — requiring code-owner review would be
   self-blocking, since GitHub forbids self-approval.

Because every merge costs a full re-run, **batch related changes into one branch** rather than
opening many single-file PRs.

## Non-negotiable audit rules

- **Trust disk, not the report.** Every "build passes / verified" claim gets re-run. This applies to
  your own claims from earlier in a session as much as anyone else's.
- **Never fabricate a boot log.** If something can only be proven in CI, say so explicitly and wait
  for the run. A plausible-looking log you did not observe is the worst possible artifact.
- **A green check is not proof of the thing you think it proves.** Ask what the assertion actually
  exercises. The E2E job asserts a sequence key reaches SSE — which the *first* payload checkpoint
  already satisfies, so a workflow that stalls immediately afterwards still shows E2E green. That
  hid a real orchestrator self-deadlock for several rounds.
- **Read the SQLSTATE.** A deterministic error that gets classified as unknown looks exactly like a
  DLQ problem after five pointless retries. See [[05-gotchas]].
- **A `timeout-minutes` expiry is reported by GitHub as `cancelled`, not `failure`.** The tell is
  arithmetic: job duration == the timeout value means it hung.

## What keeps going wrong

See [[05-gotchas]] — service names, ports, changefeed envelope shape, `aggregate_id` vs `event_id`,
DECIMAL string comparisons, quoting SQL through `bash -c`. Nearly every round has shipped two to
four of these.

Related: [[04-progress-ledger]] · [[03-locked-constraints]] · [[05-gotchas]]
