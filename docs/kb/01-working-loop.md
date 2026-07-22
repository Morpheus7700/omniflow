# 01 · The Working Loop (Builder / Sentinel)

Two AI agents collaborate, with the human (Aniket) as pilot/overseer.

## Roles
- **Antigravity** (Google's agentic IDE, Gemini) = the **Builder**. It scaffolds and edits files on
  disk. It has NO Docker and cannot run the stack — it can only build/vet/syntax-check. Historically
  it has **verified syntax only and reported success**, so its "verified" ≠ "runs."
- **Claude** (this agent) = the **Principal Sentinel / Auditor**, and — since 2026-07-23 — the
  **lead engineer**. Claude audits from disk each round, patches contained bugs directly, and either
  builds the work itself or hands the Builder a precise prompt.

## The paste loop (how a round works)
1. Sentinel writes a builder prompt → `docs/antigravity/PROMPT_0X_*.md`.
2. Human pastes it into Antigravity; Antigravity edits files + replies with a walkthrough.
3. Human pastes Antigravity's reply back to the Sentinel.
4. **Sentinel audits FROM DISK — never from the pasted diff** (the report is often garbled or
   over-optimistic). Reads the real files, runs build/vet, checks assertions against the real schema.
5. Sentinel patches contained bugs itself, records the verdict in `docs/audit/STATE.md`, and issues
   the next prompt.

## Non-negotiable audit rules
- **Trust disk, not the report.** Every "build passes / verified" claim gets re-run.
- **Never fabricate a boot log.** If it can only be proven in CI, say so explicitly.
- **The Sentinel never commits during a round** — it leaves diffs for human review (the audit loop
  detects Antigravity's edits via `git status`/`git diff`). EXCEPTION: an explicit milestone commit
  the human authorizes (e.g. the push-ready commit `d351b96`).
- **Contained bugs → Sentinel patches directly.** Design-level or risky changes → back to a prompt.

## What Antigravity keeps getting wrong (audit these first every time)
See [[05-gotchas]] — service names, ports, changefeed envelope shape, aggregate_id vs event_id,
DECIMAL string comparisons. Every failtest round so far has shipped 2–4 of these.

Related: [[04-progress-ledger]] · [[03-locked-constraints]]
