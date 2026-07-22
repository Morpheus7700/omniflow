# OmniFlow Knowledge Base — Map of Content

> Obsidian-style vault. Entry point for **both** the Sentinel (Claude) and the Builder (Antigravity)
> to restore full context cheaply at the start of a session — read this first instead of re-deriving
> from code. Notes cross-link with `[[wikilinks]]`. Keep it current: when a fact changes, edit the
> note, don't append contradictions.

## Read order for a cold start
1. [[00-project-overview]] — what OmniFlow is and the mandate.
2. [[01-working-loop]] — the Builder/Sentinel protocol (who does what, the paste loop).
3. [[02-architecture]] — the event spine: topics, changefeeds, services, data flow.
4. [[03-locked-constraints]] — files/logic that must never regress.
5. [[04-progress-ledger]] — Prompt 1–6 status and what's next.
6. [[05-gotchas]] — hard-won findings. **Highest value note. Read before editing anything.**
7. [[06-build-and-test]] — build/vet/E2E commands and the seeder.
8. [[07-the-gate]] — the push + license blocker that's currently the critical path.

## Canonical sources (authoritative, this KB summarizes them)
- `docs/audit/STATE.md` — the running Sentinel state ledger (most detailed).
- `CLAUDE.md` — repo constraints auto-loaded into every Claude session.
- `docs/antigravity/PROMPT_0X_*.md` — the builder prompts, one per milestone.
- `docs/audit/COMPOSE_PLAN_AUDIT.md` — the round-1..3 compose audit trail.

## One-line status (update every session)
**As of 2026-07-23:** Prompts 1–6 built + audited + committed (`d351b96` on `master`). Nothing pushed
yet. Critical path = [[07-the-gate]] (human must push repo + set `CRDB_LICENSE`/`CRDB_ORG`). No CI run
has happened; all "survives failure" proofs are CI-only. Next build item after green = README/SCOPE.md,
then the deferred WebSocket upgrade (`docs/antigravity/WEBSOCKET_UPGRADE_SPEC.md`).
