# Contributing to OmniFlow

Thanks for looking. This is a solo-maintained portfolio system, but it is run like a team repo:
everything lands through a pull request that the full suite has passed, and the suite is the
enforcement mechanism — not this document.

## Before you start

- Read `README.md` for what the system is, `SCOPE.md` for what it deliberately is not, and
  `docs/kb/INDEX.md` for how it is built. `docs/kb/05-gotchas.md` is the note to read before
  touching scripts, consumers or schema; nearly every recurring bug is catalogued there.
- Locked decisions (`CLAUDE.md` → "Locked decisions") are not up for re-litigation in a PR:
  CockroachDB native changefeeds (no Debezium), franz-go, SSE, one Next.js dashboard, `omniflow`
  as the database name, `sequence_engine_key` as a STRING end to end.

## Toolchain

| Tool | Version | Where it is pinned |
|---|---|---|
| Go | see `toolchain` in `go.mod` | the one pin; `go` auto-downloads it |
| Node | see `frontend/.nvmrc` | Active LTS; matches the frontend image |
| Docker + Compose v2 | any current | needed only for the boot proofs |
| golangci-lint | see `.github/workflows/e2e.yml` | `make lint` installs it |
| shellcheck | any ≥0.9 | `make lint` |

`make help` lists every target. `make check` is what CI's fast tier runs.

## The loop

1. Branch off an up-to-date `master`. `master` is protected: no direct pushes, `strict: true`
   (your branch must be current before it can merge), admins included.
2. Make the change. Keep it to one concern; the suite re-runs in full on every merge, so batching
   *related* changes into one PR is encouraged and unrelated ones are not.
3. Run `make check` (build, vet, race tests, lint, gofmt, frontend lint/typecheck) and, if you
   touched compose, scripts, Dockerfiles or a consumer, one of the boot proofs:
   `make e2e` or `bash scripts/failtest_*.sh`.
4. Open the PR. The template asks **which test proves the change is safe** — answer it. For a
   load-bearing path, `.github/CODEOWNERS` names the test that guards it. A change no test objects
   to is either safe or a gap in the suite; if it is the second, add the test in the same PR.
5. All required checks green → squash-merge. Approvals are not required (GitHub forbids
   self-approval on a solo repo, so requiring them would block every merge).

## What a good PR looks like here

- **Commit message** in the existing style: `type(scope): what changed, as a sentence`. The body
  explains *why*, and names the failure it prevents if there was one.
- **Tests verify behaviour, not implementation.** Integration tests run against a real
  CockroachDB via testcontainers; the image is read from `docker-compose.yml`, never restated.
- **Errors are never discarded.** Wrap with `%w`; classify DB/network errors through
  `internal/platform/errclass`; the confirmed-DLQ-before-commit ordering in every consumer is
  load-bearing.
- **No new frontend libraries without a decision.** Tailwind utilities and plain React.
- **Versions are pinned, and pins are surveilled.** Actions by SHA, images by digest, scanners by
  tag. Dependabot raises the bumps; if you add a dependency source, add it to
  `.github/dependabot.yml` too.
- **Docs move with code.** If a change invalidates something in `docs/kb/`, fix the note in the
  same PR — the vault is trusted precisely because it is usually right, which makes a stale line in
  it the most expensive kind.

## Reporting a security issue

See `SECURITY.md`. Please do not open a public issue for a vulnerability.

## Reporting a bug

Use the bug template. The single most useful thing you can include is the failing proof's
container logs — every boot job uploads them as a workflow artifact on failure.
