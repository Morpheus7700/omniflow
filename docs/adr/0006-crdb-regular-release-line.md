# ADR 0006 — Track CockroachDB's Regular release line, not Innovation

- **Status:** Accepted
- **Date:** 2026-09-22

## Context

CockroachDB alternates two kinds of release: **Regular** (even minors — v26.2, v26.4 …), which are
required and get the full support window, and **Innovation** (odd minors — v26.1, v26.3 …), which
are explicitly optional, skippable, and shorter-lived.

Dependabot does not know the difference. It opened a PR bumping the stack from v26.2.5 to v26.3.1,
which also failed CI — an integration guard compared the compose pin against a Go constant, and the
bot cannot edit a Go string.

## Decision

The stack follows the **Regular** line. Innovation minors are ignored in `.github/dependabot.yml`
(each odd minor listed explicitly, since Dependabot has no "even minors only" rule), and the list is
extended as new versions land.

## Consequences

- Upgrades are less frequent and land on versions with the longer support window — the correct trade
  for a database holding a financial ledger.
- The ignore list is a manual obligation. It sits in the config with the reason attached, so the next
  person sees why a v27.1 PR never appears.
- The integration suites now **read** the image from `docker-compose.yml`
  (`internal/platform/testinfra`) instead of restating it, so a Regular bump merges without a hand
  edit. That was the real defect the failing PR exposed: a guard only a human can satisfy is a guard
  that blocks the automation it was meant to protect.

## Guarded by

`internal/platform/testinfra/compose_test.go` (the pin is readable and is never a floating tag) and
`TestCRDBImageMatchesCompose`, which asserts compose is self-consistent — the database node and the
`crdb-init` CLI container must run the same engine.
