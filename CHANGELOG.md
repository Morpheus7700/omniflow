# Changelog

All notable changes to OmniFlow are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Until the first tag, everything is `Unreleased`.

Dependency bumps are omitted unless they close an advisory.

## [Unreleased]

### Security
- Next.js 16.3.5 — closes two critical unauthenticated remote-code-execution advisories
  (GHSA-p293-qw3h-jr36, GHSA-2xp9-vwfh-vxw4) and the sharp/libheif advisory.
- gRPC-Go 1.83.2 — closes GO-2026-6348 (heap exhaustion via HTTP/2 DATA fragmentation), which had
  turned the govulncheck gate red on every open PR.
- Go toolchain 1.27.1 (from 1.26.6) — picks up the five stdlib CVEs fixed in 1.26.7.
- Every GitHub Action is pinned by commit SHA; the Trivy installer is checksum-verified before it runs.
- Every base image is pinned by digest; the six Go images run as `nonroot` on distroless with a
  declared `HEALTHCHECK`. The frontend image moved from Node 26 (Current) to Node 24 (Active LTS).
- Compose publishes every port on `127.0.0.1` instead of `0.0.0.0`; the license values crdb-init
  interpolates into SQL are escaped.
- Kafka broker auto-topic-creation is off: a topic missing from `kafka-init.sh` now fails loudly
  instead of being created ad hoc by the changefeed sink.

### Added
- `LICENSE` (Apache-2.0). The repository was previously unlicensed.
- Release workflow: on a `v*` tag, the five deployable images are published to GHCR with an SBOM,
  SLSA provenance, a keyless cosign signature and a GitHub build-provenance attestation.
- CI gates: `go test -race`, `golangci-lint` (both modules, `.golangci.yml`), `shellcheck` on the
  proofs and init scripts, and a frontend job (lint, typecheck, `next build`) that fails in minutes
  instead of inside a 25-minute boot job. Frontend lint is a gate for the first time.
- CodeQL now scans TypeScript and the viz-gateway module, not only the root Go module.
- Boot proofs upload their full container logs as a workflow artifact on failure.
- `scripts/lib.sh`: the shared harness every boot proof is built on.
- `internal/platform/testinfra`: integration tests read the CockroachDB image from
  `docker-compose.yml` instead of restating it, so a Dependabot bump can no longer fail the guard.
- `mock-llm` answers `-probe`, so the orchestrator waits for it to be healthy, not merely started.
- `Makefile`, `.editorconfig`, `.env.example`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, PR and issue
  templates, `frontend/.nvmrc`.

### Changed
- One Go toolchain pin: the `toolchain` directive in both `go.mod` files, read by every CI job via
  `go-version-file`. Ten YAML pins are gone, including the one job that had drifted to a floating
  `'1.25'`.
- Boot proofs poll for readiness and terminal state instead of fixed sleeps, and no longer skip on
  fork PRs (a skipped required job counted as passing).
- Dependabot: CockroachDB Innovation releases (odd minors) are ignored — the stack follows the
  Regular line; Docker `golang`/distroless bumps and Actions bumps are grouped; limits raised so the
  queue cannot silently black out.
- Errors that wrap a sentinel and a cause now wrap both (`%w … %w`) so `errors.Is` sees either.
- Kafka JVM heap and every container's memory are bounded in compose.

## [0.1.0-proven] — 2026-08-20

The state reached by the "Make It Real" mandate: the event spine is proven end-to-end on real
infrastructure in CI and locally, with five failure-survival proofs. This is the baseline the
`Unreleased` section builds on; the commits below are its history.

### Added
- The four services (`commbot`, `p2p-orchestrator`, `inventory-intelligence`, `viz-gateway`) and the
  Next.js ledger dashboard, on CockroachDB native JSON changefeeds → Kafka (KRaft) → franz-go.
- Proofs: boot + seed E2E, durable checkpoint resume, exactly-once delivery suppression, FIFO
  late-arrival restatement, DLQ poison-pill routing (wire and semantic), agent exactly-once effect.
- The drafting agent (`draft_po`): a real model call outside the workflow transaction, with a
  deterministic idempotency key, an `aigov` spend guard and an audit trail.
- Real `/healthz` and `/readyz` on all four services; distroless self-probe healthchecks in compose.
- Session `statement_timeout` for every DB call; bounded DLQ produce and offset commit that survive
  SIGTERM (`internal/platform/delivery`).
- viz-gateway origin allowlist, replay row cap and SSE client cap.
- Unit + integration test tiers (testcontainers against real CockroachDB); govulncheck as the gating
  CVE scanner; gosec, Trivy and CodeQL report-only; Dependabot across six ecosystems.
- Branch protection on `master`: nine required checks, strict, admins included.
- `docs/kb/` — the knowledge-base vault, with `05-gotchas.md` as the record of everything that bit.

### Fixed (the ones worth remembering)
- Orchestrator self-deadlock: re-fetch under the lease's own transaction, not the pool.
- CockroachDB dialect: data-modifying CTEs must return columns; one placeholder gets one type.
- franz-go never asks the broker to auto-create topics — `kafka-init.sh` creates all of them.
- The DLQ poison test never reached the validator; a semantic pill now does.
- viz-gateway answered `Access-Control-Allow-Origin: *` with no auth and no `LIMIT`.
- A floating `go-version` in CI silently disarmed the CVE gate.
- Frontend image built from the host's `node_modules` (missing `frontend/.dockerignore`).
