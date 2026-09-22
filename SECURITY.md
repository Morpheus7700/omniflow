# Security

## Reporting a vulnerability

Report privately through GitHub's [private vulnerability reporting](https://github.com/Morpheus7700/omniflow/security/advisories/new). Please do not open a public issue for anything exploitable.

Include the affected service, the commit you tested, and the smallest reproduction you have. If it involves the quarantine boundary or the event spine, the seed harness (`tools/seed`) is usually the fastest way to demonstrate it.

## Automated scanning

Every push runs four scanners. Three report into the repository Security tab; one gates the build:

| Scanner | Scope | Gates? |
|---|---|---|
| govulncheck | reachable CVEs, both Go modules | **yes** — call-graph aware, so its output is small and true enough to block on |
| CodeQL | Go dataflow (both modules) and TypeScript | no |
| gosec | Go SAST, both modules, SARIF uploaded | no |
| Trivy | dependency CVEs, misconfiguration, secret detection | no |

The three report-only scanners are deliberate: they are broad and noisy enough that gating on them would train everyone to ignore a red build. govulncheck is different in kind — it reports a vulnerability only if the code actually reaches the affected symbol.

Dependabot tracks six ecosystems separately — the root Go module, the `viz-gateway` Go module, npm, Dockerfile base images, the compose-only runtime images (`docker` and `docker-compose` are distinct ecosystems, and the former does not read compose files), and the Actions themselves. The two Go modules are listed independently on purpose: `services/viz-gateway` has its own `go.mod` and is invisible to the root manifest.

Two things Dependabot cannot do, so they are standing manual obligations: it will never touch the Go `toolchain` line (bump it when govulncheck names a stdlib CVE), and a full `open-pull-requests-limit` does not queue — it **errors silently** at `/network/updates` and drops the update, which is why the limits are generous and the bumps are grouped.

## Supply chain

Every third-party Action is pinned to a **commit SHA** with the version as a trailing comment, and every base image is pinned by **digest** — a tag is a moving pointer its owner can re-point; a digest is a build input. Digest pinning is also the only way `gcr.io/distroless/static:nonroot` is surveilled at all, since it is a floating non-semver tag Dependabot cannot version-bump. The Trivy installer script is checksum-verified before it runs.

Tagging `v*` publishes the five deployable images to GHCR with an SBOM, SLSA provenance, a keyless cosign signature (Sigstore, bound to the workflow's OIDC identity — there is no key to leak) and a GitHub build-provenance attestation. Verification commands are in the README.

Dependabot **alerts** are enabled as of 2026-08-19. They were not before, and that gap had a cost worth recording: nothing raised a dependency advisory automatically, so `govulncheck` was carrying vulnerability detection alone. It gates on *reachability*, which makes it precise and small — and means a High-severity finding in a test-only dependency (`moby/go-archive`, tar path traversal) sat reported-but-unread in the Trivy output for as long as it took someone to open the Security tab. Report-only scanners become unread scanners; that is the same lesson this repo already wrote down when pinning gosec.

## Trust boundaries

**Vendor-supplied content is hostile until proven otherwise.** CommBot never inlines vendor text into a prompt. Message bodies live behind quarantine URIs, and `validateQuarantineURI` enforces, in this order: HTTPS only, host on an explicit object-store allowlist, and rejection of any host resolving to loopback, link-local, private, or unspecified addresses — which is what stops an allowlisted name being pointed at `169.254.169.254`. Model output is then parsed as untrusted input: a strict integer in a fixed range, never free text.

One caveat we state rather than hide: validation resolves the host and then fetches it, so a DNS-rebind window exists between the two. Closing it requires pinning the resolved IP into a `net.Dialer.Control` hook on the fetch client. It is tracked, not fixed.

**The frontend is presentation-only.** No API routes, no server actions, no database or filesystem access, no credentials, and no committed data. Its only contact with the system is the viz-gateway SSE stream and a replay `GET`, so a fully compromised browser bundle discloses no more than the read model already broadcasts. The gateway origin is a build argument (`NEXT_PUBLIC_API_BASE`), and because `NEXT_PUBLIC_*` is inlined at build time, it is baked into the image rather than injectable at runtime.

That paragraph used to end there, and it was doing more reassuring than it had earned. "No more than the read model already broadcasts" is only a bound if you also know *who the read model broadcasts to* — and the gateway answered both endpoints with `Access-Control-Allow-Origin: *` while authenticating nothing. Any page in any tab of any browser that could route to the gateway could read the whole projection history with one `fetch`. On a laptop that is a demo; inside a network perimeter it is an exfiltration path that never crosses the perimeter. Three bounds now exist, and they are the interesting part of this section rather than a footnote to it:

| Control | Default | Env |
|---|---|---|
| CORS origin allowlist, echoed per-request with `Vary: Origin` | `http://localhost:3000` | `VIZ_ALLOWED_ORIGINS` |
| `/api/replay` row cap — out-of-range is rejected, never clamped | 500, max 5000 | — |
| `/api/replay` per-client rate limit, refused with `429` + `Retry-After` | 5 rps, burst 10 | `VIZ_REPLAY_RPS`, `VIZ_REPLAY_BURST` |
| Concurrent SSE stream cap, refused with `503` + `Retry-After` | 100 | `VIZ_MAX_SSE_CLIENTS` |
| Method-scoped routes — `GET` only; `POST /api/replay` used to run the query | — | — |
| Security headers on every response: `nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy`, CSP with `frame-ancestors 'none'` | — | — |

The rate limit is keyed on the connecting peer, not on `X-Forwarded-For` — that header is caller-controlled, and honouring it blindly would let one client pick any bucket it likes. Set `VIZ_TRUST_PROXY=true` only when the gateway genuinely sits behind a proxy that rewrites it.

The dashboard sets the same class of headers (CSP, HSTS, `nosniff`, `frame-ancestors 'none'`, `Referrer-Policy`, `Permissions-Policy`) and disables `X-Powered-By`. `next/image` is switched off entirely: nothing uses it, and that code path carried two critical unauthenticated-RCE advisories in 2026.

Two honest caveats. **CORS is a browser control, not an access control**: it stops a *page* reading the response, and stops nothing holding a socket — `curl` is unaffected by design, which is also why the E2E proof still works. And **neither endpoint authenticates**. The read model is still readable by anything that can reach port 8081. Making that a real boundary means an authenticating proxy or a token on the gateway, which this system does not have and does not claim to.

The row cap is a bound on disclosure *and* on memory: the replay query previously had no `LIMIT`, so `?from_seq=0` materialised the entire `workflows` table into one slice and one response body. The `statement_timeout` bounds how long a query may run; only a `LIMIT` bounds how much it returns.

**The event spine can be secured in transit.** Every Kafka client reads `KAFKA_TLS`, `KAFKA_TLS_CA` and `KAFKA_SASL_MECHANISM` / `USERNAME` / `PASSWORD` through one helper (`internal/platform/kafkaconf`), supporting PLAIN and SCRAM-SHA-256/512. **SASL without TLS is refused at boot**, so a password cannot be sent in the clear by misconfiguration. Plaintext remains the default because the compose stack is a local test rig.

**The approval topic is a control plane.** Anything able to produce a `HumanApprovalEvent` approves a purchase order. The orchestrator validates the message (UUID event id, W3C traceparent, non-zero sequence key, a non-empty bounded `approved_by`) and records the approver in the durable outbox event — but validation is not authentication. In a real deployment the boundary is a Kafka ACL on that topic plus the SASL credentials above; this repo states that rather than implying the validation is a substitute for it.

**Credentials never live in the repository.** `.gitignore` covers `.env*`; DSNs and broker lists are read from the environment by every service, and a missing `CRDB_DSN` or `KAFKA_BROKERS` is a refused boot rather than a silent fallback to `localhost`. The compose stack is a local test rig and runs CockroachDB insecure with no password — it is not a deployment topology, and it publishes on `127.0.0.1` only, so it is not reachable from the network the host sits on.

## Supported versions

This is a portfolio and demonstration system, not a supported product. Fixes land on `master`; there are no maintained release branches.
