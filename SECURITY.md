# Security

## Reporting a vulnerability

Report privately through GitHub's [private vulnerability reporting](https://github.com/Morpheus7700/omniflow/security/advisories/new). Please do not open a public issue for anything exploitable.

Include the affected service, the commit you tested, and the smallest reproduction you have. If it involves the quarantine boundary or the event spine, the seed harness (`tools/seed`) is usually the fastest way to demonstrate it.

## Automated scanning

Every push runs three scanners, and their findings land in the repository Security tab rather than only in a log:

| Scanner | Scope |
|---|---|
| CodeQL | Go dataflow analysis |
| gosec | Go SAST, both modules, SARIF uploaded |
| Trivy | dependency CVEs, misconfiguration, secret detection |

Dependabot tracks five ecosystems separately — the root Go module, the `viz-gateway` Go module, npm, container base images, and the Actions themselves. The two Go modules are listed independently on purpose: `services/viz-gateway` has its own `go.mod` and is invisible to the root manifest.

## Trust boundaries

**Vendor-supplied content is hostile until proven otherwise.** CommBot never inlines vendor text into a prompt. Message bodies live behind quarantine URIs, and `validateQuarantineURI` enforces, in this order: HTTPS only, host on an explicit object-store allowlist, and rejection of any host resolving to loopback, link-local, private, or unspecified addresses — which is what stops an allowlisted name being pointed at `169.254.169.254`. Model output is then parsed as untrusted input: a strict integer in a fixed range, never free text.

One caveat we state rather than hide: validation resolves the host and then fetches it, so a DNS-rebind window exists between the two. Closing it requires pinning the resolved IP into a `net.Dialer.Control` hook on the fetch client. It is tracked, not fixed.

**The frontend is presentation-only.** No API routes, no server actions, no database or filesystem access, no credentials, and no committed data. Its only contact with the system is the viz-gateway SSE stream and a replay `GET`, so a fully compromised browser bundle discloses no more than the read model already broadcasts. The gateway origin is a build argument (`NEXT_PUBLIC_API_BASE`), and because `NEXT_PUBLIC_*` is inlined at build time, it is baked into the image rather than injectable at runtime.

**Credentials never live in the repository.** `.gitignore` covers `.env*`; DSNs and broker lists are read from the environment by every service. The compose stack is a local test rig and runs CockroachDB insecure with no password — it is not a deployment topology, and it publishes to localhost only.

## Supported versions

This is a portfolio and demonstration system, not a supported product. Fixes land on `master`; there are no maintained release branches.
