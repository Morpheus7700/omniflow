---
paths:
  - "services/*/internal/adapters/**"
  - "services/viz-gateway/**"
  - "infrastructure/**"
  - "frontend/src/app/**"
  - "scripts/**"
---

# Security

- Validate every input at the system boundary — HTTP handlers, SSE endpoints, and Kafka message
  decoders alike. A message off a topic is untrusted input, not a trusted internal call.
- Parameterized queries only. Never build SQL by concatenating or `fmt.Sprintf`-ing user or
  message-derived values, including table and column names.
- Never log or embed secrets, tokens, passwords, connection strings, or PII. That includes
  CockroachDB DSNs and any `CRDB_LICENSE` value.
- Secrets come from the environment, never from a committed file. `.env`, `*.pem`, `*.key`,
  `*_credentials.json`, and `crdb-license.txt` are gitignored and must stay that way.
- Constant-time comparison (`crypto/subtle`) for tokens and signatures. Never `==`.
- Set explicit timeouts and body-size limits on every server and outbound client. An unbounded
  `http.Server` with no `ReadHeaderTimeout` is a denial-of-service surface, and `gosec` flags it.
- SSE endpoints: enforce an origin check, cap concurrent connections, and make handlers respect
  request context cancellation so a disconnected browser doesn't pin a goroutine.
- Don't weaken a `gosec` finding by adding `#nosec` to silence it. Fix it, or record why it is a
  false positive in the same commit.
