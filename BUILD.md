# OmniFlow — Build & Codegen (fixing the red "unresolved import" errors)

The red files in your editor are gopls reporting **unresolved imports**, not logic bugs. Two causes:
`omniflow/contracts/communication/v1` (the protobuf Go package) has never been generated, and `go.mod`
hasn't been tidied. This clears both.

## 1. Prerequisites
- Go 1.21+
- `buf` (https://buf.build) — used because `vendor_email_received.proto` imports `buf/validate/validate.proto`,
  which buf resolves from the Buf Schema Registry (raw `protoc` would need those protos vendored manually).
- A C toolchain + `librdkafka` — `confluent-kafka-go/v2` is CGO-based:
  - macOS: `brew install librdkafka`  •  Debian/Ubuntu: `apt-get install -y librdkafka-dev`

## 2. Generate the protobuf Go stubs
```bash
buf dep update          # fetches the protovalidate dependency
buf generate            # writes ./contracts/communication/v1/*.pb.go (package communicationv1)
```
This resolves `omniflow/contracts/communication/v1` for both CommBot and the Orchestrator.

## 3. Resolve and pin module dependencies
`go.mod` is currently missing several imports the code uses. Let tidy add them:
```bash
go mod tidy
```
Expected additions (among others): `google.golang.org/protobuf`, `go.opentelemetry.io/otel/sdk`,
`go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp`, `golang.org/x/time`,
`github.com/jackc/pgx/v5`, `github.com/bufbuild/protovalidate-go`.

## 4. Build & vet
```bash
CGO_ENABLED=1 go build ./...
go vet ./...
```

## 5. Known follow-ups (from the Sentinel audit)
- **Proto package inconsistency:** `vendor_email_received.proto` declares `package omniflow.events.communication.v1`
  while `orchestration.proto` declares `package omniflow.communication.v1`. They share one `go_package`, so Go
  compiles fine, but unify the proto package for cleanliness.
- **CRDB JSON `BYTES` encoding:** CockroachDB emits `BYTES` columns as a `\x`-prefixed hex string, not base64.
  The orchestrator consumer's `decodeChangefeedBytes` now handles both — verify against one real changefeed
  message on `omniflow.orchestration.v1` before trusting it.
- **CGO/librdkafka** must be present in the CI image and the runtime container base.
