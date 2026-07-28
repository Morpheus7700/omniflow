# OmniFlow — Build & Codegen

OmniFlow is two Go modules plus generated protobuf contracts. Everything compiles as **static,
CGO-free binaries** — the Kafka client is [franz-go](https://github.com/twmb/franz-go) (pure Go), so
there is **no `librdkafka` and no C toolchain** anywhere in the build or the container images.

> If your editor shows red "unresolved import" errors on `omniflow/contracts/...`, they are gopls
> reporting ungenerated protobuf stubs, not logic bugs — run step 2 (codegen) and step 3 (`go mod
> tidy`) to clear them.

## 1. Prerequisites
- **Go 1.25+** (both `go.mod` files declare `go 1.25.0`).
- **`buf`** (<https://buf.build>) — the contracts import `buf/validate/validate.proto`. That schema is
  **vendored in-tree** at `third_party/proto/buf/validate/validate.proto` and listed as a local module
  in `buf.yaml`; it is *not* fetched from the Buf Schema Registry. `buf.yaml` declares no `deps:`.
- That's it. No CGO, no `librdkafka`, no C compiler.

## 2. Generate the protobuf Go stubs
```bash
buf generate            # writes ./contracts/**/*.pb.go
```
There is deliberately no `buf dep update` step: with no `deps:` in `buf.yaml` it is a no-op, and
running it invites the belief that the validate schema comes from the registry when it does not.

> ⚠️ **Two independently-versioned copies of the same schema.**
> `third_party/proto/buf/validate/validate.proto` (compile time) and
> `buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go` in `go.mod` (runtime) describe the same
> rules but are pinned separately. Refresh the vendored `.proto` without bumping the `go.mod` pin and
> the runtime rejects rules it cannot recognise — **every** message then fails validation.
>
> Both consumers construct their validator with `WithDisableLazy()` + `WithMessages(...)` precisely so
> that mismatch surfaces as a **crash at startup** rather than as a silently healthy service routing
> 100% of traffic to the DLQ. If a service dies on boot with a protovalidate compilation error, these
> two pins have drifted — bump them together.
This resolves `omniflow/contracts/communication/v1` and `omniflow/contracts/inventory/v1` for every
service that consumes or produces those events.

## 3. Resolve and pin module dependencies
```bash
go mod tidy                                  # root module
( cd services/viz-gateway && go mod tidy )   # viz-gateway is a separate module
```

## 4. Build & vet — both modules, static
```bash
# root module: commbot, p2p-orchestrator, inventory-intelligence, tools/*
CGO_ENABLED=0 go build ./...
go vet ./...

# viz-gateway is its own module (SSE read model)
cd services/viz-gateway
CGO_ENABLED=0 go build ./...
go vet ./...
```
Both must exit 0. `CGO_ENABLED=0` is deliberate: it produces static binaries that drop straight into
the `scratch`/distroless container stages and guarantees no accidental `librdkafka` linkage.

## 5. Booting the full stack
Building is DB- and Kafka-free. Running the stack (Kafka + CockroachDB + all services) is done via
`docker compose` and needs **no license key**: compose runs CockroachDB as a single node
(`start-single-node`), and single-node v24.3+ includes Kafka-sink changefeeds licence-free.
`CRDB_LICENSE`/`CRDB_ORG` remain supported as optional env for a multi-node cluster, and `crdb-init`
fails loudly rather than skipping if a key ever turns out to be required. See the
[README](README.md) ("Running it") and `docs/kb/06-build-and-test.md`. The end-to-end and
failure-survival proofs live in `scripts/` and run as GitHub Actions jobs.

## 6. Notes
- **Proto package naming:** contracts are organized under `omniflow.<domain>.v1`; the generated Go
  packages share `go_package` paths so the modules compile as one dependency graph.
- **Changefeed JSON, not protobuf BYTES on the read side:** viz-gateway is deliberately JSON-native — it
  reads the changefeed's projection columns from the `{"after":{…}}` envelope and never decodes the
  protobuf `payload` BYTES. See `docs/kb/02-architecture.md` and `05-gotchas.md`.
