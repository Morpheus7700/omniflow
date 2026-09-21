# syntax=docker/dockerfile:1
# omniflow-commbot — root-module service; build context is the repo root.
#
# Base images are pinned by DIGEST, not just tag. A tag is a moving pointer: `golang:1.27` and
# `distroless/static:nonroot` both change content silently, so two builds of the same commit could
# ship different toolchains and different runtime layers. The digest makes the build reproducible
# and lets Dependabot's docker ecosystem raise a PR when the pinned content moves — which is the only
# way the floating `nonroot` tag can be surveilled at all (see .github/dependabot.yml).
FROM --platform=$BUILDPLATFORM golang:1.27.1@sha256:3680233e3204827fbdc66088528ae6d4b3d034f51d03a99d454f6de034888244 AS builder
WORKDIR /src
COPY go.mod go.sum ./
# The module cache and build cache are BuildKit cache mounts, shared across every image built from
# this context, so six services do not download and compile the same dependencies six times.
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# Cross-compile for the target platform on the build platform. Without --platform=$BUILDPLATFORM
# above, a multi-arch build runs this whole stage under QEMU emulation; with it, Go simply sets
# GOARCH and the arm64 image costs seconds instead of minutes.
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/svc ./services/commbot/cmd

FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
LABEL org.opencontainers.image.title="omniflow-commbot" \
      org.opencontainers.image.source="https://github.com/Morpheus7700/omniflow" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=builder /out/svc /svc
USER nonroot:nonroot
EXPOSE 8080
# The image is distroless — no shell — so the healthcheck is the binary's own probe mode against its
# /readyz. Compose repeats this so `depends_on: service_healthy` is real; declaring it here makes
# the image self-describing for anything that runs it outside compose.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 CMD ["/svc", "-probe"]
ENTRYPOINT ["/svc"]
