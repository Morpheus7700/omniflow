FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
# Explicitly verify compilation of the viz-gateway module
RUN go build ./...
RUN go build -trimpath -ldflags="-s -w" -o /out/svc ./cmd

FROM gcr.io/distroless/static:nonroot
# SSE /api/stream port
EXPOSE 8080
COPY --from=builder /out/svc /svc
USER nonroot:nonroot
ENTRYPOINT ["/svc"]
