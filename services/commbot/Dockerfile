FROM golang:1.27 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
# Explicitly verify compilation of the root module
RUN go build ./...
RUN go build -trimpath -ldflags="-s -w" -o /out/svc ./services/commbot/cmd

FROM gcr.io/distroless/static:nonroot
# Cloud Run expects the service to listen on a port for health checks
EXPOSE 8080
COPY --from=builder /out/svc /svc
USER nonroot:nonroot
ENTRYPOINT ["/svc"]
