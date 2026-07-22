FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w" -o /out/svc ./services/inventory-intelligence/cmd

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/svc /svc
USER nonroot:nonroot
ENTRYPOINT ["/svc"]
