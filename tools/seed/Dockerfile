FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w" -o /out/seed ./tools/seed

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/seed /seed
USER nonroot:nonroot
ENTRYPOINT ["/seed"]
