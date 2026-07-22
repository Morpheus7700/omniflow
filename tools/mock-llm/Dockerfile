FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w" -o /out/mock-llm ./tools/mock-llm

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/mock-llm /mock-llm
USER nonroot:nonroot
EXPOSE 4000
ENTRYPOINT ["/mock-llm"]
