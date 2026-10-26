# Build from the repository root; run /loadtest with the same flags as go run.
FROM golang:1.27-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -o /out/loadtest ./examples/benchmark/loadtest

FROM alpine:3.23
COPY --from=builder /out/loadtest /loadtest
CMD ["sleep", "infinity"]
