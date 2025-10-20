# syntax=docker/dockerfile:1

FROM golang:1.24 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/nokv ./cmd/nokv
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/nokv-manifest ./scripts/manifestctl
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/nokv-tso ./scripts/tso

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/nokv nokv \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/nokv /usr/local/bin/nokv
COPY --from=builder /out/nokv-manifest /usr/local/bin/nokv-manifest
COPY --from=builder /out/nokv-tso /usr/local/bin/nokv-tso
USER nokv
WORKDIR /var/lib/nokv
ENTRYPOINT ["/usr/local/bin/nokv"]
CMD ["help"]
