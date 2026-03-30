# syntax=docker/dockerfile:1

FROM golang:1.26 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/nokv ./cmd/nokv
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/nokv-config ./cmd/nokv-config
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/nokv-redis ./cmd/nokv-redis

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/nokv nokv \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/nokv /usr/local/bin/nokv
COPY --from=builder /out/nokv-config /usr/local/bin/nokv-config
COPY --from=builder /out/nokv-redis /usr/local/bin/nokv-redis
COPY scripts /usr/local/lib/nokv-scripts
RUN chmod +x /usr/local/lib/nokv-scripts/dev/serve-store.sh /usr/local/lib/nokv-scripts/dev/bootstrap.sh \
    && ln -sf /usr/local/lib/nokv-scripts/dev/serve-store.sh /usr/local/bin/serve-store.sh \
    && ln -sf /usr/local/lib/nokv-scripts/dev/bootstrap.sh /usr/local/bin/bootstrap.sh \
    && mkdir -p /etc/nokv
COPY raft_config.example.json /etc/nokv/raft_config.json
USER nokv
WORKDIR /var/lib/nokv
ENTRYPOINT ["/usr/local/bin/nokv"]
CMD ["help"]
