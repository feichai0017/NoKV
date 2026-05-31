# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.26 AS go-builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-$(go env GOOS)} GOARCH=${TARGETARCH:-$(go env GOARCH)} \
    go build -o /out/nokv ./cmd/nokv && \
    go build -o /out/nokv-fsmeta ./cmd/nokv-fsmeta

FROM --platform=$BUILDPLATFORM rust:1.88-bookworm AS rust-builder
WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY pb ./pb
COPY raftstore ./raftstore
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/raftstore/target \
    cargo build --manifest-path raftstore/Cargo.toml --release -p nokv-raftstore-server \
    && mkdir -p /out \
    && cp raftstore/target/release/nokv-raftstore-server /out/nokv-raftstore-server

FROM --platform=$TARGETPLATFORM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/nokv nokv \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/nokv/fsmeta /var/lib/nokv/raftstore/holt /var/lib/nokv/raftstore/raftlog \
    && chown -R nokv:nokv /var/lib/nokv
COPY --from=go-builder /out/nokv-fsmeta /usr/local/bin/nokv-fsmeta
COPY --from=go-builder /out/nokv /usr/local/bin/nokv
COPY --from=rust-builder /out/nokv-raftstore-server /usr/local/bin/nokv-raftstore-server
USER nokv
WORKDIR /var/lib/nokv
ENTRYPOINT ["/usr/local/bin/nokv-fsmeta"]
CMD ["--addr=0.0.0.0:8090", "--metrics-addr=0.0.0.0:9400", "--local-work-dir=/var/lib/nokv/fsmeta", "--local-mount-id=default", "--local-mount-key-id=1"]
