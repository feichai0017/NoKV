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
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/nokv-fsmeta ./cmd/nokv-fsmeta

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/nokv nokv \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/nokv /usr/local/bin/nokv
COPY --from=builder /out/nokv-config /usr/local/bin/nokv-config
COPY --from=builder /out/nokv-redis /usr/local/bin/nokv-redis
COPY --from=builder /out/nokv-fsmeta /usr/local/bin/nokv-fsmeta
COPY scripts /usr/local/lib/nokv-scripts
# Wrappers instead of symlinks: the scripts resolve $SCRIPT_DIR from
# $BASH_SOURCE[0], which with a symlink points at /usr/local/bin and breaks
# the sibling `../lib/common.sh` source. `exec <real path>` makes the target
# script see its real location as BASH_SOURCE[0].
RUN chmod +x /usr/local/lib/nokv-scripts/ops/serve-store.sh /usr/local/lib/nokv-scripts/ops/bootstrap.sh \
    && printf '#!/usr/bin/env bash\nexec /usr/local/lib/nokv-scripts/ops/serve-store.sh "$@"\n' > /usr/local/bin/serve-store.sh \
    && printf '#!/usr/bin/env bash\nexec /usr/local/lib/nokv-scripts/ops/bootstrap.sh "$@"\n' > /usr/local/bin/bootstrap.sh \
    && chmod +x /usr/local/bin/serve-store.sh /usr/local/bin/bootstrap.sh \
    && mkdir -p /etc/nokv /var/lib/nokv/store /var/lib/nokv-meta-root \
               /volumes/store-1 /volumes/store-2 /volumes/store-3 \
    && chown -R nokv:nokv /var/lib/nokv /var/lib/nokv-meta-root /volumes
COPY raft_config.example.json /etc/nokv/raft_config.json
USER nokv
WORKDIR /var/lib/nokv
ENTRYPOINT ["/usr/local/bin/nokv"]
CMD ["help"]
