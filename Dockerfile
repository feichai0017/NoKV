# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

# syntax=docker/dockerfile:1

FROM rust:1.88-bookworm AS builder
WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY bench ./bench

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --locked -p nokvfs-cli --bin nokv-fs \
    && cp /workspace/target/release/nokv-fs /usr/local/bin/nokv-fs

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/nokv nokv \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates fuse3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/nokv/.nokv-fs /mnt/nokv \
    && chown -R nokv:nokv /var/lib/nokv /mnt/nokv

COPY --from=builder /usr/local/bin/nokv-fs /usr/local/bin/nokv-fs

USER nokv
WORKDIR /var/lib/nokv
ENTRYPOINT ["/usr/local/bin/nokv-fs"]
CMD ["serve"]
