---
title: NoKV
layout: home
hero:
  name: NoKV
  text: A Rust filesystem for AI training and agent workspaces.
  tagline: Holt-backed metadata, S3-compatible immutable object bodies, FUSE and SDK paths — a self-contained filesystem with no separate database to run.
  image:
    src: /img/logo.svg
    alt: NoKV
  actions:
    - theme: brand
      text: Architecture
      link: /architecture
    - theme: alt
      text: Quick Start
      link: /rustfs
    - theme: alt
      text: Benchmarks
      link: /benchmarks
features:
  - title: Self-contained metadata
    details: A path-native metadata engine (Holt) built in. No Redis, TiKV, or external database to operate — you run a filesystem, not a filesystem plus a database.
  - title: Atomic checkpoints
    details: Object bytes land first, then metadata publishes atomically as a new generation. Readers see a complete checkpoint or the previous one — never a half-written one.
  - title: Built for AI training
    details: ~127K metadata ops/s, single-scan directory listing, immutable cacheable blocks, dataset snapshots, and typed watch events — shaped around datasets, checkpoints, and agent workspaces.
  - title: Object-backed bodies
    details: File bytes are immutable blocks in S3, RustFS, MinIO, or Ceph RGW. Elastic, cheap, zero-ops byte durability — NoKV owns the namespace.
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->
