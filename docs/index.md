---
title: NoKV-FS
layout: home
hero:
  name: NoKV-FS
  text: Rust filesystem metadata for AI training and agent workspaces.
  tagline: Holt-backed metadata, S3-compatible object storage, and a path toward a high-performance OSS DFS.
  actions:
    - theme: brand
      text: Architecture
      link: /architecture
    - theme: alt
      text: Product Design
      link: /product-design
features:
  - title: Metadata-first filesystem
    details: Inode/dentry namespace semantics, metadata commands, dentry projection, snapshots, and typed watches.
  - title: Object storage for bodies
    details: File bytes live in S3-compatible storage such as RustFS, MinIO, Ceph RGW, or AWS S3.
  - title: Built for AI workloads
    details: Designed around datasets, checkpoints, artifact publish, and scoped agent workspace views.
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->
