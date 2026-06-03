---
title: RustFS Backend
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# RustFS Backend

NoKV-FS can store file bodies in RustFS through the S3-compatible object backend.
RustFS supports single-node single-disk, single-node multi-disk, and multi-node
multi-disk deployment modes. The single-node single-disk mode is useful for
local testing and small non-critical deployments; it is not a high-availability
production layout.

## Start RustFS

One local Docker shape is:

```bash
docker pull rustfs/rustfs

mkdir -p /tmp/rustfs-data
docker run -d \
  --name rustfs_local \
  -p 9000:9000 \
  -p 9001:9001 \
  -v /tmp/rustfs-data:/data \
  rustfs/rustfs:latest \
  /data
```

Create a bucket with an S3-compatible client such as `mc`:

```bash
mc alias set rustfs http://localhost:9000 rustfsadmin rustfsadmin
mc mb rustfs/nokv
```

## Use RustFS With NoKV-FS

```bash
cargo run --release -p nokvfs-client --bin nokv-fs -- \
  --object-backend rustfs \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin \
  init
```

The same object flags work for artifact publish, `cat`, and FUSE mount:

```bash
cargo run --release -p nokvfs-client --bin nokv-fs -- \
  --object-backend rustfs \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin \
  put-artifact /runs/1/checkpoint.bin ./checkpoint.bin
```

## Benchmark Against RustFS

```bash
cargo run --release -p nokvfs-client --bin nokv-fs-bench -- \
  --profile smoke \
  --workload checkpoint-publish \
  --object-backend rustfs \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin
```

`mdtest-easy` and `mdtest-hard` are metadata-only and do not exercise object
storage. `checkpoint-publish` and `training-read` are the useful object-backed
workloads for RustFS.

References:

- [RustFS Linux installation](https://docs.rustfs.com/installation/linux/index.html)
- [RustFS Docker installation](https://docs.rustfs.com/installation/docker/index.html)
