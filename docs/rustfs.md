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

Install the RustFS binary with Homebrew:

```bash
brew tap rustfs/homebrew-tap
brew install rustfs
rustfs --version
```

Start a single-node local RustFS process:

```bash
mkdir -p /tmp/rustfs-data
RUSTFS_ACCESS_KEY=rustfsadmin \
RUSTFS_SECRET_KEY=rustfsadmin \
rustfs server \
  --address 127.0.0.1:9000 \
  --console-enable \
  --console-address 127.0.0.1:9001 \
  --buffer-profile AiTraining \
  /tmp/rustfs-data
```

Create the default NoKV-FS bucket with an S3-compatible client:

```bash
AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
aws --endpoint-url http://127.0.0.1:9000 \
  s3api create-bucket --bucket nokv
```

## Use RustFS With NoKV-FS

```bash
cargo run --release -p nokvfs-cli --bin nokv-fs -- \
  init
```

Those are the CLI defaults for the RustFS backend: bucket `nokv`, endpoint
`http://127.0.0.1:9000`, access key `rustfsadmin`, and secret key
`rustfsadmin`.

The same object flags work for artifact publish, `cat`, and FUSE mount:

```bash
cargo run --release -p nokvfs-cli --bin nokv-fs -- \
  --object-backend rustfs \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin \
  put-artifact /runs/1/checkpoint.bin ./checkpoint.bin
```

## Benchmark Against RustFS

```bash
cargo run --release -p nokvfs-bench --bin nokv-fs-bench -- \
  --profile smoke \
  --workload checkpoint-publish \
  --metadata-mode remote \
  --object-backend rustfs \
  --object-concurrency 4 \
  --checkpoint-bytes 1048576 \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin
```

`mdtest-easy` and `mdtest-hard` are metadata-only and do not exercise object
storage. `checkpoint-publish` and `training-read` are the useful object-backed
workloads for RustFS. Use `--metadata-mode local` for the in-process Holt
baseline and `--metadata-mode remote` for the deployable `metad` service
boundary. Use `--block-cache off` as a control run when measuring object
backend latency instead of NoKV-FS cache reuse.

References:

- [RustFS Linux installation](https://docs.rustfs.com/installation/linux/index.html)
- [RustFS Docker installation](https://docs.rustfs.com/installation/docker/index.html)
