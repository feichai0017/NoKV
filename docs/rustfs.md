---
title: RustFS Backend
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# RustFS Backend

NoKV can store file bodies in RustFS through the S3-compatible object backend.
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

Create the default NoKV bucket with an S3-compatible client:

```bash
AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
aws --endpoint-url http://127.0.0.1:9000 \
  s3api create-bucket --bucket nokv
```

For local end-to-end testing, the repository script can do these steps for a
temporary RustFS directory and then run the NoKV benchmark harness:

```bash
scripts/run-rustfs-e2e.sh
```

Use `NOKV_E2E_PROFILE`, `NOKV_E2E_WORKLOAD`, and
`NOKV_E2E_OBJECT_CONCURRENCY` to change the benchmark shape without editing the
script.

## Use RustFS With NoKV

```bash
cargo run --release -p nokv --bin nokv -- \
  init
```

Those are the CLI defaults for the RustFS backend: bucket `nokv`, endpoint
`http://127.0.0.1:9000`, access key `rustfsadmin`, and secret key
`rustfsadmin`.

The same object flags work for artifact publish, `cat`, and FUSE mount:

```bash
cargo run --release -p nokv --bin nokv -- \
  --object-backend rustfs \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin \
  put-artifact /runs/1/checkpoint.bin ./checkpoint.bin
```

## Benchmark Against RustFS

```bash
cargo run --release -p nokv-bench --bin nokv-bench -- \
  --profile smoke \
  --workload checkpoint-publish \
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
workloads for RustFS. The benchmark harness uses the deployable single-node
`metad` service boundary by default. Use `--block-cache off` as a control run
when measuring object backend latency instead of NoKV cache reuse.

References:

- [RustFS Linux installation](https://docs.rustfs.com/installation/linux/index.html)
- [RustFS Docker installation](https://docs.rustfs.com/installation/docker/index.html)
