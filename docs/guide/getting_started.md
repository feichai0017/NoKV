<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Getting Started

This guide starts the local Badger-backed fsmeta runtime. The distributed Rust
data plane lives in `raftstore`; it is available through the `raftstore`
runtime but is not the default Compose demo path yet.

## Prerequisites

- Go 1.26+
- Git
- Optional: Docker + Docker Compose
- Optional for distributed data-plane work: Rust toolchain and protobuf compiler

## Local Server

```bash
go run ./cmd/nokv-fsmeta \
  --addr 127.0.0.1:8090 \
  --metrics-addr 127.0.0.1:9400 \
  --local-work-dir ./nokv-fsmeta-local \
  --local-mount-id default \
  --local-mount-key-id 1
```

Metrics and Go profiles are available at:

```bash
curl http://127.0.0.1:9400/debug/vars
```

## Docker Demo

```bash
docker compose up -d --build
docker compose logs -f
docker compose down -v
```

This starts one `nokv-fsmeta` service backed by a Badger workdir in the
`fsmeta-data` volume.

## Distributed Entrypoints

The distributed path is not wired into the default Compose demo yet. The
process split is:

```bash
go run ./cmd/nokv meta-root ...
go run ./cmd/nokv coordinator ...
NOKV_RAFTSTORE_HOLT_DIR=/tmp/nokv-raftstore-holt \
NOKV_RAFTSTORE_LOG_DIR=/tmp/nokv-raftstore-log \
cargo run --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server -- ...
go run ./cmd/nokv-fsmeta \
  --runtime raftstore \
  --coordinator-addr 127.0.0.1:23800 \
  --bootstrap-mount default
```

`cmd/nokv` owns Go control-plane startup only. The Rust data-plane binary owns
the replicated metadata data-plane service.

## Library Usage

```go
package main

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/model"
	fsmetalocal "github.com/feichai0017/NoKV/fsmeta/runtime/local"
)

func main() {
	ctx := context.Background()
	rt, err := fsmetalocal.Open(ctx, fsmetalocal.Options{
		WorkDir: "./nokv-fsmeta-local",
		Mount:   model.MountIdentity{MountID: "default", MountKeyID: 1},
	})
	if err != nil {
		panic(err)
	}
	defer rt.Close()

	_, err = rt.Executor.Create(ctx, model.CreateRequest{
		Mount:  "default",
		Parent: model.RootInode,
		Name:   "hello.txt",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	if err != nil {
		panic(err)
	}
}
```

## Benchmarks

Run the local fsmeta workload matrix:

```bash
NOKV_FSMETA_BENCH_MODE=local make fsmeta-bench
```

Override the workload and scale:

```bash
NOKV_FSMETA_PROFILE=official \
NOKV_FSMETA_WORKLOADS=mdtest-easy \
NOKV_FSMETA_BENCH_MODE=local \
make fsmeta-bench
```

## Tests

```bash
go test ./...
cargo test --manifest-path raftstore/Cargo.toml --workspace
make test
```
