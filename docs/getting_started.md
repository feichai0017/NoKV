# Getting Started

This guide gets you from zero to a running NoKV cluster (or an embedded DB) in a few minutes.

## Prerequisites
- Go 1.26+
- Git
- (Optional) Docker + Docker Compose for containerized runs

## Option A: Local Cluster (recommended for dev)
This launches the 333 separated layout: 3 replicated meta-root peers (Truth plane),
1 coordinator (Service plane), and the stores declared in the config (Execution plane).

```bash
./scripts/dev/cluster.sh --config ./raft_config.example.json
```

The launcher stays attached, streams meta-root/Coordinator/store logs to the terminal, and also writes them under `./artifacts/cluster/`.

If you stop one store and want to restart it later, restart it against the same
workdir:

```bash
./scripts/ops/serve-store.sh \
  --config ./raft_config.example.json \
  --store-id 1 \
  --workdir ./artifacts/cluster/store-1
```

On restart, NoKV recovers hosted peers from local metadata in the store workdir.
The config file is only used to start stores and Coordinator. Runtime clients
discover store addresses from Coordinator heartbeats.
Do not rerun `scripts/ops/bootstrap.sh` or treat `scripts/dev/cluster.sh` as the
restart path for an already-running store.

Start the Redis-compatible gateway in another shell:

```bash
go run ./cmd/nokv-redis --addr 127.0.0.1:6380 --coordinator-addr 127.0.0.1:2379
```

Quick smoke test:

```bash
redis-cli -p 6380 ping
```

### Inspect stats

```bash
go run ./cmd/nokv stats --workdir ./artifacts/cluster/store-1
```

## Option B: Docker Compose
This runs the cluster and gateway in containers.

```bash
docker compose up --build
```

Tear down:

```bash
docker compose down -v
```

## Embedded Usage (single-process)
Use NoKV as a library when you do not need raftstore.

```go
package main

import (
	"fmt"
	"log"

	NoKV "github.com/feichai0017/NoKV"
)

func main() {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = "./workdir-demo"

	db, err := NoKV.Open(opt)
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	key := []byte("hello")
	if err := db.Set(key, []byte("world")); err != nil {
		log.Fatalf("set failed: %v", err)
	}

	entry, err := db.Get(key)
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("value=%s\n", entry.Value)
}
```

> Note:
> - `DB.Get` returns detached entries (do not call `DecrRef`).
> - `DB.GetInternalEntry` returns borrowed entries and callers must call `DecrRef` exactly once.
> - `DB.SetWithTTL` accepts `time.Duration` (relative TTL). `DB.Set`/`DB.SetBatch`/`DB.SetWithTTL` reject `nil` values; use `DB.Del` or `DB.DeleteRange(start,end)` for deletes.
> - `DB.NewIterator` exposes user-facing entries, while `DB.NewInternalIterator` scans raw internal keys (`cf+user_key+ts`).

## Benchmarks
Micro benchmarks:

```bash
go test -bench=. -run=^$ ./...
```

YCSB (default: NoKV + Badger + Pebble, workloads A-F):

```bash
make bench
```

Override defaults with env vars:

```bash
YCSB_RECORDS=1000000 YCSB_OPS=1000000 YCSB_CONC=8 make bench
```
Detailed benchmark methodology and latest result snapshots are maintained in:
[`benchmark/README.md`](../benchmark/README.md).

## Cleanup
If a local run crashes or you want a clean slate:

```bash
make clean
```

## Troubleshooting
- **WAL replay errors after crash**: wipe the workdir and restart the cluster.
- **Port conflicts**: adjust addresses in `raft_config.example.json`.
- **Slow startup**: reduce `YCSB_RECORDS` or `YCSB_OPS` when benchmarking locally.
