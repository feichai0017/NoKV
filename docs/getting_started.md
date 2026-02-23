# Getting Started

This guide gets you from zero to a running NoKV cluster (or an embedded DB) in a few minutes.

## Prerequisites
- Go 1.26+
- Git
- (Optional) Docker + Docker Compose for containerized runs

## Option A: Local Cluster (recommended for dev)
This launches a 3-node Raft cluster plus the optional TSO helper.

```bash
./scripts/run_local_cluster.sh --config ./raft_config.example.json
```

Start the Redis-compatible gateway in another shell:

```bash
go run ./cmd/nokv-redis --addr 127.0.0.1:6380 --raft-config raft_config.example.json
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

	db := NoKV.Open(opt)
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

> Note: Public read APIs (`DB.Get`, `DB.GetCF`, `DB.GetVersionedEntry`, `Txn.Get`) return detached entries and do not require `DecrRef`.

## Benchmarks
Micro benchmarks:

```bash
go test -bench=. -run=^$ ./...
```

YCSB (default: NoKV + Badger + Pebble, workloads A-G):

```bash
make bench
```

Override defaults with env vars:

```bash
YCSB_RECORDS=1000000 YCSB_OPS=1000000 YCSB_CONC=8 make bench
```

Latest full baseline (2026-02-23):

| Workload | NoKV (ops/s) | Badger (ops/s) | Pebble (ops/s) |
| :--- | ---: | ---: | ---: |
| YCSB-A | 847,660 | 396,314 | 1,282,218 |
| YCSB-B | 1,742,820 | 716,151 | 1,941,330 |
| YCSB-C | 2,070,856 | 826,766 | 847,764 |
| YCSB-D | 1,754,955 | 842,637 | 2,509,809 |
| YCSB-E | 205,489 | 41,508 | 554,557 |
| YCSB-F | 715,946 | 326,343 | 1,123,473 |
| YCSB-G | 413,521 | 399,405 | 583,584 |


## Cleanup
If a local run crashes or you want a clean slate:

```bash
make clean
```

## Troubleshooting
- **WAL replay errors after crash**: wipe the workdir and restart the cluster.
- **Port conflicts**: adjust addresses in `raft_config.example.json`.
- **Slow startup**: reduce `YCSB_RECORDS` or `YCSB_OPS` when benchmarking locally.
