# Redis Gateway

`cmd/nokv-redis` exposes NoKV through a RESP-compatible endpoint. The gateway reuses the engine’s MVCC/transaction semantics and can operate in two modes:

| Mode | Description | Key flags |
| --- | --- | --- |
| Embedded (`embedded`) | Opens a local `*NoKV.DB` work directory. Commands (`SET`, `SET NX/XX`, `EX/PX/EXAT/PXAT`, `MSET`, `INCR/DECR`, `DEL`, `MGET`, `EXISTS`, …) run inside `db.Update` / `db.View`, providing atomic single-key updates and snapshot reads across multiple keys. | `--workdir <dir>` |
| Raft (`raft`) | Routes requests through `raftstore/client` and a TinyKv cluster. Writes execute via TwoPhaseCommit; TTL metadata is stored under `!redis:ttl!<key>`. When `--tso-url` is omitted, the gateway consults the `tso` block in `raft_config.json` and falls back to a local oracle if the block is absent. | `--raft-config <file>`<br>`--tso-url http://host:port` (optional) |

## Usage examples

### Embedded backend

```bash
go run ./cmd/nokv-redis \
  --addr 127.0.0.1:6380 \
  --workdir ./work_redis \
  --metrics-addr 127.0.0.1:9100  # optional expvar endpoint
```

Validate with `redis-cli -p 6380 ping`. Metrics are exposed at `http://127.0.0.1:9100/debug/vars` under the `NoKV.Redis` key.

### Raft backend

1. Start TinyKv and, if configured, the TSO using the helper script or Docker Compose. Both consume `raft_config.example.json`, initialise manifests for each store, and launch `nokv-tso` automatically when `tso.listen_addr` is present:

   ```bash
   ./scripts/run_local_cluster.sh
   # or: docker compose up --build
   ```

2. Run the gateway:

   ```bash
   go run ./cmd/nokv-redis \
     --addr 127.0.0.1:6380 \
     --raft-config raft_config.example.json
   ```

   Supply `--tso-url` only when you need to override the config file; otherwise the gateway uses `tso.advertise_url` (or `listen_addr`) from the same JSON. If the block is missing, it falls back to the embedded timestamp oracle.

## Supported commands

- String operations: `GET`, `SET`, `SET NX/XX`, `EX/PX/EXAT/PXAT`, `DEL`, `MGET`, `MSET`, `EXISTS`
- Integer operations: `INCR`, `DECR`, `INCRBY`, `DECRBY`
- Utility: `PING`, `ECHO`, `QUIT`

In both modes write commands are atomic. The Raft backend batches multi-key updates (`MSET`, `DEL`, …) into a single TwoPhaseCommit, matching the embedded semantics. Reads use snapshot transactions locally (`db.View`) and leader reads with TTL checks remotely.

## Configuration file

`raft_config.example.json` is shared by `scripts/run_local_cluster.sh`, Docker Compose, and the Redis gateway. Important fields:

- `stores` – store ID, gRPC address, and optional container listen/advertise addresses
- `regions` – region ID, start/end keys (use `hex:<bytes>` for binary data), epoch, peer list, leader store ID
- `max_retries` – maximum retries for region errors in the distributed client

Use `nokv-config` to inspect or validate the configuration:

```bash
nokv-config stores --config raft_config.json
nokv-config regions --config raft_config.json --format json | jq '.[] | {id:.id, peers:.peers}'
```

For Go tooling, import `github.com/feichai0017/NoKV/config` and call `config.LoadFile` / `Validate` to reuse the same schema and defaults across CLIs, scripts, and applications.

## Metrics

With `--metrics-addr` enabled the gateway publishes `NoKV.Redis` on `/debug/vars`, for example:

```json
{
  "commands_total": 128,
  "errors_total": 0,
  "connections_active": 1,
  "connections_accepted": 4,
  "commands_per_operation": {
    "PING": 4,
    "SET": 32,
    "GET": 64,
    "MGET": 8,
    "DEL": 10,
    "INCR": 10
  }
}
```

These counters are part of the process-wide expvar output and can be scraped alongside the rest of NoKV’s metrics.
