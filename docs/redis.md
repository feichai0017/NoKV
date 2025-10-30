# Redis Gateway

`cmd/nokv-redis` exposes NoKV through a RESP-compatible interface. The gateway reuses the engine’s MVCC/transaction semantics and can drive two different backends:

| Mode | Description | Key flags |
| --- | --- | --- |
| Embedded (`embedded`) | Default mode. Operates directly on a local `*NoKV.DB` work directory. All commands (`SET`, `SET NX/XX`, `EX/PX/EXAT/PXAT`, `MSET`, `INCR/DECR`, `DEL`, `MGET`, `EXISTS` …) run inside `db.Update` / `db.View`, so single-key operations are atomic and multi-key reads share a consistent snapshot. | `--workdir <dir>` |
| Raft (`raft`) | Talks to a TinyKv cluster through `raftstore/client`. Writes are executed via TwoPhaseCommit; TTL metadata lives under `!redis:ttl!<key>`. An external TSO (e.g. `scripts/tso`, Docker’s `nokv-tso`) can be used for timestamps. | `--raft-config <file>`<br>`--tso-url http://host:port` |

## Usage examples

### Embedded backend

```bash
go run ./cmd/nokv-redis \
  --addr 127.0.0.1:6380 \
  --workdir ./work_redis \
  --metrics-addr 127.0.0.1:9100  # optional expvar endpoint
```

Validate with `redis-cli -p 6380 ping`. Metrics are available at `http://127.0.0.1:9100/debug/vars` (`NoKV.Redis` section).

### Raft backend

1. Boot TinyKv + TSO using the helper script or Docker Compose (the example config uses store IDs `1-3`, peer IDs `101/201/301`):

   ```bash
   ./scripts/run_local_cluster.sh --tso-port 9494
   # or docker compose up --build
   ```

2. Start the gateway:

   ```bash
   go run ./cmd/nokv-redis \
     --addr 127.0.0.1:6380 \
     --raft-config cmd/nokv-redis/raft_config.example.json \
     --tso-url http://127.0.0.1:9494
   ```

   The `--tso-url` flag is optional; without it the gateway falls back to a local timestamp oracle.

## Supported commands

- String operations: `GET`, `SET`, `SET NX/XX`, `EX/PX/EXAT/PXAT`, `DEL`, `MGET`, `MSET`, `EXISTS`
- Integer operations: `INCR`, `DECR`, `INCRBY`, `DECRBY`
- Utility: `PING`, `ECHO`, `QUIT`

All write commands are applied atomically in both backends. In raft mode, multi-key writes (`MSET`, `DEL`) are batched into a single TwoPhaseCommit call, matching the embedded behaviour. Reads use snapshot transactions in embedded mode (`db.View`) and TinyKv leader reads plus TTL metadata in raft mode.

## Configuration file

`cmd/nokv-redis/raft_config.example.json` matches the layout produced by `scripts/run_local_cluster.sh` and the Docker Compose stack. Fields:

- `stores` – store ID to gRPC address mapping
- `regions` – region ID, start/end keys (plain ASCII or base64), epoch, peer list, leader store ID
- `max_retries` – maximum retries for region errors in the client

Binary ranges can be encoded with `base64.StdEncoding.EncodeToString`.

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
