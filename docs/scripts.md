# Scripts Overview

NoKV now groups shell entrypoints by role instead of keeping every helper flat under `scripts/`.

## Layout

| Path | Role |
| --- | --- |
| `scripts/dev` | Local development and bootstrap helpers for running a cluster from `raft_config*.json`. |
| `scripts/ops` | Operator-style workflows that drive the formal migration CLI. |
| `scripts/lib` | Shared shell helpers for config lookup, workdir hygiene, and build/bootstrap rules. |
| `scripts/*.sh` | Tooling or benchmark entrypoints that are still intentionally top-level. |

This split is deliberate:
- `dev` scripts are allowed to help with local experiments and smoke tests.
- `ops` scripts must treat the migration CLI as source of truth and stay stricter.
- `lib` is where shared rules live, so shell semantics do not drift across scripts.

## Bootstrap & Local Launch

### `scripts/dev/cluster.sh`
- Purpose: build `nokv` and `nokv-config`, read `raft_config.json`, seed local peer catalogs, start Coordinator, then start the configured stores.
- Uses shared rules from:
  - `scripts/lib/common.sh`
  - `scripts/lib/config.sh`
  - `scripts/lib/workdir.sh`
- Example:
  ```bash
  ./scripts/dev/cluster.sh --config ./raft_config.example.json --workdir ./artifacts/cluster
  ```
- Notes:
  - `--config` defaults to `./raft_config.example.json`
  - `--workdir` defaults to `./artifacts/cluster`
  - store workdirs are rejected if they contain unexpected files
  - logs stream with `[coordinator]` / `[store-<id>]` prefixes and are still written to `coordinator.log` / `server.log`
  - this is a bootstrap/dev launcher, not a restart command
  - it may seed fresh workdirs from `config.regions`, so it should not be used against stores that already contain runtime raft/local metadata

### `scripts/ops/bootstrap.sh`
- Purpose: seed local peer catalog metadata into a set of store directories derived from a path template.
- Intended for:
  - Docker Compose bootstrap
  - local static-topology experiments
- Example:
  ```bash
  ./scripts/ops/bootstrap.sh --config /etc/nokv/raft_config.json --path-template /data/store-{id}
  ```
- Notes:
  - skips stores that already contain `CURRENT`
  - refuses to seed into dirty directories
  - this is bootstrap-only; it does not recover runtime topology
  - use `nokv serve` / `scripts/ops/serve-store.sh` to restart an existing store from the same workdir

### `scripts/ops/serve-store.sh`
- Purpose: thin wrapper around `nokv serve` for one store.
- Example:
  ```bash
  ./scripts/ops/serve-store.sh \
    --config ./raft_config.example.json \
    --store-id 1 \
    --workdir ./artifacts/cluster/store-1 \
    --scope local
  ```
- Notes:
  - resolves store listen/workdir/Coordinator defaults through `nokv serve --config`
  - remote peer recovery comes from `raftstore/localmeta`; config `stores` only
    provide `storeID -> addr`
  - no longer treats `config.regions` as restart-time topology truth
  - if a static transport override is needed, use `--store-addr <store-id>=<addr>`
    rather than a peer-id keyed mapping
  - `--scope docker` selects container-friendly addresses

## Migration Workflow

### `scripts/ops/migrate-cluster.sh`
- Purpose: one-shot local operator wrapper for the standalone-to-cluster migration path.
- Drives:
  - `nokv migrate plan`
  - `nokv migrate init`
  - `nokv migrate expand`
  - optional `transfer-leader`
  - optional `remove-peer`
- Example:
  ```bash
  ./scripts/ops/migrate-cluster.sh \
    --config ./raft_config.example.json \
    --workdir ./artifacts/standalone \
    --seed-store 1 \
    --seed-region 1 \
    --seed-peer 101 \
    --target 2:201 \
    --target 3:301 \
    --transfer-leader 201 \
    --remove-peer 101
  ```
- Notes:
  - seed workdir must already contain standalone data
  - target store workdirs must be fresh
  - uses the migration CLI as the only source of truth

## Shared Shell Rules

### `scripts/lib/common.sh`
- shared repo-root detection
- shared build helpers for `nokv` and `nokv-config`
- shared TCP readiness helper

### `scripts/lib/config.sh`
- shared `nokv-config` lookups for:
  - store lines
  - region lines
  - Coordinator address
  - Coordinator workdir

### `scripts/lib/workdir.sh`
- shared workdir hygiene rules:
  - remove stale `LOCK`
  - reject unexpected directory contents
  - assert a directory is fresh before seeding or bootstrap

This is where shell-level correctness rules should keep living. New scripts should reuse these helpers instead of open-coding workdir or config parsing logic again.

## Tooling & Benchmarks

| Script | Purpose |
| --- | --- |
| `scripts/run_benchmarks.sh` | Execute YCSB benchmarks (default engines: NoKV/Badger/Pebble, optional RocksDB). |
| `scripts/build_rocksdb.sh` | Build local RocksDB artifacts used by benchmark comparisons. |
| `scripts/debug.sh` | Wrap `dlv test` for focused debugging. |
| `scripts/gen.sh` | Format protobufs and regenerate Go bindings through Buf. |

For recovery and transport fault validation, use direct Go tests instead of shell wrappers:

```bash
RECOVERY_TRACE_METRICS=1 \
go test ./... -run 'TestRecovery(RemovesStaleValueLogSegment|CleansMissingSSTFromManifest|ManifestRewriteCrash|SlowFollowerSnapshotBacklog|SnapshotExportRoundTrip|WALReplayRestoresData)' -count=1 -v

CHAOS_TRACE_METRICS=1 \
go test -run 'TestGRPCTransport(HandlesPartition|MetricsWatchdog|MetricsBlockedPeers)' -count=1 -v ./raftstore/transport
```

## Relationship with `nokv-config`

- `nokv-config stores` / `regions` / `coordinator` remain the structured topology source for shell scripts.
- `config.regions` remain bootstrap/deployment metadata, not restart-time peer truth.
- `nokv-config catalog` writes Region metadata into the local peer catalog.
- `cmd/nokv-redis` uses the same `raft_config.json`, so local scripts and Redis gateway stay aligned.
- Go tools can import `github.com/feichai0017/NoKV/config` and call `config.LoadFile` / `Validate` directly.

Maintaining a single `raft_config.json` still keeps development scripts, Docker Compose, Redis gateway, and automated tests aligned. The difference now is that shell behavior is shared and explicit instead of repeated across four separate entrypoints.
