# CLI (`cmd/nokv`) Reference

The `nokv` command provides operational visibility similar to RocksDB's `ldb` and Badger's `badger` CLI, but emits JSON to integrate easily with scripts and CI pipelines.

---

## Installation

```bash
go install ./cmd/nokv
```

Use `GOBIN` if you prefer a custom binary directory.

---

## Shared Flags

- `--workdir <path>` – location of the NoKV database (must contain `CURRENT`).
- `--json` – emit structured JSON (default is human-readable tables).
- `--expvar <url>` – for `stats` command, pull metrics from a running process exposing `expvar`.
- `--no-region-metrics` – for `stats` offline mode; skip attaching `RegionMetrics` and report manifest-only figures.

---

## Subcommands

### `nokv stats`

- Reads `StatsSnapshot` either offline (`--workdir`) or via HTTP (`--expvar`).
- Output fields include:
  - `flush.queue`, `flush.wait_ms`, `flush.build_ms`
  - `compaction.backlog`, `wal.active_segment`, `wal.removed_segments`
  - `value_log.head_fid`, `value_log.gc_runs`
  - `txns.active`, `txns.committed`, `txns.conflicts`
  - `regions.total (new/running/removing/tombstone/other)`
  - `hot_keys` (Top-N hits captured by `hotring`)
- Example:

```bash
nokv stats --workdir ./testdata/db --json | jq '.flush.queue'
```

### `nokv manifest`

- Parses the manifest using `manifest.Manager.Version()`.
- Reports per-level file counts, smallest/largest keys, WAL checkpoint, and ValueLog metadata.
- Helpful for verifying flush/compaction results and ensuring manifest rewrites succeeded.

### `nokv vlog`

- Lists vlog segments with status flags (`active`, `candidate_for_gc`, `deleted`).
- Shows head file/offset and pending GC actions.
- Use after running GC or recovery to confirm stale segments are purged.

---

## Integration Tips

- Combine with `RECOVERY_TRACE_METRICS=1` to cross-check logs: run tests, then inspect CLI output to ensure metrics match expectations.
- In CI, capture JSON output and diff against golden files to detect regressions (see `cmd/nokv/main_test.go`).
- When comparing against RocksDB/Badger, treat `nokv manifest` + `nokv vlog` as equivalents to `ldb manifest_dump` and Badger's `badger` `inspect vlog` commands.

---

For architecture context, see [architecture.md](architecture.md) and the module deep dives.
- **`nokv regions`** – Dumps the manifest-backed Region catalog (ID/state/key range/peers). Supports `--json` for automation and complements the Region metrics shown in `nokv stats`.
