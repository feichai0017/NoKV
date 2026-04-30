# Recovery Model

NoKV recovery is intentionally strict. Startup verifies durable metadata and
fails fast on missing or corrupt authoritative files instead of silently
repairing them.

---

## 1. Startup Sequence

1. Validate workdir mode and open the manifest.
2. Run `manifest.Verify` and `wal.VerifyDir`.
3. Open LSM tables from manifest state.
4. Replay retained LSM WAL records that are not already covered by flushed SSTs.
5. Open raft WAL shards and raftstore local metadata.
6. Rebuild runtime views from local metadata and rooted coordinator state.

Values are inline in LSM records. The removed value-log path is not replayed or
reconciled; legacy pointer records return `ErrUnsupportedValueLog`.

---

## 2. Failure Policy

| Failure | Policy |
| --- | --- |
| Missing SST referenced by manifest | Startup fails and leaves manifest intact |
| Corrupt SST referenced by manifest | Startup fails and leaves manifest intact |
| Torn WAL tail | Replay stops at the last complete record |
| Partial `CURRENT.tmp` rewrite | Previous `CURRENT` remains authoritative |
| Legacy value pointer | Read/materialization fails with `ErrUnsupportedValueLog` |

---

## 3. Recovery Tests

Useful focused checks:

```bash
go test ./... -run 'TestRecovery(FailsOnMissingSST|FailsOnCorruptSST|ManifestRewriteCrash|SlowFollowerSnapshotBacklog|SnapshotExportRoundTrip|WALReplayRestoresData)' -count=1 -v
```

Relevant suites:

- `db_test.go`: WAL replay, SST validation, manifest rewrite safety.
- `engine/manifest/manager_test.go`: manifest append/rewrite safety.
- `engine/wal/*_test.go`: WAL replay, durability, retention, backpressure.
- `raftstore/raftlog/*_test.go`: raft log replay and snapshot import/export.

---

## 4. Operator Commands

- `nokv manifest --workdir <dir>`: inspect manifest level state.
- `nokv stats --workdir <dir>`: inspect local runtime stats.
- `nokv regions --workdir <dir>`: inspect local region catalog.
