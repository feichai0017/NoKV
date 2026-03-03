# Manifest & Version Management

The manifest is NoKV's metadata log for:

- SST files (`EditAddFile` / `EditDeleteFile`)
- WAL checkpoint (`EditLogPointer`)
- value-log metadata (`EditValueLogHead`, `EditDeleteValueLog`, `EditUpdateValueLog`)
- raft and region metadata (`EditRaftPointer`, `EditRegion`)

Implementation: [`manifest/manager.go`](../manifest/manager.go), [`manifest/codec.go`](../manifest/codec.go), [`manifest/types.go`](../manifest/types.go).

---

## 1. Files on Disk

```text
WorkDir/
  CURRENT
  MANIFEST-000001
  MANIFEST-000002
```

- `CURRENT` stores the active manifest filename.
- `CURRENT` is updated via `CURRENT.tmp -> CURRENT` rename.
- `MANIFEST-*` stores append-only encoded edits.

---

## 2. In-Memory Version Model

```go
type Version struct {
    Levels       map[int][]FileMeta
    LogSegment   uint32
    LogOffset    uint64
    ValueLogs    map[ValueLogID]ValueLogMeta
    ValueLogHead map[uint32]ValueLogMeta
    RaftPointers map[uint64]RaftLogPointer
    Regions      map[uint64]RegionMeta
}
```

- `Levels`: per-level SST metadata.
- `LogSegment/LogOffset`: WAL replay checkpoint.
- `ValueLogs` + `ValueLogHead`: all known vlog segments and per-bucket active heads.
- `RaftPointers/Regions`: raftstore progress + region metadata.

---

## 3. Edit Append Semantics

`Manager.LogEdits(edits...)` does:

1. Encode edits to a buffer.
2. Write encoded bytes to current manifest file.
3. Conditionally call `manifest.Sync()` when:
   - `Manager.syncWrites == true`, and
   - at least one edit type requires sync (`Add/DeleteFile`, `LogPointer`, value-log edits).
4. Apply edits to in-memory `Version`.
5. Trigger manifest rewrite if size crosses threshold.

`SetSync(bool)` and `SetRewriteThreshold(int64)` are configured by LSM options.

---

## 4. Rewrite Flow

When rewrite threshold is exceeded (or `Rewrite()` is called):

1. Create next `MANIFEST-xxxxxx`.
2. Write a full snapshot of current `Version`.
3. Flush writer, and `Sync()` the new manifest when `syncWrites` is enabled.
4. Update `CURRENT` to point to new file.
5. Reopen the new manifest for appends and remove old manifest file.

If rewrite fails before `CURRENT` update, restart continues using previous manifest.

---

## 5. Interaction with Other Modules

| Module | Manifest usage |
| --- | --- |
| `lsm/levels.go::flush` | Logs `EditAddFile` + `EditLogPointer` after SST install; compaction logs add/delete edits. |
| `lsm/levels.go::build` | During startup, missing/corrupt SST entries are marked stale and cleaned via `EditDeleteFile`. |
| `wal` | Replays from manifest checkpoint (`LogSegment`, `LogOffset`). |
| `vlog` | Persists head/update/delete metadata and uses manifest state for stale/orphan cleanup on startup. |
| `raftstore` | Persists raft pointers and region metadata through manifest edits. |

---

## 6. Recovery-Relevant Guarantees

1. Manifest append is ordered by single manager mutex.
2. WAL replay starts from manifest checkpoint.
3. Stale manifest SST entries are self-healed on startup (delete edit appended).
4. `CURRENT` indirection protects against partial manifest rewrite publication.

---

## 7. Operational Commands

```bash
nokv manifest --workdir <dir>
```

Useful fields:

- `log_pointer.segment`, `log_pointer.offset`
- `levels[*].files`
- `value_log_heads`
- `value_logs[*].valid`

See [recovery.md](recovery.md) and [flush.md](flush.md) for startup and flush ordering details.
