<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Manifest & Version Management

The manifest is NoKV's metadata log for:

- SST files (`EditAddFile` / `EditDeleteFile`)

Implementation: [`engine/manifest/manager.go`](https://github.com/feichai0017/NoKV/blob/main/engine/manifest/manager.go), [`engine/manifest/codec.go`](https://github.com/feichai0017/NoKV/blob/main/engine/manifest/codec.go), [`engine/manifest/types.go`](https://github.com/feichai0017/NoKV/blob/main/engine/manifest/types.go).

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
}
```

- `Levels`: per-level SST metadata.

---

## 3. Edit Append Semantics

`Manager.LogEdits(edits...)` does:

1. Encode edits to a buffer.
2. Write encoded bytes to current manifest file.
3. Conditionally call `manifest.Sync()` when:
   - `Manager.syncWrites == true`, and
   - at least one add/delete edit was appended.
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
| `engine/lsm/level_manager.go::flush` | Logs `EditAddFile` after SST install; compaction logs add/delete edits. `EditAddFile.LogSeg` carries the WAL segment coverage signal. |
| `engine/lsm/level_manager.go::build` | During startup, missing/corrupt SST entries are marked stale and cleaned via `EditDeleteFile`. |
| `wal` | Replays from its own retained segments; physical GC is controlled by WAL retention participants. |
| `raftstore` | Does not own manifest state. Store-local region catalogs and raft WAL replay checkpoints live in `raftstore/localmeta`; runtime routing state lives in Coordinator storage. |

---

## 6. Recovery-Relevant Guarantees

1. Manifest append is ordered by single manager mutex.
2. WAL replay uses manifest table coverage plus WAL retention state.
3. Restart replays only storage-engine metadata.
4. `CURRENT` indirection protects against partial manifest rewrite publication.

---

## 7. Operational Commands

```bash
nokv manifest --workdir <dir>
```

Useful fields:

- `levels[*].files`

See [recovery.md](recovery.md) and [flush.md](flush.md) for startup and flush ordering details.
