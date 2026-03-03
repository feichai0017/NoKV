# VFS

The `vfs` package provides a small filesystem abstraction used by WAL, manifest, SST, value-log, and raftstore paths.

---

## 1. Core Interfaces

`vfs.FS` includes:

- file open/create: `OpenHandle`, `OpenFileHandle`
- path ops: `MkdirAll`, `Remove`, `RemoveAll`, `Rename`, `RenameNoReplace`, `Stat`, `ReadDir`, `Glob`
- helpers: `ReadFile`, `WriteFile`, `Truncate`, `Hostname`

`vfs.File` includes:

- read/write/seek APIs
- `Sync`, `Truncate`, `Close`, `Stat`, `Name`

`vfs.Ensure(fs)` maps nil to `OSFS`.

---

## 2. Rename Semantics

`Rename`:

- normal rename/move semantics.
- target replacement behavior is platform-dependent (`os.Rename` behavior).

`RenameNoReplace`:

- contract: fail with `os.ErrExist` when destination already exists.
- on Linux, uses `renameat2(..., RENAME_NOREPLACE)` when supported.
- fallback path (`renameNoReplaceFallback`) checks destination existence then calls `Rename`.

Important: fallback is **not atomic** and has TOCTOU risk. Callers needing strict no-overwrite atomicity should avoid relying on fallback behavior.

---

## 3. Directory Sync Helper

`vfs.SyncDir(fs, dir)` fsyncs directory metadata to persist entry updates (create/rename/remove).

This is used in strict durability paths (for example SST install before manifest publication) to guarantee directory entry persistence.

---

## 4. Fault Injection (`FaultFS`)

`FaultFS` wraps an underlying `FS` and can inject failures by operation/path.

- Rule helpers: `FailOnceRule`, `FailOnNthRule`, `FailAfterNthRule`, `FailOnceRenameRule`
- File-handle faults: write/sync/close/truncate
- Rename fault matching supports `src`/`dst` targeting

Used to test manifest/WAL/recovery failure paths deterministically.

---

## 5. Current Implementation Set

- `OSFS`: production implementation (Go `os` package).
- `FaultFS`: failure-injection wrapper over any `FS`.

No in-memory FS is included yet.

---

## 6. Design Notes

- Keep storage code decoupled from direct `os.*` calls.
- Make crash/failure tests reproducible.
- Keep API minimal and only add operations required by real storage call sites.

References:

- Pebble VFS: <https://pkg.go.dev/github.com/cockroachdb/pebble/vfs>
- Pebble errorfs: <https://pkg.go.dev/github.com/cockroachdb/pebble/vfs/errorfs>
