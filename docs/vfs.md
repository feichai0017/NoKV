# VFS

The `vfs` package defines a unified filesystem abstraction used by NoKV storage paths (`wal`, `manifest`, `vlog`, `file`, and parts of `raftstore`).

It is intentionally small and focused:

- Keep production code independent from direct `os.*` calls.
- Make fault-path testing deterministic.
- Keep the same call shape in embedded and distributed modes.

---

## 1. Design Goals

1. One filesystem API for all persistence-critical modules.
2. Explicit file-handle operations (`OpenHandle` / `OpenFileHandle`) so injected failures can target both path-level and handle-level behavior.
3. Test-first error injection without adding module-local test hooks.

---

## 2. Core Interfaces

`vfs.FS` (namespace-level operations):

- open/create (`OpenHandle`, `OpenFileHandle`)
- path ops (`MkdirAll`, `Remove`, `RemoveAll`, `Rename`, `Stat`, `ReadDir`, `Glob`)
- whole-file helpers (`ReadFile`, `WriteFile`, `Truncate`)

`vfs.File` (handle-level operations):

- `Read` / `ReadAt`
- `Write` / `WriteAt`
- `Seek`
- `Sync`
- `Truncate`
- `Close`
- `Stat`
- `Name`

`Fd` is intentionally optional via `vfs.FDProvider`/`vfs.FileFD(...)` so non-OS
file implementations can still satisfy `vfs.File`.

`vfs.OSFS` is the production implementation backed by the Go `os` package.  
`vfs.Ensure(fs)` normalizes `nil` to `OSFS`, so call sites can safely accept an optional `vfs.FS`.
`vfs.SyncDir(fs, dir)` centralizes directory fsync semantics for create/rename/remove durability.

---

## 3. Fault Injection Model

NoKV provides `FaultFS` with rule-based injection:

- `FailOnceRule`
- `FailOnNthRule`
- `FailAfterNthRule`

Rules can match by operation + path, and for rename by src/dst path.

### 3.1 File-Handle-Level Injection (`FaultFile`)

When `OpenHandle`/`OpenFileHandle` succeeds, the returned handle is wrapped as `faultFile`.
`faultFile` supports deterministic failures on:

- `Write` / `WriteAt` (`OpFileWrite`)
- `Sync` (`OpFileSync`)
- `Close` (`OpFileClose`)
- `Truncate` (`OpFileTrunc`)

This is used to validate rollback and retry behavior for close/sync/truncate paths in WAL/Manifest verification and shutdown flows.

---

## 4. Where It Is Used

- `wal`: segment switch, close, verify/truncate.
- `manifest`: edit logging, rewrite, verify, close.
- `vlog`: verify/truncate and file open path.
- `file`: mmap open/sync directory path via `vfs`.
- `utils`: dir lock and utility wrappers.
- `raftstore/engine`: disk storage and snapshot import/export.

---

## 5. Comparison with Pebble VFS

NoKV's design is inspired by Pebble's VFS layering and error-injection philosophy, while intentionally keeping a smaller surface area.

| Aspect | Pebble VFS | NoKV VFS |
| --- | --- | --- |
| API scope | Broad FS API (create/open/open-dir/link/reuse/lock/list/path helpers/disk usage) | Minimal API required by NoKV storage paths |
| Wrapping model | Rich wrappers (`WithDiskHealthChecks`, `OnDiskFull`, logging wrappers) | `FaultFS` focused on deterministic fault tests |
| In-memory FS | Built-in `MemFS` / `StrictMem` for advanced test semantics | Not yet built-in (currently wraps real FS with injected errors) |
| Error injection | Dedicated `errorfs` package and injector model | Built-in policy/rule model in `FaultFS` |
| Handle-level faulting | Supports wrapped file behavior through wrappers | `FaultFile` with count-based fail on `Write/Sync/Close/Truncate` |

Practical interpretation:

- Pebble is a general-purpose storage-engine VFS toolkit.
- NoKV currently implements the subset needed for its engine and recovery semantics, with strong testability on critical failure paths.

---

## 6. Current Gaps and Next Steps

1. Add an in-memory `FS` implementation for fully isolated IO tests.
2. Add optional disk-health and slow-IO instrumentation wrappers.
3. Consider adding higher-level primitives where needed (`Lock`, `OpenDir`, hard-link/reuse helpers), but only when justified by real call sites.

---

## 7. References

- Pebble `vfs`: <https://pkg.go.dev/github.com/cockroachdb/pebble/vfs>
- Pebble `errorfs`: <https://pkg.go.dev/github.com/cockroachdb/pebble/vfs/errorfs>
