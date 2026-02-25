# File Abstractions

The `file` package encapsulates direct file-system interaction for WAL, SST, and value-log files. It provides portable mmap helpers, allocation primitives, and log file wrappers.

---

## 1. Core Types

| Type | Purpose | Key Methods |
| --- | --- | --- |
| [`Options`](../file/file.go#L5-L16) | Parameter bag for opening files (FID, path, size). | Used by WAL/vlog managers. |
| [`MmapFile`](../file/mmap_linux.go#L12-L98) | Cross-platform mmap wrapper. | `OpenMmapFile`, `AppendBuffer`, `Truncature`, `Sync`. |
| [`LogFile`](../file/vlog.go#L16-L130) | Value-log specific helper built on `MmapFile`. | `Open`, `Write`, `Read`, `DoneWriting`, `EncodeEntry`. |

Darwin-specific builds live alongside (`mmap_darwin.go`, `sstable_darwin.go`) ensuring the package compiles on macOS without manual tuning.

---

## 2. Mmap Management

* `OpenMmapFile` opens or creates a file, optionally extending it to `maxSz`, then mmaps it. The returned `MmapFile` exposes `Data []byte` and the underlying `*os.File` handle.
* Writes grow the map on demand: `AppendBuffer` checks if the write would exceed the current mapping and calls `Truncature` to expand (doubling up to 1 GiB increments).
* `Sync` flushes dirty pages (`mmap.Msync`), while `Delete` unmaps, truncates, closes, and removes the file—used when dropping SSTs or value-log segments.

RocksDB relies on custom Env implementations for portability; NoKV keeps the logic in Go, relying on build tags for OS differences.

---

## 3. LogFile Semantics

`LogFile` wraps `MmapFile` to simplify value-log operations:

```go
lf := &file.LogFile{}
_ = lf.Open(&file.Options{FID: 1, FileName: "00001.vlog", MaxSz: 1<<29})
ptr, _ := lf.EncodeEntry(entry, buf, offset)
_ = lf.Write(offset, buf.Bytes())
_ = lf.DoneWriting(nextOffset)
```

* `Open` mmaps the file and records current size (guarded to `< 4 GiB`).
* `Read` validates offsets against both the mmap length and tracked size, preventing partial reads when GC or drop operations shrink the file.
* `EncodeEntry` uses the shared `kv.EntryHeader` and CRC32 helpers to produce the exact on-disk layout consumed by `vlog.Manager` and `wal.Manager`.
* `DoneWriting` syncs, truncates to the provided offset, reinitialises the mmap, and keeps the file open in read-write mode—supporting subsequent appends.
* `Rewind` (via `vlog.Manager.Rewind`) leverages `LogFile.Truncate` and `Init` to roll back partial batches after errors.

---

## 4. SST Helpers

While SSTable builders/readers live under `lsm/table.go`, they rely on `file` helpers to map index/data blocks efficiently. The build tags (`sstable_linux.go`, `sstable_darwin.go`) provide OS-specific tuning for direct I/O hints or mmap flags.

---

## 5. Comparison

| Engine | Approach |
| --- | --- |
| RocksDB | C++ Env & random-access file wrappers. |
| Badger | `y.File` abstraction with mmap. |
| NoKV | Go-native mmap wrappers with explicit log helpers. |

By keeping all filesystem primitives in one package, NoKV ensures WAL, vlog, and SST layers share consistent behaviour (sync semantics, truncation rules) and simplifies testing (`file/mmap_linux_test.go`).

---

## 6. Operational Notes

* Value-log and WAL segments rely on `DoneWriting`/`Truncate` to seal files; avoid manipulating files externally or mmap metadata may desynchronise.
* `LogFile.AddSize` updates the cached size used by reads—critical when rewinding or rewriting segments.
* `vfs.SyncDir` is invoked when new files are created to persist directory entries, similar to RocksDB's `Env::FsyncDir`.

For more on how these primitives plug into higher layers, see [`docs/wal.md`](wal.md) and [`docs/vlog.md`](vlog.md).
