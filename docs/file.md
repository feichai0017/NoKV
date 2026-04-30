# File and mmap primitives

The `engine/file` package provides low-level file and mmap helpers shared by
WAL, SST, and slab consumers.

---

## 1. Components

| Type | Role |
| --- | --- |
| `file.Options` | Common open parameters: FID, path, flags, max size, and VFS |
| `MmapFile` | Portable mmap-backed file with append, sync, truncate, delete, and remap helpers |
| `vfs.FS` | Filesystem abstraction used by tests for deterministic fault injection |

---

## 2. Sync and Truncation

- `MmapFile.Sync` flushes dirty pages.
- `MmapFile.Truncate` changes the physical file size and remaps when needed.
- `MmapFile.Delete` unmaps, closes, and removes the file.
- Directory sync is handled through `vfs.SyncDir` when callers need rename
  publication safety.

---

## 3. Current Users

| Layer | Usage |
| --- | --- |
| WAL | Segment files and fsync/rotation tests |
| SST | mmap-backed table files and block reads |
| slab | append-only sidecar segments for namespace-derived caches |

The file layer intentionally does not encode storage semantics such as WAL
record headers, SST block formats, or slab consumer payloads.
