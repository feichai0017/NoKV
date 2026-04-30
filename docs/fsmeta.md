# FSMetadata

## TL;DR

- Topic: NoKV's namespace metadata substrate.
- Core objects: Mount, Inode, Dentry, SubtreeAuthority, SnapshotEpoch, QuotaFence, UsageCounter.
- Call chain: `fsmeta/client -> fsmeta/server -> fsmeta/exec -> TxnRunner -> raftstore/percolator/coordinator`.
- Code contract: wire is in `pb/fsmeta/fsmeta.proto`, the executor is in `fsmeta/exec`, and the default NoKV runtime is `fsmeta/exec.OpenWithRaftstore`.

## 1. Conclusion

`fsmeta` is NoKV's native metadata service. It isn't a FUSE frontend, it doesn't handle object body I/O, and it doesn't promise full POSIX. What it provides is a metadata substrate that distributed filesystems, object-storage namespaces, and AI dataset metadata can all reuse.

The value of this layer isn't picking a few keys to encode inode/dentry. The real boundary is: common namespace operations are exposed as server-side primitives, instead of asking each upper-layer application to stitch a protocol out of `Get` / `Put` / `Scan`.

## 2. Current API

The current v1 API is defined by `pb/fsmeta/fsmeta.proto`. `fsmeta/server` exposes gRPC; `fsmeta/client` provides a Go typed client.

| RPC | Current semantics |
|---|---|
| `Create` | Atomically creates a dentry and inode; the server uses `AssertionNotExist` to reject duplicate creation. |
| `Lookup` | Read a dentry by `(mount, parent_inode, name)`. |
| `ReadDir` | Scan one directory page by dentry prefix. |
| `ReadDirPlus` | Scan dentries and batch-read inode attrs under the same snapshot version. |
| `WatchSubtree` | A prefix-scoped change feed; supports ready, ack, back-pressure, and bounded cursor replay. Cursor expiry requires full-state reconcile. |
| `SnapshotSubtree` | Publishes an MVCC read-version token; subsequent `ReadDir` / `ReadDirPlus` can use it to read that version. Data-plane GC retention is a separate boundary. |
| `RetireSnapshotSubtree` | Proactively retire a snapshot epoch. |
| `GetQuotaUsage` | Read the persistent quota usage counter for a mount/scope. |
| `RenameSubtree` | Atomically move the root dentry of a subtree; descendants follow naturally via inode references. |
| `Link` | Create a second dentry for an existing non-directory inode and increment link count in the same transaction. |
| `Unlink` | Delete a dentry; decrement link count and delete the inode record when the last link is removed. |

## 3. Data model

`fsmeta`'s key schema is in `fsmeta/keys.go`; value schema is in `fsmeta/value.go`.

| Object | Storage location | Notes |
|---|---|---|
| Mount metadata key | `EncodeMountKey` | Reserved mount-level data key; mount lifecycle truth does not live here. |
| Inode | `EncodeInodeKey(mount, inode)` | File/directory attributes, including `size`, `mode`, `link_count`, and a bounded `opaque_attrs` payload. |
| Dentry | `EncodeDentryKey(mount, parent, name)` | Mapping from parent/name to inode. |
| Chunk | `EncodeChunkKey(mount, inode, chunk)` | Schema is in place; the current fsmeta API doesn't expose object body / chunk I/O. |
| Session | `EncodeSessionKey(mount, session)` | Schema reserved for later session/lease use. |
| Usage | `EncodeUsageKey(mount, scope)` | Quota usage counter; scope=0 means mount-wide, non-zero means a direct accounting scope. |

Both keys and values carry a magic + schema version. Values use a hand-written binary layout — not JSON.

`InodeRecord.opaque_attrs` is application-owned bytes capped at 16 KiB. It is intended for compact body references, checksums, content type, or a caller-defined protobuf payload. NoKV stores and returns it, but does not parse, index, authorize, or quota it separately.

## 4. Execution boundary

`fsmeta/exec.Executor` depends on a single narrow interface:

```go
type TxnRunner interface {
    ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
    Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
    BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
    Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
    Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error
}
```

The default runtime uses `OpenWithRaftstore` to wire up coordinator, raftstore client, TSO, watch source, mount/quota cache, snapshot publisher, and subtree handoff publisher. Embedded users can use this entry point directly; tests and custom deployments can keep passing in their own `TxnRunner`.

`OpenWithRaftstore` can also wire two derived slab caches when explicitly configured:

- `NegativeCacheDir` enables a persistent negative dentry cache. It remembers recent lookup misses and invalidates on namespace mutations.
- `DirPageCacheDir` enables a ReadDirPlus page cache. It materializes fused dentry+inode pages and invalidates by parent directory epoch.

Both caches are derived state. They can be dropped or rebuilt without changing authoritative namespace truth.

The layering constraints are:

- `Executor` does not directly know about raft region / store routing.
- `OpenWithRaftstore` is NoKV's default adapter; it owns the raftstore wiring.
- `meta/root` does not store high-frequency inode/dentry data — only lifecycle / authority truth.
- `raftstore` and `percolator` don't understand fsmeta semantics; they only provide transactions and apply observation.

## 5. Native primitives

### ReadDirPlus

`ReadDirPlus` is the most direct shape advantage today: one dentry scan plus one `BatchGet` of inode attrs, all read under the same snapshot version. A generic-KV baseline has to do a point lookup per dentry after the scan, producing N+1.

Strict semantics: if any inode is missing or fails to decode, the whole page returns an error. fsmeta does not return half-true directory pages.

### WatchSubtree

`WatchSubtree` subscribes to an fsmeta key prefix and externally exposes a `(region_id, term, index)` cursor and a `commit_version`. Event sources include:

- a successful `CMD_COMMIT`;
- `CMD_RESOLVE_LOCK` with `commit_version != 0`.

`CMD_PREWRITE`, rollback, and diagnostic commands do not produce visible events.

v1 already supports:

- ready signal;
- back-pressure window;
- ack;
- per-region recent ring;
- resume cursor replay;
- `ErrWatchCursorExpired` when a cursor has expired.

`ErrWatchCursorExpired` is not a fatal protocol state. The client-side recovery
pattern is: open a fresh watch without the old cursor, take a full `ReadDirPlus`
baseline for the watched directory, then apply live events idempotently. The Go
typed client exposes this as `client.WatchDirectoryWithReconcile`.

### SnapshotSubtree

`SnapshotSubtree` only publishes a read epoch — it does not copy the directory tree. The token shape is `(mount, root_inode, read_version)`. Subsequent `ReadDir` / `ReadDirPlus` use `snapshot_version` to read the same MVCC view.

`SnapshotEpochPublished` / `SnapshotEpochRetired` rooted events are already in place. Coordinator root views now carry active snapshot epochs and expose the oldest active `read_version` as the MVCC-GC retention floor. The current data-plane compactor does not yet drop MVCC history by read-version, so this is the enforcement hook for a future version-GC worker rather than a destructive cleanup path today.

### RenameSubtree

The current dentry schema references the parent directory via `parent_inode_id`, so a subtree rename's physical write volume is the same as a regular rename: delete the old root dentry, write the new root dentry. Descendants don't have to be rewritten one by one.

The extra semantics live in the authority layer:

- publish `SubtreeHandoffStarted` before mutation;
- the dentry mutation goes through Percolator 2PC;
- publish `SubtreeHandoffCompleted` after mutation;
- the runtime monitor uses `WatchRootEvents` to discover pending handoffs and complete them.

This design prioritizes guaranteeing that rooted authority can never get stuck in an unknown state forever. In extreme cases it may advance an empty era — but it will not leave behind an orphaned pending handoff that no one will repair.

### Link / Unlink

`Link` is allowed only for non-directory inodes. It creates a new dentry and increments `InodeRecord.LinkCount` in the same transaction.

`Unlink` deletes one dentry and updates the inode based on link count:

- link count > 1: decrement and write back the inode;
- link count ≤ 1: delete the inode record.

Hard links to directories remain illegal.

### Quota Fence

The quota fence is rooted truth; the usage counter is a data-plane key. The write path packs the usage counter mutation and the dentry/inode mutation into the same Percolator transaction.

This solves two problems:

- multiple `nokv-fsmeta` gateways won't each maintain a local counter and breach the limit;
- a gateway restart won't lose usage.

Fence changes are pushed to the fsmeta runtime via the coordinator root-event stream; on cache miss, the runtime falls back to querying the coordinator.

## 6. Rooted truth vs runtime view

| Domain | Rooted truth | Runtime view |
|---|---|---|
| Mount | `MountRegistered` / `MountRetired` | fsmeta mount admission cache; a retired mount closes related watch subscriptions. |
| Subtree authority | `SubtreeAuthorityDeclared` / `SubtreeHandoffStarted` / `SubtreeHandoffCompleted` | RenameSubtree frontier, pending handoff repair. |
| Snapshot epoch | `SnapshotEpochPublished` / `SnapshotEpochRetired` | snapshot-version reads. |
| Quota fence | `QuotaFenceUpdated` | quota fence cache + persisted usage counter keys. |
| WatchSubtree | Not in `meta/root` | raftstore apply observer + fsmeta router. |

On startup, `nokv-fsmeta` first pulls `ListMounts` / `ListQuotaFences` / `ListSubtreeAuthorities` for bootstrap, then follows subsequent changes via `WatchRootEvents`. `MonitorInterval` is the reconnect backoff after the root-event stream drops — not a steady-state polling interval.

## 7. Deployment

Docker Compose brings up meta-root, coordinator, raftstore, and fsmeta gateway, and registers a default mount via `mount-init`:

```bash
docker compose up -d
```

Start the fsmeta gateway directly:

```bash
go run ./cmd/nokv-fsmeta \
  --addr 127.0.0.1:8090 \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --metrics-addr 127.0.0.1:9400 \
  --negative-cache-dir ./artifacts/fsmeta/negative-cache \
  --dirpage-cache-dir ./artifacts/fsmeta/dirpage-cache
```

Register a mount:

```bash
nokv mount register \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --mount default \
  --root-inode 1 \
  --schema-version 1
```

Set quota:

```bash
nokv quota set \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --mount default \
  --limit-bytes 10737418240 \
  --limit-inodes 10000000
```

## 8. Metrics

`nokv-fsmeta --metrics-addr` exposes four expvar groups:

| Namespace | Meaning |
|---|---|
| `nokv_fsmeta_executor` | transaction retry / retry exhausted. |
| `nokv_fsmeta_watch` | subscribers, events, delivered, dropped, overflow, remote source state. |
| `nokv_fsmeta_mount` | mount cache hit/miss, admission rejects. |
| `nokv_fsmeta_quota` | fence check/reject, cache hit/miss, fence updates, usage mutations. |

When the derived caches are enabled, `nokv_fsmeta_executor` also reports whether
NegativeCache / DirPage are active and the DirPage hit/miss/store counters.

## 9. Benchmarks

The fsmeta benchmark lives in `benchmark/fsmeta`. The core comparison is two paths against the same NoKV cluster:

| Driver | Behavior |
|---|---|
| `native-fsmeta` | Calls the fsmeta typed API. |
| `generic-kv` | Uses the same raftstore/percolator substrate but stitches the metadata schema on the client. |

Stage 1 headline: `ReadDirPlus` average latency 12.0 ms vs 510.3 ms — about 42.5×. Result CSVs are in `benchmark/fsmeta/results/`.

The WatchSubtree evidence workload lives in the same benchmark package; `watch_notify` reaches sub-second p95 on a Docker Compose 3-node cluster.

Derived-cache evidence uses the same harness:

- `hotspot-fanin` with `ReadDirPlus` measures the DirPage path when `nokv-fsmeta --dirpage-cache-dir ...` is enabled.
- `negative-lookup` repeatedly probes missing dentries and measures the NegativeCache path when `nokv-fsmeta --negative-cache-dir ...` is enabled.

## 10. Non-goals

- No FUSE / NFS / SMB frontend.
- No S3 HTTP gateway or object body I/O.
- No indexing or interpretation of `InodeRecord.opaque_attrs`.
- No write of every inode/dentry mutation into `meta/root`.
- No recursive materialized snapshot — `SnapshotSubtree` is an MVCC read epoch.
- No claim that data-plane MVCC version GC already deletes or retains by snapshot epoch; active epochs are materialized as a retention floor for the future GC worker.
