# 2026-04-25 SnapshotSubtree: subtree-scoped MVCC epoch

## Conclusion

`SnapshotSubtree` is fsmeta's native snapshot primitive. It does **not** copy the directory tree, and it does **not** write dentries or inodes into `meta/root`. The current semantics are deliberately narrow:

1. fsmeta fetches a `read_version` from the coordinator TSO.
2. fsmeta returns `(mount, root_inode, read_version)` as the `SnapshotSubtree` token to the caller.
3. The caller subsequently uses this token to do `ReadDir` / `ReadDirPlus`, reading from the same MVCC snapshot.
4. fsmeta also writes a `SnapshotEpochPublished` event into rooted truth, for later retention / audit / namespace-authority extension.

The core value of this primitive is dataset versioning: AI training / object namespace / DFS frontends can publish a stable subtree epoch and then let readers materialize directory pages at the same version at any later point in time.

## What it intentionally doesn't do

v0 explicitly does not:

- Materialize a copy of the snapshot.
- Do recursive subtree traversal.
- Support historical catch-up watch.
- Enforce MVCC GC retention. NoKV does not yet have a data-plane MVCC GC; the rooted event currently only records a retention claim.
- Implement snapshot delete / retire. If GC arrives later, we add `SnapshotEpochRetired`.

## API

fsmeta wire API:

```proto
message SnapshotSubtreeRequest {
  string mount = 1;
  uint64 root_inode = 2;
}

message SnapshotSubtreeResponse {
  string mount = 1;
  uint64 root_inode = 2;
  uint64 read_version = 3;
}

message ReadDirRequest {
  string mount = 1;
  uint64 parent = 2;
  string start_after = 3;
  uint32 limit = 4;
  uint64 snapshot_version = 5;
}
```

`snapshot_version == 0` means the normal "latest snapshot" read; when non-zero, the executor stops reserving a fresh TSO and reads directly at that version.

## Rooted Event

Rooted truth only records the authority/retention contract of the snapshot epoch:

```go
SnapshotEpochPublished{
    SnapshotID:  "mount/root/read_version",
    Mount:       "dataset-a",
    RootInode:   42,
    ReadVersion: 170000000,
}
```

`meta/root` here is not storing filesystem metadata — it's storing the *fact* that "this snapshot epoch was once published." Future GC / audit / namespace authority can rely on it.

## Correctness Contract

`SnapshotSubtree` guarantees:

- The `read_version` inside the token comes from coordinator TSO.
- Multiple `ReadDirPlus` calls on the same token use the same `read_version`.
- Dentries created after the token must not appear in pages read with that token.
- v0 only supports the direct parent page; recursive subtree snapshot is deferred to the future directory-tree index work.

## Evidence

The minimum test must cover:

1. Create `a`.
2. Call `SnapshotSubtree(root)`, receive `read_version`.
3. Create `b`.
4. `ReadDirPlus(snapshot_version=read_version)` sees only `a`.
5. Plain `ReadDirPlus` sees both `a` and `b`.
6. Rooted snapshot contains the matching `SnapshotEpochPublished` record.

This proves both the fsmeta API layer and the rooted authority layer are wired up.
