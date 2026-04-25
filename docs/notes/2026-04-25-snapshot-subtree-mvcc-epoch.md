# 2026-04-25 SnapshotSubtree：subtree-scoped MVCC epoch

## 结论

`SnapshotSubtree` 是 fsmeta 的原生 snapshot primitive。它不复制目录树，也不把 dentry / inode 写入 `meta/root`。当前语义很窄：

1. fsmeta 从 coordinator TSO 取得一个 `read_version`。
2. fsmeta 把 `(mount, root_inode, read_version)` 作为 `SnapshotSubtree` token 返回给调用方。
3. 调用方之后用这个 token 做 `ReadDir` / `ReadDirPlus`，读到的是同一个 MVCC snapshot。
4. fsmeta 同时把 `SnapshotEpochPublished` 事件写入 rooted truth，用于后续 retention / audit / namespace authority 扩展。

这条 primitive 的核心价值是数据集版本化：AI training / object namespace / DFS frontend 可以先发布一个稳定的 subtree epoch，再让 reader 在之后任意时间以同一个版本读目录页。

## 不做什么

v0 明确不做：

- 不做物化 snapshot copy。
- 不做 recursive subtree traversal。
- 不做历史 catch-up watch。
- 不做 MVCC GC retention enforcement。当前 NoKV 还没有数据面 MVCC GC，rooted event 先记录 retention claim。
- 不做 snapshot delete / retire。后续如果引入 GC，再补 `SnapshotEpochRetired`。

## API

fsmeta wire API：

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

`snapshot_version == 0` 表示普通最新快照读；非零时，executor 不再重新 reserve TSO，而是直接按该 version 读。

## Rooted Event

rooted truth 只记录 snapshot epoch 的 authority/retention 合同：

```go
SnapshotEpochPublished{
    SnapshotID:  "mount/root/read_version",
    Mount:       "dataset-a",
    RootInode:   42,
    ReadVersion: 170000000,
}
```

这里的 `meta/root` 不是存 filesystem metadata，而是存"这个 snapshot epoch 曾被发布"这个事实。后续 GC / audit / namespace authority 都可以依赖它。

## Correctness Contract

`SnapshotSubtree` 保证：

- token 内的 `read_version` 来自 coordinator TSO。
- 对同一 token 的多次 `ReadDirPlus` 使用同一个 `read_version`。
- token 创建之后的新 dentry 不应出现在用该 token 读取的页里。
- 当前 v0 只支持直接 parent page；recursive subtree snapshot 留给后续目录树索引。

## Evidence

最小测试必须覆盖：

1. 创建 `a`。
2. 调用 `SnapshotSubtree(root)` 得到 `read_version`。
3. 创建 `b`。
4. `ReadDirPlus(snapshot_version=read_version)` 只能看到 `a`。
5. 普通 `ReadDirPlus` 能看到 `a` 和 `b`。
6. rooted snapshot 中存在对应 `SnapshotEpochPublished` 记录。

这证明 fsmeta API 层和 rooted authority 层都接上了。
