# WAL 子系统设计

> 目标：提供 RocksDB/Badger 级别的写前日志保障，支持安全重放、可控轮转以及与 MemTable/manifest 的协同。

## 1. 设计目标

1. **崩溃一致性**：所有写入在持久化到 SST 之前必须先写入 WAL；`fsync` 后即可承诺。
2. **可恢复性**：DB 重启时能够按顺序重放 WAL 段文件，恢复至最新一致状态。
3. **轮转与回收**：日志达到阈值后自动切段；在对应 MemTable/SST 落盘并记录于 manifest 后可安全删除。
4. **简单易测**：格式清晰、接口稳定，支持针对追加/轮转/回放/截断的单元测试。

## 2. 文件命名与目录

- WAL 段文件命名：`%05d.wal`（例如 `00001.wal`）。位于 `options.WorkDir/wal/`（可配置）。
- 当前活跃段：Manager 打开的最新文件，追加写；达到阈值或主动 Rotate 时生成新段。
- 回放顺序：按文件名升序遍历，忽略 manifest 中标记过的“可删除”段。

## 3. 记录格式

每条日志记录由以下部分组成：

```
uint32  length (大端编码)
[]byte  payload (已编码的 Entry)
uint32  checksum (CRC32 Castagnoli)
```

- 由上层（DB 写入流水线）决定 payload 内容。初期可复用 `utils.WalCodec`。
- 回放时先读取 length，若遇到 EOF 或剩余字节不足，则视为截断并终止该段的读取（保证幂等）。
- `checksum` 校验失败时返回错误并停止恢复。

## 4. 核心接口

定义 `wal.Manager`，负责段文件的生命周期及顺序写入。

```go
type Config struct {
    Dir           string        // 日志目录
    SegmentSize   int64         // 单段最大尺寸，默认 64 MiB
    FileMode      os.FileMode   // 创建文件权限，默认 0o666
    SyncOnWrite   bool          // 每次 Append 后是否立即 Sync
}

type EntryInfo struct {
    SegmentID uint32
    Offset    int64
    Length    uint32
}

type Manager interface {
    Append(entries ...[]byte) ([]EntryInfo, error)
    Sync() error
    Rotate() error
    Replay(func(info EntryInfo, payload []byte) error) error
    ActiveSegment() uint32
    ListSegments() ([]string, error)
    Close() error
}
```

实现要点：
- `Append` 内部自动判断是否需要 Rotate，返回每条记录的 `(segment, offset, length)`。
- `Sync` 保证活跃段数据落盘；`SyncOnWrite` 打开时 `Append` 会自动调用。
- `Rotate` 关闭当前段并创建新段；`Manager` 保持对活跃段的独占写锁。
- `Replay` 迭代所有段，对每条记录调用回调；若回调返回错误则终止。
- `ListSegments` 用于运维 / flush 管线判定哪些段可删除。

## 5. 与 MemTable/Manifest 的协作

1. 写入路径：`DB → WAL.Append → MemTable`。只有 WAL 追加成功并按需 Sync 后，才允许对外确认。
2. Flush 流程：MemTable 冻结 -> 生成 SST -> Manifest 更新 -> 标记 `wal.Segment` 可删除。
3. 回收策略：由 `FlushManager`/Manifest 管理器告知 WAL Manager 可以删除的段（例如提供 `MarkObsolete(segmentID)`）。
4. Crash 恢复：`DB.Open` 时：
   - 根据 `CURRENT` 找到最新 manifest。
   - 通过 `wal.Manager.Replay` 重放 manifest 之后的新 WAL 记录。
   - 若某段结尾被截断，回放过程自动截断；必要时更新段文件长度。

## 6. 错误处理

- **磁盘写失败**：`Append` 返回错误，DB 需停止写入并报告。
- **Sync 失败**：同上，记录错误并阻塞后续写。
- **Checksum 错误**：回放过程中若 CRC 不匹配，返回错误提示手工干预；不会尝试自动修复。
- **截断**：遇到部分记录时直接停止读取，保留已读内容，方便手动修复或重放。

## 7. 测试矩阵

1. 顺序写 / Replay 匹配。
2. 触发自动 rotate，保证生成多段并正确回放。
3. 手动截断末尾字节，Replay 保持幂等。
4. `SyncOnWrite` 与 `Sync` 行为验证。
5. 并发 Append（通过 goroutine + channel），确保不会出现数据交叉。

## 8. 后续扩展

- WAL 压缩（可选）。
- 写入批次头部（记录事务边界）。
- 与 ValueLog 的联合持久化策略（预留 `Ack` / `Commit` 标志）。
- WAL replay 的检查点机制，减少冷启动时间。

本设计为后续 MemTable/Manifest 重构的基础。接下来会实现 `wal.Manager` 及其单元测试，并逐步替换现有 `file/wal.go` 中的逻辑。 

