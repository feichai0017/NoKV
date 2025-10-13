# ValueLog Design Overview

## 1. Goals

1. **顺序写入 + 快速回放**：所有大尺寸 Value 顺序写入 ValueLog 段文件（`.vlog`），崩溃恢复时能够按段重放并恢复 head。
2. **安全 GC**：通过 discard stats 与 manifest/WAL 协同，只在确认段不再引用后执行回收。
3. **统一管理**：引入独立的 `Manager` 负责段的打开、轮转、引用计数、删除，避免业务层直接操作底层文件。

## 2. 目录结构

```
WorkDir/
  vlog/
    00001.vlog
    00002.vlog
  vlog/CURRENT.head      # 可选，记录 head checkpoint（Fid,Offset）
```

## 3. Log 记录格式

沿用现有结构：
```
| Header | Key | Value | CRC |
Header = Meta + KeyLen + ValueLen + ExpiresAt (varint 编码)
```

## 4. Manager 接口

```go
type Config struct {
    Dir         string
    SegmentSize int64
    FileMode    os.FileMode
}

type Manager interface {
    Open() error
    Close() error
    Writer() (*SegmentWriter, error)
    Reader(fid uint32) (*SegmentReader, error)
    Rotate() (uint32, error)
    Remove(fid uint32) error
    List() ([]uint32, error)
    Stats() Metrics
}
```

### 4.1 段 writer
- 负责 Append（返回 offset、长度、新 ValuePtr）。
- 达到 SegmentSize 时触发 Rotate。
- 提供 Sync/Close（对应 vlog `sync()`/`close()`）。

### 4.2 段 reader
- 单段只负责顺序读取，提供 `ReadAt(ptr)` 返回原始 WAL payload。
- 回放时从 offset + len 继续读取，遇到截断/CRC 错误即停止。

## 5. 生命周期

1. **Open**：扫描目录、构建 `fid -> LogFile` 映射、记录最大 fid。
2. **Append**：写入活跃段，必要时 `Rotate()` 创建新段。
3. **Read**：读取任意段，按 ValuePtr 定位数据。
4. **GC**：在 manifest/flush 确认段可删除后，调用 `Remove(fid)`。
5. **头部管理**：Manager 可维护 head（Fid/Offset），用于 crash 后快速跳过已重放区域。

## 6. 与现有模块协同

- **WAL**：WAL flush 为 SST/manifest 提供顺序保障；ValueLog append 前应确保 WAL 记录成功。
- **Manifest**：VersionEdit 记录最新 head / 删除的 ValueLog 段，以便恢复。
- **Flush/Compaction**：在收集 discard stats 时增加 ValueLog 引用，GC 完成后记录 delete edit。

## 7. 重构计划

1. **Manager 实现**：抽取旧 `valueLog` 中的段管理逻辑（filesMap、maxFid、create/delete 等）。
2. **Write/Read 接入**：`valueLog.newValuePtr` 与 `valueLog.read` 改用 Manager Append/Read。
3. **head & GC**：重新设计 head Checkpoint，与 manifest 协同；GC 时引用计数由 Manager 跟踪。
4. **恢复流程**：Open 时按 head + manifest 重放 ValueLog；检测不完整记录并截断。
5. **测试矩阵**：写入/读回、截断容错、GC 前后数据一致、崩溃恢复、多迭代器引用。

未来可扩展：
- 压缩/加密策略；
- 段冷热分层；
- ValueLog 快照导出。
