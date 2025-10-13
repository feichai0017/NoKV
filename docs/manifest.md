# Manifest & VersionEdit Design

## 1. 目标

1. **一致性**：所有 SST/WAL/ValueLog 元数据通过 manifest 序列化，保证 crash 后恢复到最新一致状态。
2. **原子性**：采用 VersionEdit + CURRENT 文件，确保 manifest 切换时要么完成，要么可以回滚。
3. **可扩展**：支持后续添加列族、多表、快照等元信息。

---

## 2. 文件结构

```
WorkDir/
  CURRENT             -> manifest 文件名（例如 MANIFEST-000123）
  MANIFEST-000123     VersionEdit 序列，记录 AddFile/DeleteFile/WAL 指针等
  MANIFEST-000124     后续生成的新文件
  logs/00001.wal      WAL 段文件
  tables/00001.sst    SST 文件
  vlog/00001.vlog     ValueLog 段
  tmp/                临时文件目录（flush/compaction/manifest rewrite）
```

CURRENT 是一个小文件，保存当前活跃 manifest 名称。更新 CURRENT 时使用 `fsync` 确保原子性。

---

## 3. VersionEdit 格式（初稿）

每个 VersionEdit 记录以下操作集合：

```
message VersionEdit {
  repeated FileEdit file_edits = 1;
  optional uint32 log_segment = 2;   // 最新 WAL 段
  optional uint64 log_offset = 3;    // 该段写入偏移
  repeated ValueLogEdit vlog_edits = 4;
  optional bytes checksum = 5;
}

message FileEdit {
  enum Type { ADD = 0; DELETE = 1; }
  Type op = 1;
  uint32 level = 2;
  uint64 file_id = 3;
  uint64 size = 4;
  bytes smallest = 5;
  bytes largest = 6;
  uint64 created_at = 7;
}

message ValueLogEdit {
  enum Type { GC_REMOVE = 0; HEAD_UPDATE = 1; }
  Type op = 1;
  uint32 fid = 2;
  uint64 offset = 3;
}
```

实际可根据需要扩展（例如记录 Bloom checksum、compaction 输出等）。

---

## 4. 状态机

### 4.1 VersionManager

```
type Manager interface {
    Recover() (Version, error)
    LogEdit(edit VersionEdit) error
    Current() Version
    Close() error
}

type Version struct {
    Levels map[int][]FileMeta
    LogSegment uint32
    LogOffset uint64
    ValueLogs map[uint32]ValueLogMeta
}
```

关键流程：
1. **Recover**：读取 CURRENT → 打开 manifest → 依次读取 VersionEdit → 应用到内存 Version。
2. **LogEdit**：将新的 edit 追加到 manifest，并在成功后更新内存 Version。必要时 rewrite（当日志太大/删除过多时）。
3. **Rewrite**：生成全量 snapshot manifest（与 RocksDB 类似），写临时文件并原子 rename + 更新 CURRENT。

### 4.2 Flush 集成

Flush 安装阶段的操作：
1. 构建 `VersionEdit`：AddFile(level=0, file_id,范围)，同时记录当前 WAL 段信息（log_segment/log_offset）用作 checkpoint。
2. 调用 `LogEdit`，成功后才 rename 临时 SST → 正式文件。
3. Stage Release 时再 `wal.RemoveSegment` 等操作。

崩溃恢复：
- 若 manifest 中存在 AddFile，但 SST 文件不存在／rename 未完成 ⇒ 删除该 Edit（可在恢复阶段检测并清理）。
- 若 manifest 缺失 AddFile，但磁盘有临时文件 ⇒ 重新安装或清理临时文件后重试 flush。

---

## 5. 崩溃恢复流程

1. 读取 CURRENT → 打开MANIFEST → 重放 VersionEdit，构建 Version。
2. 根据 Version 构造 level handler、SST 缓存、ValueLog 状态。
3. 通过 WAL 重放（从 Version.LogSegment/Offset 之后开始），恢复 memtable。
4. 如果存在未完成 flush 任务（manifest 中有 AddFile 但 WAL 段未释放，或者 tmp SST 仍在），需要根据临时文件/metadata 重新提交 flush 队列。

---

## 6. 测试矩阵

1. **顺序操作**：AddFile/DeleteFile/WAL 指针更新，恢复后 Version 正确。
2. **Rewrite**：触发 rewrite，确保 CURRENT 原子切换；模拟 rewrite 崩溃（tmp 文件存在、CURRENT 指向旧文件）。
3. **Flush 崩溃**：
   - VersionEdit 写入后崩溃：重启时 manifest 已记录 AddFile，应完成 Release。
   - 生成临时 SST 后崩溃，manifest 未记录：恢复流程应清理 temp 并重新 flush。
4. **WAL 指针**：确保 log_segment/log_offset 记录准确；恢复后 WAL replay 从正确位置开始。
5. **ValueLog 编辑**：记录 GC 删除、head 更新；恢复后 GC 状态一致。

---

## 7. 实现步骤建议

1. 在 `manifest/` 目录实现 `Manager` + `VersionEdit` 编解码（可以使用 protobuf 或自定义编码）。
2. 调整 `levelManager.flush` 使用 `LogEdit` 记录数据，而不是直接操作 manifest 结构。
3. 重写 `lsm.initLevelManager`/`recovery`，使用 `Manager.Recover()` 获取 Version。
4. 加入 rewrite 逻辑，控制 manifest 大小。
5. 单元测试覆盖上述矩阵；增加命令行工具（后续阶段）支持 dump manifest。

---

## 8. 后续扩展

- 记录 compaction 历史（从哪个 level→哪个 level）。
- 支持列族（Column Family）：Version 中携带多 CF 的 FileEdit。
- 与 ValueLog 进一步协作：GC 计划、discard stats checkpoint。
- 提供 manifest dump 工具，便于运维分析。

完成 Manifest 重构后，可继续 ValueLog 重构，确保 WAL→MemTable→Flush→Manifest→ValueLog 整体链路闭合。 

