# NoKV VFS 抽象：跨越 OS 边界的存储契约与确定性故障模拟

NoKV 的 VFS（Virtual File System）层不是为了增加复杂性，它是整个引擎实现 **“确定性可靠性”** 的核心堡垒。通过将存储语义与操作系统细节彻底解耦，NoKV 实现了跨平台的原子语义保障和极高强度的故障模拟测试。

---

## 1. 为什么工业级存储引擎需要 VFS？

在存储引擎开发中，直接依赖原生 `os` 包会带来三个致命问题：
1.  **原子语义缺失**：LSM 引擎依赖 `Rename` 的原子性来更新 Manifest。但在不同操作系统上，`Rename` 是否允许覆盖现有文件、是否保证原子性，其表现差异巨大。
2.  **测试黑盒**：如何验证磁盘在 `Sync` 时突然断电的行为？如何在不拆硬盘的情况下模拟磁盘坏道？
3.  **扩展受限**：如果未来需要接入 **分布式文件系统 (HDFS/S3)** 或者实现 **纯内存模式 (In-Memory)**，没有 VFS 将意味着需要重写整个存储内核。

NoKV 通过 `vfs.FS` 和 `vfs.File` 接口（位于 `vfs/vfs.go`），将所有的 IO 行为抽象为一套统一的契约。

---

## 2. FaultFS：精准的“故障手术刀”

`vfs/faultfs.go` 是 NoKV 最引以为傲的可靠性测试工具。它通过装饰器模式包装了标准文件系统，允许测试用例以编程方式注入各种极端故障。

### 2.1 故障策略 (FaultPolicy)
开发者可以定义极其复杂的故障场景，并观察引擎的自愈能力：
*   **FailOnce**：在操作某特定文件时触发一次错误（模拟瞬时 IO 抖动）。
*   **FailOnNth**：在第 N 次操作（如第 100 次 `Write`）时触发故障。这在验证 **崩溃恢复 (Recovery)** 的幂等性时至关重要。
*   **FailOnOp**：只在执行 `Sync` 或 `Truncate` 这种改变文件系统元数据的重型操作时触发故障。

### 2.2 实现机制解析
```go
// faultFile 的 WriteAt 实现
func (f *faultFile) WriteAt(p []byte, off int64) (int, error) {
    // 1. 在真正 IO 前，先通过 Policy 进行前置检查
    if err := f.fs.before(OpWriteAt, f.name); err != nil {
        return 0, err // 模拟故障返回
    }
    // 2. 执行真正的 OS 调用
    return f.File.WriteAt(p, off)
}
```
通过这套机制，NoKV 的测试集成功模拟了：
*   Manifest 写入一半时磁盘满。
*   SSTable 生成后 `Sync` 失败，但文件已存在的情况。
*   WAL 在回滚过程中发生权限错误的极端场景。

---

## 3. 跨平台语义抹平：原子重命名协议

LSM 引擎的命脉在于 **Manifest 的原子替换**。NoKV 在 VFS 层针对不同系统做了极致的封装。

### 3.1 Linux 平台的 RenameNoReplace
在 Linux 上，NoKV 利用了 `unix.RENAME_NOREPLACE` 系统调用。
*   **设计价值**：它保证了如果目标 Manifest 文件已存在，Rename 会直接原子地报错，而不是覆盖它。这从根本上杜绝了由于进程异常重启导致的旧元数据被误覆盖的问题。

### 3.2 Darwin 平台的模拟支持
由于 macOS 并不原生支持 `RENAME_NOREPLACE`，NoKV 在 VFS 层通过专有的 `getattrlist` 和原子判断逻辑模拟了这一行为，确保了开发者在 Mac 上也能跑出与 Linux 生产环境完全一致的逻辑闭环。

---

## 4. 性能提升：PRead/PWrite 并发契约

VFS 不仅是为了可靠性，它还解锁了高性能的 **Lock-free 并发读** 模式。

*   **PRead 语义 (ReadAt)**：`vfs.File` 强制要求实现 `ReadAt`。
*   **无竞争读取**：在 ValueLog 的读取路径中，多个查询协程可以并发地使用同一个文件描述符执行 `ReadAt`。由于 `ReadAt` 是自带 Offset 的原子操作，它不需要像 `Seek + Read` 模式那样需要获取文件句柄级别的互斥锁。
*   **结果**：在多核机器上执行大 Value 读取时，NoKV 的吞吐量随着 CPU 核心数呈完美的线性增长。

---

## 5. 存储引擎对比分析

| 特性 | NoKV | Pebble (CockroachDB) | RocksDB |
| :--- | :--- | :--- | :--- |
| **VFS 核心架构** | 精简接口 + 装饰器注入 | 深度集成 (errorfs) | 复杂的 Env/FileSystem 抽象 |
| **故障注入强度** | 强（支持路径/操作级别计数）| 极强（支持各种计数策略）| 中（依赖 Env 注入点） |
| **并发读契约** | 强制 PRead/PWrite | 深度优化 PRead | 依赖操作系统支持 |
| **跨平台原子性** | 抹平 Linux/Darwin 差异 | 通过 Go 运行时保证 | 依赖特定的插件实现 |

**总结**：VFS 并不是一种代码开销，它是 **“对每一行磁盘操作负责”** 的态度。通过 VFS，NoKV 将复杂的底层系统调用和不可预测的硬件故障，收敛为了一个可预测、可测试、可证明的确定性模型。
