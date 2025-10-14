# 🚀 NoKV – High-Performance LSM KV Engine

<div align="center">
  <img src="./img/logo.svg" width="220" alt="NoKV Logo" />
  <p>
    <a href="https://github.com/feichai0017/NoKV/actions">
      <img src="https://img.shields.io/badge/status-active-success.svg" alt="Status"/>
    </a>
    <img src="https://img.shields.io/badge/go-1.23+-blue.svg" alt="Go Version"/>
    <img src="https://img.shields.io/badge/license-Apache%202.0-yellow.svg" alt="License"/>
    <img src="https://img.shields.io/badge/version-1.0.0-blue.svg" alt="Version"/>
  </p>
  <p><strong>LSM Tree • ValueLog • MVCC • Hot-Key Tracking</strong></p>
</div>

---

## ✨ Highlights

- 🔁 **LSM + ValueLog** hybrid design inspired by RocksDB & Badger  
- ⚡ **MVCC transactions** with snapshot isolation & conflict detection  
- 🔥 **Hot Ring collector** for real-time hot-key analytics (exposed via `nokv stats`)  
- ♻️ **Resilient recovery**: WAL / Manifest / ValueLog replay with scripted scenarios  
- 🛠️ **CLI toolchain**: inspect stats, manifest, vlog segments in seconds  

---

## 🚀 Quick Start

```bash
go get github.com/feichai0017/NoKV

go test ./...                   # 单元 + 集成测试
./scripts/recovery_scenarios.sh # 一键覆盖 WAL/Manifest/ValueLog 恢复矩阵
```

> 运行脚本时默认输出结构化 `RECOVERY_METRIC` 日志至 `artifacts/recovery/`，方便集成 CI。

---

## 🧱 Architecture Glimpse

| 模块 | 亮点 |
| ---- | ---- |
| **WAL** | 顺序写 + 段切换；崩溃后重放 MemTable |
| **MemTable** | SkipList + Arena；flush pipeline 四阶段状态机 |
| **SSTable** | leveled/size-tiered 混合 compaction；索引、Bloom 缓存 |
| **ValueLog** | 大 value 分离、GC 重写、head 指针持久化 |
| **Oracle / Txn** | MVCC 时间戳、冲突检测、事务迭代器快照 |
| **Hot Ring** | 读路径统计热点 key， Stats/CLI 输出 Top-N，为缓存/调度提供信号 |

完整设计详见 [docs/architecture.md](docs/architecture.md)。

---

## 🔍 Observability & Recovery

- `cmd/nokv stats`：离线/在线拉取 backlog 指标、热点 Key、ValueLog 状态  
- `cmd/nokv manifest` / `cmd/nokv vlog`：检查 manifest 层级与 vlog 段  
- `scripts/recovery_scenarios.sh`：覆盖 WAL 重放、缺失 SST、ValueLog 截断等场景  
- `RECOVERY_TRACE_METRICS=1`：调试模式输出结构化恢复指标  

---

## 📊 Benchmarking

- `go test ./benchmark -run TestBenchmarkResults -count=1`  
  生成 NoKV vs Badger 写入/读取/批量/范围扫描对比，并写入 `benchmark/benchmark_results/*.txt`  
- RocksDB 对比：  
  ```bash
  go env -w CGO_ENABLED=1
  go get github.com/tecbot/gorocksdb
  go test -tags benchmark_rocksdb ./benchmark -run TestBenchmarkResults -count=1
  ```

---

## 🛠️ Development Guide

| 项目 | 说明 |
| ---- | ---- |
| 语言 | Go 1.23+ |
| 测试 | 提交前请确保 `go test ./...` 全绿，并补充针对性用例 |
| 性能 | `benchmark/` 提供基准测试骨架，欢迎扩展 workload |
| 贡献 | 欢迎 PR，与我们一同完善下一代 Go KV 引擎 🧑‍💻 |

相关文档：
- [Architecture & Design Overview](docs/architecture.md)
- [Testing & Validation Plan](docs/testing.md)
- [Crash Recovery Verification](docs/recovery.md)
- [Flush Pipeline](docs/flush.md)
- [Manifest & VersionEdit](docs/manifest.md)

---

## 📄 License

Apache-2.0. 详见 [LICENSE](LICENSE)。

<div align="center">
  <sub>Made with ❤️ for high-throughput, embeddable storage.</sub>
</div>
