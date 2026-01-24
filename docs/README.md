# Overview

<div align="center">
  <img src="https://raw.githubusercontent.com/feichai0017/NoKV/main/img/logo.svg" width="180" alt="NoKV Logo">
  <h1>NoKV</h1>
  <p style="font-size: 1.2em; color: #666;">
    <strong>High-Performance, Cloud-Native Distributed Key-Value Database</strong>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV">
      <img src="https://img.shields.io/github/stars/feichai0017/NoKV?style=for-the-badge&color=blue" alt="Stars">
    </a>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV">
      <img src="https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go" alt="Go Version">
    </a>
    <a href="https://github.com/feichai0017/NoKV/blob/main/LICENSE">
      <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License">
    </a>
  </p>

  <p>
    <a href="getting_started.html" style="text-decoration: none;">
        <button style="background-color: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-size: 1em;">🚀 Quick Start</button>
    </a>
    &nbsp;&nbsp;
    <a href="architecture.html" style="text-decoration: none;">
        <button style="background-color: #6c757d; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-size: 1em;">🏗️ Architecture</button>
    </a>
  </p>
</div>

<br>

---

## 🔥 Why NoKV?

NoKV is designed for **modern hardware** and **distributed workloads**. It combines the best of academic research (WiscKey, W-TinyLFU) with industrial-grade engineering (Raft, Percolator).

<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-top: 20px;">

  <div style="border: 1px solid #e1e4e8; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); background-color: var(--bg);">
    <h3 style="margin-top: 0;">🏎️ Extreme Performance</h3>
    <p><strong>Lock-Free</strong> commit queue and <strong>Batch WAL</strong> writing deliver write throughput that saturates NVMe SSDs. <strong>io_uring</strong> support for Linux.</p>
  </div>

  <div style="border: 1px solid #e1e4e8; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); background-color: var(--bg);">
    <h3 style="margin-top: 0;">🧠 Smart Caching</h3>
    <p>Built-in <strong>W-TinyLFU</strong> Block Cache (via Ristretto) and <strong>HotRing</strong> implementation ensure 99% cache hit rates and adapt to skew access patterns.</p>
  </div>

  <div style="border: 1px solid #e1e4e8; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); background-color: var(--bg);">
    <h3 style="margin-top: 0;">🌐 Distributed Consistency</h3>
    <p><strong>Multi-Raft</strong> replication for high availability. <strong>Percolator</strong> model for cross-row ACID transactions. Snapshot Isolation by default.</p>
  </div>

  <div style="border: 1px solid #e1e4e8; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); background-color: var(--bg);">
    <h3 style="margin-top: 0;">🔌 Redis Compatible</h3>
    <p>Drop-in replacement for Redis. Supports the <strong>RESP protocol</strong> so you can use your existing tools and client libraries.</p>
  </div>

</div>

<br>

## 📊 Performance Benchmark

NoKV outperforms BadgerDB significantly in read-heavy and mixed workloads.

| Workload | Operation | NoKV (OPS) | Badger (OPS) | Improvement |
| :--- | :--- | :--- | :--- | :--- |
| **YCSB-C** | 100% Read | **1,540,744** | 521,586 | <span style="color:green">**+195%**</span> 🚀 |
| **YCSB-B** | 95% Read | **911,199** | 349,608 | <span style="color:green">**+160%**</span> |
| **YCSB-A** | 50% Update | **410,578** | 262,153 | <span style="color:green">**+56%**</span> |
| **YCSB-D** | 5% Insert | **1,270,717** | 707,607 | <span style="color:green">**+79%**</span> |

<details>
<summary><em>Click to view detailed latency stats</em></summary>

```text
Summary:
ENGINE  OPERATION  MODE                          OPS/S    AVG LATENCY  P99
NoKV    YCSB-C     100% read                     1540744  649ns        128µs
NoKV    YCSB-A     50/50 read/update             410578   2.435µs      155µs
Badger  YCSB-C     100% read                     521586   1.917µs      427µs
Badger  YCSB-A     50/50 read/update             262153   3.814µs      160µs
```
</details>

<br>

## 🏗️ Architecture

```mermaid
graph TD
    Client[Client / Redis] -->|RESP Protocol| Gateway[Redis Gateway]
    Gateway -->|RaftCmd| RaftStore
    
    subgraph "RaftStore (Distributed Layer)"
        RaftStore -->|Propose| RaftLog[Raft Log (WAL)]
        RaftLog -->|Consensus| Apply[Apply Worker]
    end
    
    subgraph "Storage Engine (LSM)"
        Apply -->|Batch Set| MemTable
        MemTable -->|Flush| SSTable[SSTables (L0-L6)]
        SSTable -->|Compact| SSTable
        
        Apply -->|Large Value| VLog[Value Log]
    end
    
    subgraph "Cache Layer"
        BlockCache[Block Cache (Ristretto)] -.-> SSTable
        IndexCache[Index Cache (W-TinyLFU)] -.-> SSTable
    end
```

## 🗺️ Roadmap

- [x] **Core**: LSM Tree, VLog, WAL
- [x] **Distributed**: Multi-Raft, Split/Merge
- [x] **Transaction**: Percolator (Snapshot Isolation)
- [ ] **Optimization**: Async Apply, SSTable-based Snapshot
- [ ] **Redis**: Hash/Set/ZSet support

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

<div align="center">
  <sub>Built with ❤️ by <a href="https://github.com/feichai0017">feichai0017</a> and contributors.</sub>
</div>