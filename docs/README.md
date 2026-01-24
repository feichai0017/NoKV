# Overview

<div align="center">
  <img src="https://raw.githubusercontent.com/feichai0017/NoKV/main/img/logo.svg" width="180" alt="NoKV Logo">
  <h1>NoKV</h1>
  <p style="font-size: 1.2em; color: #666;">
    <strong>High-Performance, Cloud-Native Distributed Key-Value Database</strong>
  </p>

  <p>
    <!-- Build / Quality -->
    <a href="https://github.com/feichai0017/NoKV/actions">
      <img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/go.yml?branch=main" />
    </a>
    <a href="https://codecov.io/gh/feichai0017/NoKV">
      <img alt="Coverage" src="https://img.shields.io/codecov/c/gh/feichai0017/NoKV" />
    </a>
    <a href="https://goreportcard.com/report/github.com/feichai0017/NoKV">
      <img alt="Go Report Card" src="https://img.shields.io/badge/go%20report-A+-brightgreen" />
    </a>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV">
      <img alt="Go Reference" src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" />
    </a>
    <a href="https://github.com/avelino/awesome-go#databases-implemented-in-go">
      <img alt="Mentioned in Awesome" src="https://awesome.re/mentioned-badge.svg" />
    </a>
  </p>

  <p>
    <!-- Meta -->
    <img alt="Go Version" src="https://img.shields.io/badge/go-1.24%2B-00ADD8?logo=go&logoColor=white" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://deepwiki.com/feichai0017/NoKV">
      <img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" />
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
    Gateway -->|RaftCmd| RaftStoreNode["RaftStore"]

    subgraph RaftStoreLayer["RaftStore (Distributed Layer)"]
        RaftStoreNode -->|Propose| RaftLog[Raft Log (WAL)]
        RaftLog -->|Consensus| Apply[Apply Worker]
    end

    subgraph StorageEngine["Storage Engine (LSM)"]
        Apply -->|Batch Set| MemTable
        MemTable -->|Flush| SSTables[SSTables (L0-L6)]
        SSTables -->|Compact| SSTables

        Apply -->|Large Value| VLog[Value Log]
    end

    subgraph CacheLayer["Cache Layer"]
        BlockCache[Block Cache (Ristretto)] -.-> SSTables
        IndexCache[Index Cache (W-TinyLFU)] -.-> SSTables
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
