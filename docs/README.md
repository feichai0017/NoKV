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
    <a href="https://dbdb.io/db/nokv">
      <img alt="DBDB.io" src="https://img.shields.io/badge/dbdb.io-listed-2f80ed" />
    </a>
  </p>

  <p>
    <!-- Meta -->
    <img alt="Go Version" src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white" />
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
    <p><strong>Lock-light</strong> commit queue and <strong>Batch WAL</strong> writing deliver write throughput that saturates NVMe SSDs.</p>
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

Latest full baseline (generated on 2026-03-11 with default `make bench` profile: records=1M, ops=1M, conc=16, value_size=1000, workloads A-F, engines NoKV/Badger/Pebble):

| Workload | NoKV (ops/s) | Badger (ops/s) | Pebble (ops/s) |
| :--- | ---: | ---: | ---: |
| YCSB-A | 481,609 | 253,610 | 199,619 |
| YCSB-B | 1,162,246 | 445,283 | 322,322 |
| YCSB-C | 914,103 | 527,690 | 178,969 |
| YCSB-D | 1,210,300 | 459,614 | 577,120 |
| YCSB-E | 271,677 | 40,402 | 99,485 |
| YCSB-F | 364,485 | 177,261 | 206,718 |

<details>
<summary><em>Click to view full benchmark summary</em></summary>

```text
NoKV    YCSB-A 481609   YCSB-B 1162246  YCSB-C 914103   YCSB-D 1210300  YCSB-E 271677  YCSB-F 364485
Badger  YCSB-A 253610   YCSB-B 445283   YCSB-C 527690   YCSB-D 459614   YCSB-E 40402   YCSB-F 177261
Pebble  YCSB-A 199619   YCSB-B 322322   YCSB-C 178969   YCSB-D 577120   YCSB-E 99485   YCSB-F 206718
```
</details>

Raw reports are generated locally under `benchmark/benchmark_results/` (gitignored).

<br>

## 🏗️ Architecture

```mermaid
graph TD
    Client["Client / Redis"] -->|RESP Protocol| Gateway["Redis Gateway"]
    Gateway -->|RaftCmd| RaftStore
    
    subgraph "RaftStore (Distributed Layer)"
        RaftStore -->|Propose| RaftLog["Raft Log (WAL)"]
        RaftLog -->|Consensus| Apply["Apply Worker"]
    end
    
    subgraph "Storage Engine (LSM)"
        Apply -->|Batch Set| MemTable
        MemTable -->|Flush| SSTable["SSTables (L0-L6)"]
        SSTable -->|Compact| SSTable
        
        Apply -->|Large Value| VLog["Value Log"]
    end
    
    subgraph "Cache Layer"
        BlockCache["Block Cache (Ristretto)"] -.-> SSTable
        IndexCache["Index Cache (W-TinyLFU)"] -.-> SSTable
    end
```

<div align="center">
  <sub>Built with ❤️ by <a href="https://github.com/feichai0017">feichai0017</a> and contributors.</sub>
</div>
