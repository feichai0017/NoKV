# NoKV

<p align="center">
  <img src="./Frontend/public/images/logo.png" alt="Project Logo" width="200"/>
</p>

![Status](https://img.shields.io/badge/status-active-success.svg)
![Language](https://img.shields.io/badge/language-go1.23-blue.svg)
![License](https://img.shields.io/badge/license-Apache2.0-yellow.svg)
![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)

A high-performance embedded key-value storage engine based on LSM Tree with MVCC transaction support.

## Table of Contents
- [NoKV](#nokv)
  - [Table of Contents](#table-of-contents)
  - [Architecture](#architecture)
    - [Core Components](#core-components)
    - [Key Features](#key-features)
  - [Performance Optimizations](#performance-optimizations)
  - [Future Plans](#future-plans)
    - [Distributed Implementation](#distributed-implementation)
  - [Implementation Details](#implementation-details)
    - [LSM-tree Structure](#lsm-tree-structure)
  - [Installation](#installation)
  - [Contributing](#contributing)
  - [License](#license)

## Architecture

### Core Components

1. **Storage Engine**
   - LSM Tree based storage
   - Separated value log for large values
   - Two-phase compaction strategy
   - Bloom filters for efficient lookups

2. **Transaction System**
   - MVCC (Multi-Version Concurrency Control)
   - Optimistic concurrency control
   - Conflict detection
   - Timestamp-based versioning

3. **Memory Management**
   - SkipList-based MemTable
   - Two-phase memory to disk transition
   - Configurable memory limits
   - Efficient garbage collection

### Key Features

- ACID transactions
- Range queries support
- Configurable compression
- Write-ahead logging
- Crash recovery
- Efficient compaction
- Iterator support

## Performance Optimizations

1. **Memory Optimization**
   - Separate storage for large values
   - Bloom filters for negative lookups
   - Efficient memory table implementation

2. **Disk Optimization**
   - Batch processing for writes
   - Level-based compaction
   - Sequential disk writes
   - Efficient merge operations

3. **Concurrency**
   - Lock-free read operations
   - Concurrent compaction
   - Parallel transaction processing

## Future Plans

### Distributed Implementation
The next phase will extend NoKV into a distributed system with:

1. **Consensus Layer**
   - Raft-based replication
   - Leader election
   - Log synchronization

2. **Sharding**
   - Range-based sharding
   - Dynamic shard rebalancing
   - Cross-shard transactions

3. **Cluster Management**
   - Node discovery
   - Health monitoring
   - Auto failover
## Implementation Details

### LSM-tree Structure
```go
// Core components of the LSM-tree
type levelManager struct {
    levels []*levelHandler  // Multiple levels of SSTable storage
    cache  *cache          // Two-level cache system
    opt    *Options        // Configuration options
}
```

## Installation

```bash
go get github.com/feichai0017/NoKV
```



## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
