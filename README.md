# NoKV

<div align="center">
  <img src="./img/logo.svg" width="200" height="200" alt="NoKV Logo">
</div>

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
    - [Advanced Features](#advanced-features)
  - [Performance Optimizations](#performance-optimizations)
  - [Implementation Details](#implementation-details)
  - [Installation](#installation)
  - [Contributing](#contributing)
  - [License](#license)

## Architecture

### Core Components

1. **Storage Engine**
   - LSM Tree based storage
   - Whiskey KV separation for large values
   - Hot Ring cache for frequently accessed data
   - Two-phase compaction strategy
   - Bloom filters for efficient lookups

2. **Transaction System**
   - MVCC (Multi-Version Concurrency Control)
   - Optimistic concurrency control
   - Conflict detection and resolution
   - Timestamp-based versioning
   - Snapshot isolation

3. **Memory Management**
   - SkipList-based MemTable
   - Two-phase memory to disk transition
   - Configurable memory limits
   - Efficient garbage collection

### Advanced Features

1. **Whisckey KV Separation**
   - Separate storage for large values
   - Reduced write amplification
   - Efficient space utilization
   - Optimized for mixed workloads

2. **Hot Ring Cache**
   - In-memory cache for hot keys
   - LRU-based eviction policy
   - Configurable cache size
   - Automatic hot spot detection

3. **MVCC Transaction**
   - Serializable isolation level
   - Read-write conflict detection
   - Non-blocking reads
   - Lock-free implementation

## Performance Optimizations

1. **Memory Optimization**
   - Hot Ring cache for frequent access
   - Bloom filters for negative lookups
   - Efficient memory table implementation
   - Smart memory allocation

2. **Disk Optimization**
   - Batch processing for writes
   - Level-based compaction
   - Sequential disk writes
   - Efficient merge operations

3. **Concurrency**
   - Lock-free read operations
   - Concurrent compaction
   - Parallel transaction processing

## Implementation Details

The storage engine is built on a Log-Structured Merge Tree (LSM Tree) architecture, which provides excellent write performance while maintaining good read performance. The LSM Tree is organized in multiple levels, with each level containing sorted data files (SSTables). The system employs a leveled compaction strategy to manage data across different levels efficiently.

## Installation

```bash
go get github.com/feichai0017/NoKV
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
