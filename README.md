# Distributed KV Store

<p align="center">
  <img src="./Frontend/public/images/logo.png" alt="Project Logo" width="200"/>
</p>

![Status](https://img.shields.io/badge/status-active-success.svg)
![Language](https://img.shields.io/badge/language-go1.23-blue.svg)
![License](https://img.shields.io/badge/license-Apache2.0-yellow.svg)
![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)

A distributed file system built with Go, incorporating distributed systems concepts from MIT 6.5840 course, featuring advanced key-value storage, consensus mechanisms, and fault tolerance.

## Table of Contents
- [Distributed KV Store](#distributed-kv-store)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Architecture](#architecture)
    - [Key Components:](#key-components)
  - [Key Features](#key-features)
  - [Tech Stack](#tech-stack)
    - [Core Storage Engine](#core-storage-engine)
    - [Key Components](#key-components-1)
    - [Development Tools](#development-tools)
  - [Implementation Details](#implementation-details)
    - [LSM-tree Structure](#lsm-tree-structure)
  - [Installation](#installation)
  - [Contributing](#contributing)
  - [License](#license)

## Overview

This project implements a distributed file system using Go, incorporating key concepts from MIT's 6.2840 distributed systems course. It features a robust key-value storage framework with Raft consensus, sharding, and fault tolerance mechanisms.

## Architecture

The system is built on three main layers:
1. **Consensus Layer**: Raft-based consensus mechanism for consistency
2. **Storage Layer**: Distributed key-value store with sharding

### Key Components:
- **Raft Consensus**: Implementation of the Raft protocol for leader election and log replication
- **Sharding Manager**: Handles data partitioning and distribution
- **Recovery System**: Manages node failures and data recovery
- **Distributed Lock Service**: Ensures distributed mutual exclusion

## Key Features

- **Distributed Consensus**: Raft-based leader election and log replication
- **Sharded Storage**: Horizontal scaling with dynamic sharding
- **Fault Tolerance**: Automatic recovery from node failures
- **Consistency Guarantees**: Strong consistency through Raft consensus
- **Atomic Operations**: Support for atomic multi-key transactions
- **Load Balancing**: Dynamic request distribution across nodes

## Tech Stack

### Core Storage Engine
- LSM-tree based storage structure
- Written in Go
- Optimized for write-intensive workloads

### Key Components
- Block-based SSTable format
- Two-level cache system (index & block)
- Efficient compaction strategies
- Bloom filter support

### Development Tools
- Go 1.21+
- Docker for containerization
- Make for build automation

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
