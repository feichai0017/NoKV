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
    - [Backend](#backend)
    - [Frontend](#frontend)
    - [Infrastructure](#infrastructure)
  - [Implementation Details](#implementation-details)
    - [Raft Consensus](#raft-consensus)
    - [Sharding Manager](#sharding-manager)
    - [Key-Value Store](#key-value-store)
  - [Installation](#installation)
  - [Usage](#usage)
    - [API Examples](#api-examples)
    - [Command Line Interface](#command-line-interface)
  - [Contributing](#contributing)
  - [License](#license)

## Overview

This project implements a distributed file system using Go, incorporating key concepts from MIT's 6.5840 distributed systems course. It features a robust key-value storage framework with Raft consensus, sharding, and fault tolerance mechanisms.

## Architecture

The system is built on three main layers:
1. **Client Layer**: RESTful API interface and web frontend
2. **Consensus Layer**: Raft-based consensus mechanism for consistency
3. **Storage Layer**: Distributed key-value store with sharding

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

### Backend
- **Language**: Go 1.21+
- **Framework**: Gin
- **Consensus Protocol**: Raft implementation
- **Storage**: 
  - MinIO for object storage
  - Redis for caching
  - MySQL for metadata

### Frontend
- **Framework**: React.js
- **State Management**: Redux
- **UI Components**: Material-UI

### Infrastructure
- **Containerization**: Docker
- **Orchestration**: Kubernetes
- **Service Discovery**: Consul
- **Monitoring**: Prometheus & Grafana

## Implementation Details

### Raft Consensus
```go
type RaftNode struct {
    mu sync.Mutex
    peers []string
    currentTerm int
    votedFor int
    log []LogEntry
    // ... other Raft-specific fields
}
```

### Sharding Manager
```go
type ShardManager struct {
    mu sync.Mutex
    shards map[int]*Shard
    config *ShardConfig
    // ... sharding-related fields
}
```

### Key-Value Store
```go
type KVStore struct {
    mu sync.Mutex
    db map[string]string
    raft *RaftNode
    // ... storage-related fields
}
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/distributed-file-system.git
cd distributed-file-system
```

2. Install dependencies:
```bash
go mod tidy
```

3. Configure the system:
```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your settings
```

4. Start the services:
```bash
docker-compose up -d
```

## Usage

### API Examples

```go
// Initialize client
client := dfs.NewClient("localhost:8080")

// Store file
err := client.Put("key", data)

// Retrieve file
data, err := client.Get("key")

// Delete file
err := client.Delete("key")
```

### Command Line Interface
```bash
# Start a node
./dfs-server --port 8080 --raft-port 9000

# Add a new node to cluster
./dfs-cli join --addr localhost:8080
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
