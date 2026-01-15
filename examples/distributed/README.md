# NoKV Distributed Cluster Demo

This example demonstrates how to run a distributed NoKV cluster (Multi-Raft) and interact with it using a standard Go Redis client.

## 1. Prerequisites

- **Go 1.24+** installed.
- **redis-cli** (optional, for manual interaction).

## 2. Start the Cluster

We use the helper script `scripts/run_local_cluster.sh`. This script will:
1. Compile necessary binaries (`nokv`, `nokv-config`, `nokv-tso`).
2. Start 3 store nodes and a Timestamp Oracle (TSO).
3. Bootstrap the cluster using `raft_config.example.json`.

**Open Terminal 1:**

```bash
# This runs in the foreground. Keep this window open.
./scripts/run_local_cluster.sh --config raft_config.example.json
```

*Wait until you see logs indicating the servers have started (e.g., "Raft loop started").*

## 3. Start the Redis Gateway

The gateway acts as a proxy, translating Redis protocol commands into NoKV's distributed transactions.

**Open Terminal 2:**

```bash
# Connects to the cluster defined in the config
go run ./cmd/nokv-redis --addr 127.0.0.1:6380 --raft-config raft_config.example.json
```

## 4. Run the Go Client

This example uses the popular `github.com/redis/go-redis/v9` library to connect to the NoKV gateway, demonstrating standard Redis compatibility.

**Open Terminal 3:**

```bash
go run examples/distributed/client/main.go
```

**Expected Output:**

```text
Connecting to NoKV Cluster via Redis Protocol...
Connected: PONG

> SET user:10086 Gopher
Set success!

> GET user:10086
Result: Gopher

> EXPIRE user:10086 60s
Expire set: true

> INCR page_view
Counter is now: 1

> DEL user:10086
Deleted keys: 1
Verified: Key does not exist.

Demo finished successfully.
```

## 5. Cleanup

To stop the cluster, simply press `Ctrl+C` in **Terminal 1**. The script will attempt to gracefully shut down all nodes.

If you encounter errors about lock files on the next run, you can manually clean the artifacts:

```bash
rm -rf artifacts/cluster
```