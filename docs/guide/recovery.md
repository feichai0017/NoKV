<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Recovery

NoKV now has two active recovery scopes.

## Local fsmeta

`fsmeta/runtime/local` stores versioned metadata records in Pebble. Recovery is
Pebble reopen plus fsmeta snapshot/watch registry reconstruction owned by the
local runtime. There is no separate NoKV WAL, generic local DB facade, or
external two-phase recovery path on the local demo runtime.

## Rooted Truth

`meta/root` recovers rooted lifecycle facts from its log and checkpoints.
Coordinator state is rebuildable from this rooted truth.

## Distributed Data Plane Target

`raftstore` owns Rust-side recovery for:

- Raft log replay;
- OpenRaft state;
- Holt state-machine trees;
- region descriptors;
- apply state;
- snapshots;
- apply watch cursors.

The Rust data plane should keep recovery evidence in Rust-owned durable state
and publish only lifecycle/control-plane facts through root/coordinator.

## Required Tests

Recovery changes should include focused tests for the changed boundary:

```bash
go test -count=1 ./fsmeta/runtime/local ./meta/root/...
cargo test --manifest-path raftstore/Cargo.toml --workspace
```
