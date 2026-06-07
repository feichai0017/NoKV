<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Native Grep Fairness Handoff

## Context

`sqlite_agentfs_v1` currently exposes `grep`, implemented as a basic substring
scan over the SQLite-backed filesystem projection. `nokv_native_v1` exposes
`ls`, `stat`, `read`, and `find`, but it does not expose an equivalent native
file-content search operation.

That asymmetry makes any AgentFS-vs-NoKV comparison unfair when a task can be
solved by locating text inside files. `find` is not equivalent: it filters
namespace fields, body descriptors, and registered index values. It does not
scan file bytes. `grep` should be understood as a filesystem operation: use
namespace fields to discover candidate files, then scan file content.

## Required Shape

Implement native grep as a product capability, not a benchmark-only shortcut.
The benchmark harness must only expose it after the NoKV product surface
supports it through the normal stack.

The target behavior is basic filesystem grep:

- input: root `path`, literal `pattern`, `recursive`, bounded `limit`, optional
  cursor, and optional byte/file scan limits.
- candidate selection: traverse regular files under `path`; if `recursive` is
  false, inspect only immediate file children.
- content scan: scan file bytes as text lines, skip or mark binary files using a
  simple NUL-byte check, and match by literal substring.
- output: path, line number, snippet, evidence handle, files scanned, bytes
  read, truncation state, and next cursor when more results exist.
- evidence: include snapshot/generation identity so a later read can validate
  the cited file version.

Do not make the first implementation regex-aware, task-aware, metric-aware, or
Yanex-aware. Literal case-sensitive substring search is enough unless the
product API explicitly chooses additional options.

## Implementation Boundary

The implementation should follow the normal NoKV layers:

- `nokvfs-protocol`: add storage-neutral wire DTOs for a namespace grep request
  and result, plus a metadata RPC operation such as `GrepPaths`.
- `nokvfs-meta`: implement traversal and byte scanning against the same
  snapshot/version used for the candidate namespace walk. Use existing object
  read paths; do not add benchmark indexes or special-case known benchmark
  paths.
- `nokvfs-server`: route the new RPC to the metadata service and convert
  service result DTOs to wire DTOs.
- `nokvfs-client`: expose a typed client method and add an agent tool named
  `grep` beside `ls`, `stat`, `read`, and `find`.
- `benchmark/agent-interface-benchmark`: once the product-native tool exists,
  register it for `nokv_native_v1` through `execute_agent_tool`. The harness
  should remain a thin adapter.

If native grep is not implemented yet, do not claim a fair AgentFS-vs-NoKV
comparison that relies on grep. Either keep `sqlite_agentfs_v1` as
sensitivity/context only, or remove `grep` from that comparison surface.

## Explicit Non-Goals

- No benchmark-only shortcut in the harness.
- No precomputed content-answer index tailored to Phase 1 tasks.
- No shelling out to POSIX tools.
- No SQL-specific semantics copied into NoKV.
- No hidden task recipe in the arm card.

The goal is parity of basic filesystem affordances, not a benchmark-specific
advantage.
