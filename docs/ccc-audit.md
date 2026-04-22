# ccc-audit

`nokv ccc-audit` is a read-only operator tool that projects a live 3-peer
meta-root cluster's current rooted state into the `coordinator/audit`
closure-complete-continuation (CCC) vocabulary. It is the operator
counterpart to the TLA+ CCC model and the `coordinator/audit` library that
coordinator/server consults at runtime.

Because NoKV only ships the separated topology (3 meta-root + N coordinator),
the audit tool speaks only remote gRPC — it does not read meta-root workdirs
directly.

## What it does

1. Dial the 3 meta-root peers through `meta/root/client` (the same client
   coordinators use) wrapped by `coordinator/rootview.OpenRootRemoteStore`.
2. Load one rooted `Snapshot` — current descriptors, allocator fences,
   `CoordinatorLease`, `CoordinatorSeal`, `CoordinatorClosure`.
3. Project the snapshot through `coordinator/audit.BuildReport(snapshot,
   holderID, nowUnixNano)` to produce a `Report` containing `SnapshotAnomalies`
   and a `ClosureDefect` enum.
4. Optionally load a reply-trace JSON (from stdin or a file) and call
   `coordinator/audit.EvaluateReplyTrace(report, records)` to flag any
   accepted replies that are illegal under the current closure witness.
5. Render the combined result as human-readable text or JSON.

The audit is read-only: it never writes to meta-root, never advances fences,
and never mutates coordinator state. It can run while the cluster is live and
healthy, or attached to a quiesced cluster during post-incident analysis.

## Usage

```bash
nokv ccc-audit \
  --root-peer 1=127.0.0.1:2380 \
  --root-peer 2=127.0.0.1:2381 \
  --root-peer 3=127.0.0.1:2382
```

Required:

- `--root-peer nodeID=addr` — repeat exactly 3 times. Same gRPC endpoints
  that `nokv coordinator --root-peer ...` uses.

Optional:

- `--holder <id>` — override the holder id used for reattach checks.
  Defaults to `snapshot.CoordinatorLease.HolderID`.
- `--now-unix-nano <ns>` — override the audit clock. Defaults to
  `time.Now().UnixNano()`. Useful for deterministic regression runs.
- `--reply-trace <path>` — path to a reply-trace JSON file (`-` for stdin).
  When omitted, only the snapshot-level audit runs.
- `--reply-trace-format <format>` — one of `nokv`, `etcd-read-index`,
  `etcd-lease-renew`, `crdb-lease-start`. Defaults to `nokv`.
- `--json` — emit JSON instead of the default human-readable text.

## Sample output

```text
CCC audit report
----------------
holder             : coord-1
now_unix_nano      : 1714857600000000000
root_desc_revision : 42
catch_up_state     : fresh
current_holder     : coord-1
current_generation : 7
closure            : stage=confirmed
closure_witness    : stage=confirmed seal_gen=6 successor_present=true successor_coverage=covered lineage_satisfied=true sealed_gen_retired=true

snapshot anomalies:
  successor_lineage_mismatch     : false
  uncovered_monotone_frontier    : false
  uncovered_descriptor_revision  : false
  lease_start_coverage_violation : false
  sealed_generation_still_live   : false
  closure_defect                 : none
```

When `--reply-trace` is provided, each accepted reply that violates the
closure witness prints as a trailing line:

```text
reply-trace anomalies (1):
  [3] kind=accepted_reply_behind_successor duty=lease_start cert_gen=5 reason="accepted reply generation 5 behind observed successor generation 7"
```

## Anomaly vocabulary

See [coordinator/audit/report.go](../coordinator/audit/report.go) for the full
enum:

- `successor_incomplete` / `missing_confirm` / `missing_close`
- `close_without_confirm`
- `lineage_mismatch`
- `reattach_without_confirm` / `reattach_without_close` /
  `reattach_lineage_mismatch` / `reattach_incomplete`

Any non-empty `closure_defect` or any `true` flag under `snapshot anomalies`
indicates that the rooted closure state has drifted from the expected
`Attached → Active → Seal → Cover → Close → Reattach` lifecycle — which is
exactly the property CCC.tla proves meta-root must preserve.

## Related

- [Rooted truth](rooted_truth.md) — lifecycle semantics audited by this tool
- [Coordinator](coordinator.md) — the runtime consumer of the same audit
  library
- [`coordinator/audit`](../coordinator/audit/) — the library this CLI wraps
