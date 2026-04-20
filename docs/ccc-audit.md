# ccc-audit

`nokv ccc-audit` is an offline diagnostic CLI that inspects **coordinator authority-handoff state** and **reply traces** for legality violations.

It consumes two kinds of input:

1. A coordinator rooted snapshot (read from a workdir) — snapshot-level audit
2. A JSON reply trace (NoKV or external system) — reply-level audit

Either or both can be provided. The tool produces a structured report of detected anomalies and is safe to run against stopped nodes or trace files archived for post-mortem review.

---

## 1. When to use it

- **Post-mortem**: a coordinator crashed mid-handoff, and you want to know whether the rooted lease/seal/closure state is consistent before restarting
- **Pre-restart sanity check**: before bringing a store back up, verify its rooted metadata has no dangling successor coverage / missing confirms / missing closures
- **Trace audit**: given a dumped reply trace from an external system, check whether any accepted reply crossed an authority-transition boundary
- **CI**: compile a reply-trace fixture from a regression scenario, run `ccc-audit`, and fail the build if the anomaly count is non-zero

The tool reads only. It does not modify the workdir or publish any rooted events.

---

## 2. CLI flags

```
nokv ccc-audit
  --workdir <path>                    Coordinator work directory containing rooted metadata
  --holder <string>                   Holder id to evaluate (default: current rooted holder)
  --now-unix-nano <int64>             Override audit time (default: real time)
  --reply-trace <path>                Optional JSON reply trace to evaluate
  --reply-trace-format <format>       nokv | etcd-read-index | etcd-lease-renew | crdb-lease-start
  --json                              Emit JSON report instead of the text table
```

At least one of `--workdir` or `--reply-trace` must be provided.

When only `--reply-trace` is given, snapshot-level anomalies are skipped (there is no snapshot to audit), but reply-trace anomalies are still evaluated against whatever `Report` can be constructed from the provided context.

---

## 3. Snapshot anomalies (from `--workdir`)

Lifted directly from [`coordinator/audit/report.go`](../coordinator/audit/report.go) — when a rooted snapshot is loaded, the tool reports the following boolean/enum fields:

| Field | What triggers it |
|---|---|
| `SuccessorLineageMismatch` | Successor lease exists but its `PredecessorDigest` doesn't match the sealed predecessor |
| `UncoveredMonotoneFrontier` | Successor present but monotone frontier (ID/TSO) not covered |
| `UncoveredDescriptorRevision` | Successor present but descriptor revision frontier not covered |
| `LeaseStartCoverageViolation` | Successor `lease_start` is below the sealed served-read frontier |
| `SealedGenerationStillLive` | A sealed generation is still marked as live-able |
| `ClosureDefect` | Enum: `successor_incomplete` / `missing_confirm` / `missing_close` / `close_without_confirm` / `close_lineage_mismatch` / `reattach_without_confirm` / `reattach_without_close` / `reattach_lineage_mismatch` / `reattach_incomplete` |

`ClosureDefect` is the canonical enum field. The legacy boolean fields (`MissingConfirm`, `MissingClose`, etc.) are kept in the struct for backward-compatible test consumption only; new code should read `ClosureDefect` directly.

---

## 4. Reply-trace anomalies (from `--reply-trace`)

The reply trace evaluator is in [`coordinator/audit/trace.go`](../coordinator/audit/trace.go). Each accepted record is checked against the snapshot's closure state plus its own successor-generation evidence.

| Kind | Meaning |
|---|---|
| `post_seal_accepted_reply` | Reply accepted at a generation that is already sealed in rooted state |
| `accepted_reply_behind_successor` | Generic — accepted reply's generation is below an observed successor generation |
| `accepted_read_index_behind_successor` | etcd-read-index specialization of the above |
| `accepted_keepalive_success_after_revoke` | etcd-lease-renew specialization — keepalive success observed after a revoke revision |
| `lease_start_coverage_violation` | crdb-lease-start specialization — accepted successor `lease_start` below carried served timestamp |
| `illegal_reply_generation` | Accepted reply whose generation cannot be legal under current closure state |

Anomaly kinds are stable; adapters for new external systems add more specializations to the `accepted_reply_behind_successor` family.

---

## 5. Trace formats

The tool's trace adapter normalizes several formats into a shared `ReplyTraceRecord` schema. See [`coordinator/audit/trace_adapter.go`](../coordinator/audit/trace_adapter.go).

### `nokv` (default)

Native format, already in `ReplyTraceRecord` shape:

```json
[
  {
    "source": "nokv",
    "duty": "alloc_id",
    "cert_generation": 5,
    "observed_successor_generation": 0,
    "accepted": true
  }
]
```

Also accepts `{"records": [...]}` envelope.

### `etcd-read-index`

```json
[
  {
    "member_id": "n1",
    "read_state_generation": 12,
    "successor_generation": 13,
    "accepted": true
  }
]
```

Projected into `duty: "read_index"` with `cert_generation = read_state_generation`, `observed_successor_generation = successor_generation`.

### `etcd-lease-renew`

```json
[
  {
    "member_id": "n1",
    "response_revision": 42,
    "revoke_revision": 45,
    "accepted": true
  }
]
```

Projected into `duty: "lease_renew"` with `cert_generation = response_revision`, `observed_successor_generation = revoke_revision`.

### `crdb-lease-start`

```json
[
  {
    "key": "k",
    "successor_lease_start": 8,
    "served_timestamp": 9,
    "accepted": true
  }
]
```

Projected into `duty: "lease_start_coverage"`. Triggers `lease_start_coverage_violation` when `successor_lease_start <= served_timestamp` and `accepted=true`.

---

## 6. Output

### Text (default)

```
HolderID                 c1
NowUnixNano              1745158800000000000
CatchUpState             fresh
RootDescriptorRevision   128
CurrentHolder            c1
CurrentGeneration        5
SealGeneration           4
ClosureSatisfied         true
ClosureStage             closed
Anomalies                none
ReplyTraceRecords        3
ReplyTraceAnomalies      accepted_read_index_behind_successor
```

### JSON (`--json`)

Full dump of `Report`, `Lease`, `Seal`, reply trace records, and reply-trace anomalies. Schema is the `cccAuditOutput` struct in `cmd/nokv/ccc_audit.go`.

---

## 7. Examples

### Workdir-only audit

```bash
nokv ccc-audit --workdir ./artifacts/cluster/store-1 --json
```

Loads the rooted snapshot, evaluates closure defects, prints JSON.

### Reply-trace-only audit

```bash
nokv ccc-audit \
  --reply-trace ./traces/etcd-read-index-sample.json \
  --reply-trace-format etcd-read-index
```

Evaluates the trace against an empty snapshot — useful when all you have is the trace file.

### Combined audit with holder override

```bash
nokv ccc-audit \
  --workdir ./artifacts/cluster/store-1 \
  --holder c1 \
  --now-unix-nano 1745158800000000000 \
  --reply-trace ./traces/failover.json \
  --json
```

Produces the full report: snapshot-level anomalies + reply-trace anomalies evaluated under the same closure context.

---

## 8. Programmatic API

Everything the CLI does is available as a Go package. From `coordinator/audit`:

```go
import (
    coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
    coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
)

store, _ := coordstorage.OpenRootLocalStore("./workdir")
defer store.Close()
snapshot, _ := store.Load()

report := coordaudit.BuildReport(snapshot, "c1", time.Now().UnixNano())

records, _ := coordaudit.DecodeReplyTrace(traceBytes, coordaudit.ReplyTraceFormatNoKV)
traceAnomalies := coordaudit.EvaluateReplyTrace(report, records)

for _, a := range traceAnomalies {
    fmt.Printf("%s at index %d: %s\n", a.Kind, a.Index, a.Reason)
}
```

---

## 9. Exit codes

- `0` — tool ran successfully; anomalies (if any) are in the report but are **not** promoted to a non-zero exit code
- `1` — tool failed to run (bad flags, missing workdir, malformed trace)

**Note**: the tool intentionally does not exit non-zero on detected anomalies. Consuming scripts should parse the JSON output and decide themselves how to react. This makes it safe to run inside broader CI pipelines without accidentally failing them.

---

## 10. Source map

| File | Role |
|---|---|
| [`cmd/nokv/ccc_audit.go`](../cmd/nokv/ccc_audit.go) | CLI entry point, flag parsing, text/JSON renderer |
| [`coordinator/audit/report.go`](../coordinator/audit/report.go) | `BuildReport`, `SnapshotAnomalies`, `ClosureDefect` |
| [`coordinator/audit/trace.go`](../coordinator/audit/trace.go) | `EvaluateReplyTrace`, `ReplyTraceAnomaly` |
| [`coordinator/audit/trace_adapter.go`](../coordinator/audit/trace_adapter.go) | Format parsing + projection into `ReplyTraceRecord` |
| [`coordinator/audit/lease_start_coverage.go`](../coordinator/audit/lease_start_coverage.go) | CRDB-style lease-start coverage helper |

Related docs:

- [Rooted Truth](rooted_truth.md) — what's in the snapshot this tool audits
- [Coordinator](coordinator.md) — the service that mutates the rooted state
- [Control and Execution Plane Protocols](control_and_execution_protocols.md) — the full contract being checked
