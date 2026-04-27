# eunomia-audit

`nokv eunomia-audit` is a read-only operator tool that projects a live 3-peer
meta-root cluster's current rooted state into the `coordinator/audit`
Eunomia vocabulary. It is the operator counterpart to the TLA+ Eunomia model
(`docs/spec/Eunomia.tla`) and the
`coordinator/audit` library that coordinator/server consults at runtime.

Because NoKV only ships the separated topology (3 meta-root + N coordinator),
the audit tool speaks only remote gRPC — it does not read meta-root workdirs
directly.

## What it does

1. Dial the 3 meta-root peers through `meta/root/client` (the same client
   coordinators use) wrapped by `coordinator/rootview.OpenRootRemoteStore`.
2. Load one rooted `Snapshot` — current descriptors, allocator fences,
   `Tenure`, `Legacy`, `Handover`.
3. Project the snapshot through `coordinator/audit.BuildReport(snapshot,
   holderID, nowUnixNano)` to produce a `Report` containing `SnapshotAnomalies`
   and a `FinalityDefect` enum.
4. Optionally load a reply-trace JSON (from stdin or a file) and call
   `coordinator/audit.EvaluateReplyTrace(report, records)` to flag any
   accepted replies that are illegal under the current handover witness.
5. Render the combined result as human-readable text or JSON.

The audit is read-only: it never writes to meta-root, never advances fences,
and never mutates coordinator state. It can run while the cluster is live and
healthy, or attached to a quiesced cluster during post-incident analysis.

## Minimal vocabulary

The audit output intentionally uses a smaller protocol vocabulary than the full
implementation:

- `Lease` — the active authority record
- `Seal` — the retired predecessor era and the frontier it already
  consumed
- `Handover` — the successor handoff-completion record
- `Era` — the authority era counter
- `Witness` — the proof bundle derived from `{Lease, Seal, Handover}`

The four safety guarantees the audit talks about are:

- `Primacy` — at most one era is active
- `Inheritance` — the successor covers predecessor commitments
- `Silence` — a sealed era does not continue serving
- `Finality` — a handoff finishes instead of hanging forever

Implementation types such as `Tenure`, `Legacy`,
`Handover`, `LineageDigest`, and `MandateFrontiers`
remain unchanged in code. The audit doc keeps the public vocabulary shorter on
purpose.

## Usage

```bash
nokv eunomia-audit \
  --root-peer 1=127.0.0.1:2380 \
  --root-peer 2=127.0.0.1:2381 \
  --root-peer 3=127.0.0.1:2382
```

Required:

- `--root-peer nodeID=addr` — repeat exactly 3 times. Same gRPC endpoints
  that `nokv coordinator --root-peer ...` uses.

Optional:

- `--holder <id>` — override the holder id used for reattach checks.
  Defaults to `snapshot.Tenure.HolderID`.
- `--now-unix-nano <ns>` — override the audit clock. Defaults to
  `time.Now().UnixNano()`. Useful for deterministic regression runs.
- `--reply-trace <path>` — path to a reply-trace JSON file (`-` for stdin).
  When omitted, only the snapshot-level audit runs.
- `--reply-trace-format <format>` — one of `nokv`, `etcd-read-index`,
  `etcd-lease-renew`, `crdb-lease-start`. Defaults to `nokv`.
- `--json` — emit JSON instead of the default human-readable text.

## Sample output

```text
Eunomia audit report
----------------
holder             : coord-1
now_unix_nano      : 1714857600000000000
root_desc_revision : 42
catch_up_state     : fresh
current_holder     : coord-1
current_era        : 7
handover           : stage=confirmed
handover_witness   : stage=confirmed legacy_era=6 successor_present=true inheritance=covered lineage_satisfied=true sealed_era_retired=true

snapshot anomalies:
  successor_lineage_mismatch     : false
  uncovered_monotone_frontier    : false
  uncovered_descriptor_revision  : false
  lease_start_coverage_violation : false
  sealed_era_still_live      : false
  finality_defect                 : none
```

When `--reply-trace` is provided, each accepted reply that violates the
handover witness prints as a trailing line:

```text
reply-trace anomalies (1):
  [3] kind=accepted_reply_behind_successor duty=lease_start cert_era=5 reason="accepted reply era 5 behind observed successor era 7"
```

## Anomaly vocabulary

See [coordinator/audit/report.go](../coordinator/audit/report.go) for the full
enum:

- `successor_incomplete` / `missing_confirm` / `missing_close`
- `close_without_confirm`
- `lineage_mismatch`
- `reattach_without_confirm` / `reattach_without_close` /
  `reattach_lineage_mismatch` / `reattach_incomplete`

Any non-empty `finality_defect` or any `true` flag under `snapshot anomalies`
indicates that the rooted handoff state has drifted from the expected
`active lease → sealed predecessor → inherited successor → closed handoff`
lifecycle — which is exactly the property `docs/spec/Eunomia.tla` proves meta-root must
preserve.

## Runtime diagnostics and metrics

The audit CLI is the read-only offline/operator view. The live coordinator
runtime now exposes a matching `eunomia_metrics` block through
`coordinator/server.DiagnosticsSnapshot()` and the `nokv_coordinator` expvar
surface.

That block exports four counter families:

- `lease_era_transitions_total`
- `handover_stage_transitions_total`
- `pre_action_gate_rejections_total`
- `guarantee_violations_total`

`guarantee_violations_total` uses the same four Eunomia guarantees:

- `primacy`
- `inheritance`
- `silence`
- `finality`

The intended split is:

- `eunomia-audit` explains whether the **current rooted snapshot** is legal
- `eunomia_metrics` shows how often the **live runtime** has rejected unsafe
  continuations while the system is running

## Related

- [Rooted truth](rooted_truth.md) — lifecycle semantics audited by this tool
- [Coordinator](coordinator.md) — the runtime consumer of the same audit
  library
- [`coordinator/audit`](../coordinator/audit/) — the library this CLI wraps
