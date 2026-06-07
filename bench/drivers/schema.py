"""Canonical CSV schema for the NoKV-FS benchmark framework.

Every row — L0 engine microbench, L1 service path, L2 mount path — carries the
same label prefix so one consumer (the summary printer, the baseline gate) can
read across boundaries without knowing which producer emitted the row.

The headline NoKV-vs-JuiceFS comparison is always L2-vs-L2 under a declared
``metadata_tier`` and ``cache_state``. Mixing boundaries (e.g. NoKV's L1 service
path against JuiceFS's L2 mount) is the mistake this schema exists to prevent.

See docs/benchmarks.md for the framework design.
"""

from __future__ import annotations

import os
from typing import Iterable, Sequence

# Order matters: this is the on-disk column order for every emitter.
CANONICAL_COLUMNS: Sequence[str] = (
    "boundary",        # L0 (engine) | L1 (service) | L2 (mount)
    "system",          # nokv | juicefs
    "metadata_tier",   # nokv-direct-wal-async | nokv-raft-none | juicefs-redis | ...
    "object_backend",  # rustfs | s3 | local
    "cache_state",     # cold | warm
    "concurrency",     # integer worker count for this row
    "tool",            # native | fio | mdtest | juicefs-bench | objbench
    "profile",         # smoke | standard | long
    "workload",        # bigfile | smallfile | stat | metadata | checkpoint | training_read | ...
    "phase",           # write | read | randread | create | stat | list | delete | ...
    "operations",
    "seconds",
    "ops_per_second",
    "throughput_MiB_s",
    "p50_us",
    "p99_us",
    "cost_breakdown",  # "fuse=..;meta_rpc=..;meta_commit=..;object=.." or "" if unknown
    "shape",
    "caveat",
    "run_id",          # repeat id within one benchmark invocation
    "env_id",          # environment record id, see scripts/fs-bench-env.py
)

# Boundaries.
L0_ENGINE = "L0"
L1_SERVICE = "L1"
L2_MOUNT = "L2"

_DEFAULT_CAVEAT = (
    "local engineering comparison; same object endpoint and generated shape; "
    "not an official MLPerf result"
)


def header() -> str:
    """The canonical CSV header line (no trailing newline)."""
    return ",".join(CANONICAL_COLUMNS)


def _csv_escape(value: object) -> str:
    text = "" if value is None else str(value)
    if any(ch in text for ch in (",", '"', "\n", "\r")):
        return '"' + text.replace('"', '""') + '"'
    return text


def percentiles_us(latencies_us: Iterable[float]) -> tuple[float, float]:
    """Return (p50, p99) of per-operation latencies, linearly interpolated.

    Returns (0.0, 0.0) for an empty sample so callers can emit a row without a
    special case.
    """
    ordered = sorted(latencies_us)
    if not ordered:
        return 0.0, 0.0

    def pct(p: float) -> float:
        if len(ordered) == 1:
            return float(ordered[0])
        rank = p / 100.0 * (len(ordered) - 1)
        lo = int(rank)
        hi = min(lo + 1, len(ordered) - 1)
        frac = rank - lo
        return ordered[lo] * (1.0 - frac) + ordered[hi] * frac

    return pct(50.0), pct(99.0)


def row(
    *,
    boundary: str,
    system: str,
    metadata_tier: str,
    object_backend: str,
    cache_state: str,
    concurrency: int,
    tool: str,
    profile: str,
    workload: str,
    phase: str,
    operations: int,
    seconds: float,
    bytes_total: int = 0,
    p50_us: float = 0.0,
    p99_us: float = 0.0,
    cost_breakdown: str = "",
    shape: str = "",
    caveat: str = _DEFAULT_CAVEAT,
    run_id: str | None = None,
    env_id: str | None = None,
) -> str:
    """Build one canonical CSV row.

    ``ops_per_second`` and ``throughput_MiB_s`` are derived from
    ``operations`` / ``bytes_total`` / ``seconds`` so every producer computes
    them identically.
    """
    ops_per_second = operations / seconds if seconds > 0 else 0.0
    mib = bytes_total / 1024.0 / 1024.0
    throughput = mib / seconds if seconds > 0 else 0.0
    if run_id is None:
        run_id = os.environ.get("NOKV_BENCH_RUN_ID", "")
    if env_id is None:
        env_id = os.environ.get("NOKV_BENCH_ENV_ID", "")
    values = (
        boundary,
        system,
        metadata_tier,
        object_backend,
        cache_state,
        concurrency,
        tool,
        profile,
        workload,
        phase,
        operations,
        f"{seconds:.6f}",
        f"{ops_per_second:.2f}",
        f"{throughput:.4f}",
        f"{p50_us:.2f}",
        f"{p99_us:.2f}",
        cost_breakdown,
        shape,
        caveat,
        run_id,
        env_id,
    )
    return ",".join(_csv_escape(v) for v in values)


def tool_missing_row(
    *,
    system: str,
    metadata_tier: str,
    object_backend: str,
    profile: str,
    tool: str,
    concurrency: int = 0,
    workload: str = "n/a",
) -> str:
    """A placeholder row recording that a real tool was unavailable.

    The framework never silently drops a requested tool — an absent fio/mdtest/
    juicefs-bench surfaces as an explicit row so a reader can tell "not run" from
    "ran and scored zero".
    """
    return row(
        boundary=L2_MOUNT,
        system=system,
        metadata_tier=metadata_tier,
        object_backend=object_backend,
        cache_state="n/a",
        concurrency=concurrency,
        tool=tool,
        profile=profile,
        workload=workload,
        phase="n/a",
        operations=0,
        seconds=0.0,
        caveat=f"tool-missing:{tool} requested_concurrency={concurrency}",
    )
