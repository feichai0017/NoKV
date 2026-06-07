#!/usr/bin/env python3
"""Aggregate repeated NoKV-FS benchmark rows.

The mounted benchmark emits one canonical row per measured phase per repeat.
This script folds those raw rows by the baseline-comparison key and writes a
canonical-compatible CSV where the main metric columns contain medians. Extra
columns preserve sample count, p95 values, and the run/env ids that contributed
to the aggregate.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from collections import defaultdict
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "bench" / "drivers"))
import schema  # noqa: E402

KEY = (
    "boundary",
    "system",
    "metadata_tier",
    "object_backend",
    "cache_state",
    "concurrency",
    "tool",
    "profile",
    "workload",
    "phase",
)

AGGREGATE_COLUMNS = (
    "samples",
    "operations_p95",
    "seconds_p95",
    "ops_p95",
    "throughput_p95_MiB_s",
    "p50_p95_us",
    "p99_p95_us",
    "run_ids",
    "env_ids",
)


def _num(row: dict[str, str], field: str) -> float:
    try:
        return float(row.get(field, "") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = pct / 100.0 * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _fmt_float(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def _fmt_metric(field: str, value: float) -> str:
    if field == "operations":
        return str(int(round(value)))
    if field == "seconds":
        return f"{value:.6f}"
    if field == "throughput_MiB_s":
        return f"{value:.4f}"
    return _fmt_float(value)


def _join_unique(rows: list[dict[str, str]], field: str) -> str:
    values = []
    seen = set()
    for row in rows:
        value = row.get(field, "")
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return ";".join(values)


def aggregate_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    groups: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(k, "") for k in KEY)].append(row)

    out: list[dict[str, str]] = []
    numeric_fields = (
        "operations",
        "seconds",
        "ops_per_second",
        "throughput_MiB_s",
        "p50_us",
        "p99_us",
    )
    for key in sorted(groups):
        group = groups[key]
        first = group[0]
        row = {col: first.get(col, "") for col in schema.CANONICAL_COLUMNS}
        for field in numeric_fields:
            median = _percentile([_num(sample, field) for sample in group], 50.0)
            row[field] = _fmt_metric(field, median)
        row["cost_breakdown"] = _join_unique(group, "cost_breakdown")
        row["shape"] = _join_unique(group, "shape")
        row["caveat"] = _join_unique(group, "caveat")
        row["run_id"] = _join_unique(group, "run_id")
        row["env_id"] = _join_unique(group, "env_id")
        row["samples"] = str(len(group))
        row["operations_p95"] = str(int(round(_percentile([_num(s, "operations") for s in group], 95.0))))
        row["seconds_p95"] = f"{_percentile([_num(s, 'seconds') for s in group], 95.0):.6f}"
        row["ops_p95"] = _fmt_float(_percentile([_num(s, "ops_per_second") for s in group], 95.0))
        row["throughput_p95_MiB_s"] = f"{_percentile([_num(s, 'throughput_MiB_s') for s in group], 95.0):.4f}"
        row["p50_p95_us"] = _fmt_float(_percentile([_num(s, "p50_us") for s in group], 95.0))
        row["p99_p95_us"] = _fmt_float(_percentile([_num(s, "p99_us") for s in group], 95.0))
        row["run_ids"] = row["run_id"]
        row["env_ids"] = row["env_id"]
        out.append(row)
    return out


def load(path: str) -> list[dict[str, str]]:
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def write(rows: list[dict[str, str]], path: str | None) -> None:
    columns = list(schema.CANONICAL_COLUMNS) + list(AGGREGATE_COLUMNS)
    handle = open(path, "w", newline="") if path else sys.stdout
    try:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if path:
            handle.close()


def _selftest() -> int:
    raw = io.StringIO(
        schema.header()
        + "\n"
        + schema.row(
            boundary=schema.L2_MOUNT,
            system="nokv",
            metadata_tier="t",
            object_backend="rustfs",
            cache_state="warm",
            concurrency=1,
            tool="native",
            profile="smoke",
            workload="metadata",
            phase="create",
            operations=100,
            seconds=1.0,
            p99_us=10,
            run_id="r1",
            env_id="e1",
        )
        + "\n"
        + schema.row(
            boundary=schema.L2_MOUNT,
            system="nokv",
            metadata_tier="t",
            object_backend="rustfs",
            cache_state="warm",
            concurrency=1,
            tool="native",
            profile="smoke",
            workload="metadata",
            phase="create",
            operations=200,
            seconds=1.0,
            p99_us=30,
            run_id="r2",
            env_id="e1",
        )
        + "\n"
    )
    rows = aggregate_rows(list(csv.DictReader(raw)))
    assert len(rows) == 1, rows
    assert rows[0]["samples"] == "2", rows
    assert rows[0]["operations"] == "150", rows
    assert rows[0]["ops_per_second"] == "150.00", rows
    assert rows[0]["p99_us"] == "20.00", rows
    assert rows[0]["run_ids"] == "r1;r2", rows
    print("aggregate-fs-benchmark selftest OK")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Aggregate repeated NoKV-FS benchmark CSV rows.")
    parser.add_argument("run", nargs="?", help="raw canonical benchmark CSV")
    parser.add_argument("--out", help="aggregate CSV path; default stdout")
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args(argv)
    if args.selftest:
        return _selftest()
    if not args.run:
        parser.error("run CSV is required")
    write(aggregate_rows(load(args.run)), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
