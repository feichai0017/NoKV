#!/usr/bin/env python3
"""Merge and aggregate layered NoKV data-plane benchmark CSVs."""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "bench" / "drivers"))
import schema  # noqa: E402

PREFIX_COLUMNS = ("benchmark_layer", "source_script", "layer_case")
OPTIONAL_DIMENSIONS = ("matrix_case", "fsync", "cache_state_scope")
KEY_COLUMNS = (
    *PREFIX_COLUMNS,
    *OPTIONAL_DIMENSIONS,
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


def parse_input(raw: str) -> tuple[str, str, str, Path]:
    parts = raw.split(":", 3)
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "--input must be layer:source:case:path"
        )
    layer, source, case, path = parts
    if not layer or not source or not case or not path:
        raise argparse.ArgumentTypeError(
            "--input must not contain empty layer/source/case/path fields"
        )
    return layer, source, case, Path(path)


def load(inputs: list[tuple[str, str, str, Path]]) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    seen_columns: list[str] = []
    seen = set()

    def note_columns(columns) -> None:
        for column in columns:
            if column not in seen:
                seen.add(column)
                seen_columns.append(column)

    for layer, source, case, path in inputs:
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                continue
            note_columns(reader.fieldnames)
            for row in reader:
                row = dict(row)
                row["benchmark_layer"] = layer
                row["source_script"] = source
                row["layer_case"] = case
                rows.append(row)

    return rows, seen_columns


def output_columns(seen_columns: list[str], aggregate: bool) -> list[str]:
    optional = [
        dimension for dimension in OPTIONAL_DIMENSIONS
        if dimension in seen_columns
    ]
    columns = [*PREFIX_COLUMNS, *optional, *schema.CANONICAL_COLUMNS]
    if not aggregate:
        for column in seen_columns:
            if column not in columns:
                columns.append(column)
    else:
        columns.extend(AGGREGATE_COLUMNS)
    return columns


def write_csv(rows: list[dict[str, str]], columns: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


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


def _fmt_metric(field: str, value: float) -> str:
    if field == "operations":
        return str(int(round(value)))
    if field == "seconds":
        return f"{value:.6f}"
    if field == "throughput_MiB_s":
        return f"{value:.4f}"
    return f"{value:.2f}"


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
        groups[tuple(row.get(column, "") for column in KEY_COLUMNS)].append(row)

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
        row = {
            column: first.get(column, "")
            for column in (*PREFIX_COLUMNS, *OPTIONAL_DIMENSIONS, *schema.CANONICAL_COLUMNS)
        }
        for field in numeric_fields:
            row[field] = _fmt_metric(
                field,
                _percentile([_num(sample, field) for sample in group], 50.0),
            )
        row["cost_breakdown"] = _join_unique(group, "cost_breakdown")
        row["shape"] = _join_unique(group, "shape")
        row["caveat"] = _join_unique(group, "caveat")
        row["run_id"] = _join_unique(group, "run_id")
        row["env_id"] = _join_unique(group, "env_id")
        row["samples"] = str(len(group))
        row["operations_p95"] = str(int(round(_percentile([_num(s, "operations") for s in group], 95.0))))
        row["seconds_p95"] = f"{_percentile([_num(s, 'seconds') for s in group], 95.0):.6f}"
        row["ops_p95"] = f"{_percentile([_num(s, 'ops_per_second') for s in group], 95.0):.2f}"
        row["throughput_p95_MiB_s"] = f"{_percentile([_num(s, 'throughput_MiB_s') for s in group], 95.0):.4f}"
        row["p50_p95_us"] = f"{_percentile([_num(s, 'p50_us') for s in group], 95.0):.2f}"
        row["p99_p95_us"] = f"{_percentile([_num(s, 'p99_us') for s in group], 95.0):.2f}"
        row["run_ids"] = row["run_id"]
        row["env_ids"] = row["env_id"]
        out.append(row)
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Merge layered benchmark CSVs and keep layer/case dimensions."
    )
    parser.add_argument(
        "--input",
        action="append",
        type=parse_input,
        required=True,
        help="layer:source:case:path",
    )
    parser.add_argument("--raw-out", required=True, type=Path)
    parser.add_argument("--aggregate-out", required=True, type=Path)
    args = parser.parse_args(argv)

    rows, seen_columns = load(args.input)
    if not rows:
        raise SystemExit("no benchmark rows to merge")
    write_csv(rows, output_columns(seen_columns, aggregate=False), args.raw_out)
    aggregates = aggregate_rows(rows)
    write_csv(
        aggregates,
        output_columns(seen_columns, aggregate=True),
        args.aggregate_out,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
