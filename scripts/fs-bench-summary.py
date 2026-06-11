#!/usr/bin/env python3
"""Print a grouped NoKV-vs-JuiceFS summary from a canonical benchmark CSV.

Groups rows by (boundary, workload, phase, cache_state, concurrency, tool) and
prints every system/tier variant under that row. Matrix runs can include more
than one NoKV metadata tier, so the tier is part of the variant label rather
than being collapsed behind ``system=nokv``.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict


def _metric(row: dict) -> tuple[str, float]:
    ops = _num(row, "ops_per_second")
    if ops > 0:
        return "ops", ops
    throughput = _num(row, "throughput_MiB_s")
    if throughput > 0:
        return "MiB/s", throughput
    return "", 0.0


def _num(row: dict, field: str) -> float:
    try:
        return float(row.get(field, "") or 0)
    except (TypeError, ValueError):
        return 0.0


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: fs-bench-summary.py RUN.csv", file=sys.stderr)
        return 2
    with open(argv[0], newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        print("fs-bench-summary: no rows", file=sys.stderr)
        return 0

    groups: dict[tuple, dict[tuple[str, str], dict]] = defaultdict(dict)
    for r in rows:
        key = (r["boundary"], r["workload"], r["phase"], r["cache_state"],
               r["concurrency"], r["tool"])
        groups[key][(r["system"], r["metadata_tier"])] = r

    print("NoKV-FS benchmark summary (L2 mount; tier/cache labeled)")
    for key in sorted(groups):
        boundary, workload, phase, cache, conc, tool = key
        variants = groups[key]
        parts = []
        for (system, tier), r in sorted(variants.items()):
            samples = r.get("samples", "")
            suffix = f"/n={samples}" if samples else ""
            parts.append(
                f"{system}[{tier}]="
                f"{r['ops_per_second']}ops/s/{r['throughput_MiB_s']}MiB/s/p99={r['p99_us']}us{suffix}"
            )
        ratios = []
        juicefs_rows = [r for (system, _), r in variants.items() if system == "juicefs"]
        nokv_rows = [((system, tier), r) for (system, tier), r in variants.items() if system == "nokv"]
        for (_, tier), nokv in sorted(nokv_rows):
            for juicefs in juicefs_rows:
                n_kind, n = _metric(nokv)
                j_kind, j = _metric(juicefs)
                if n_kind != j_kind or n <= 0 or j <= 0:
                    continue
                ratios.append(
                    f"juicefs/{tier} {j / n:.2f}x" if j >= n else f"{tier}/juicefs {n / j:.2f}x"
                )
        ratio = f"  ({'; '.join(ratios)})" if ratios else ""
        print(f"  [{boundary}] {workload}/{phase} cache={cache} p={conc} {tool}: "
              + " | ".join(parts) + ratio)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
