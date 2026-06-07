#!/usr/bin/env python3
"""Benchmark regression gate.

Diffs a run against a stored baseline CSV, keyed on the canonical label prefix +
workload/phase, and flags a regression when ``ops_per_second`` drops more than
``--ops-tol`` or ``p99_us`` rises more than ``--p99-tol``. The gate also fails
when baseline rows are missing or the run contains blocking caveats such as
``workload-failed``/``parse-failed``. That keeps CI from accepting an incomplete
matrix.

Run ``python3 compare-baseline.py --selftest`` to exercise the comparison logic.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys

KEY = ("boundary", "system", "metadata_tier", "object_backend", "cache_state",
       "concurrency", "tool", "profile", "workload", "phase")

BLOCKING_CAVEAT_PREFIXES = (
    "workload-failed:",
    "parse-failed",
    "tool-missing:",
    "fio-failed:",
    "mdtest-failed:",
    "juicefs-bench-failed:",
)


def _index(reader) -> dict:
    return {tuple(row.get(k, "") for k in KEY): row for row in reader}


def load(path: str) -> dict:
    with open(path, newline="") as handle:
        return _index(csv.DictReader(handle))


def _num(row: dict, field: str) -> float:
    try:
        return float(row.get(field, "") or 0)
    except (TypeError, ValueError):
        return 0.0


def _is_allowed_caveat(caveat: str, allowed_prefixes: tuple[str, ...]) -> bool:
    return any(caveat.startswith(prefix) for prefix in allowed_prefixes)


def find_blocking_caveats(run: dict, allowed_prefixes: tuple[str, ...] = ()):
    caveats = []
    for key, row in run.items():
        caveat = row.get("caveat", "")
        if not caveat or _is_allowed_caveat(caveat, allowed_prefixes):
            continue
        if caveat.startswith(BLOCKING_CAVEAT_PREFIXES):
            caveats.append((key, caveat))
    return caveats


def find_regressions(baseline: dict, run: dict, ops_tol: float, p99_tol: float):
    regressions = []
    missing = []
    for key, base_row in baseline.items():
        run_row = run.get(key)
        if run_row is None:
            missing.append(key)
            continue
        b_ops, r_ops = _num(base_row, "ops_per_second"), _num(run_row, "ops_per_second")
        if b_ops > 0 and r_ops < b_ops * (1 - ops_tol):
            regressions.append((key, f"ops/s {b_ops:.0f}->{r_ops:.0f} (-{(1 - r_ops / b_ops) * 100:.0f}%)"))
        b_p99, r_p99 = _num(base_row, "p99_us"), _num(run_row, "p99_us")
        if b_p99 > 0 and r_p99 > b_p99 * (1 + p99_tol):
            regressions.append((key, f"p99 {b_p99:.0f}->{r_p99:.0f}us (+{(r_p99 / b_p99 - 1) * 100:.0f}%)"))
    return regressions, missing


def _selftest() -> int:
    header = ",".join(KEY) + ",ops_per_second,p99_us"
    base = _index(csv.DictReader(io.StringIO(
        header + "\nL2,nokv,t,rustfs,warm,1,native,smoke,metadata,create,1000,50\n")))
    # 20% ops drop + 50% p99 rise -> both flagged.
    run = _index(csv.DictReader(io.StringIO(
        header + "\nL2,nokv,t,rustfs,warm,1,native,smoke,metadata,create,800,75\n")))
    regs, missing = find_regressions(base, run, 0.10, 0.20)
    assert len(regs) == 2, regs
    assert not missing, missing
    # within tolerance -> clean
    ok = _index(csv.DictReader(io.StringIO(
        header + "\nL2,nokv,t,rustfs,warm,1,native,smoke,metadata,create,950,55\n")))
    regs2, _ = find_regressions(base, ok, 0.10, 0.20)
    assert not regs2, regs2
    # Missing baseline rows fail by default in main().
    missing_run = _index(csv.DictReader(io.StringIO(header + "\n")))
    _, missing = find_regressions(base, missing_run, 0.10, 0.20)
    assert len(missing) == 1, missing
    failed = _index(csv.DictReader(io.StringIO(
        header + ",caveat\n"
        "L2,nokv,t,rustfs,n/a,1,native,smoke,bigfile,n/a,0,0,workload-failed:boom\n")))
    caveats = find_blocking_caveats(failed)
    assert len(caveats) == 1, caveats
    assert not find_blocking_caveats(failed, ("workload-failed:",))
    print("compare-baseline selftest OK")
    return 0


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Benchmark regression gate.")
    p.add_argument("run", nargs="?", help="run CSV to check")
    p.add_argument("--baseline", help="baseline CSV")
    p.add_argument("--ops-tol", type=float, default=0.10, help="allowed ops/s drop fraction")
    p.add_argument("--p99-tol", type=float, default=0.20, help="allowed p99 rise fraction")
    p.add_argument("--allow-missing-rows", action="store_true",
                   help="report baseline rows missing from the run but do not fail")
    p.add_argument("--allow-caveat-prefix", action="append", default=[],
                   help="allow a blocking caveat prefix such as tool-missing:")
    p.add_argument("--selftest", action="store_true")
    a = p.parse_args(argv)
    if a.selftest:
        return _selftest()
    if not a.run or not a.baseline:
        p.error("run and --baseline are required")
    baseline, run = load(a.baseline), load(a.run)
    regressions, missing = find_regressions(baseline, run, a.ops_tol, a.p99_tol)
    caveats = find_blocking_caveats(run, tuple(a.allow_caveat_prefix))
    for key in missing:
        label = "note" if a.allow_missing_rows else "missing"
        print(f"  {label}: baseline row missing from run: {'/'.join(key)}", file=sys.stderr)
    if caveats:
        print(f"BLOCKING CAVEAT: {len(caveats)} row(s)", file=sys.stderr)
        for key, caveat in caveats:
            print(f"  {'/'.join(key)}: {caveat}", file=sys.stderr)
    if regressions:
        print(f"REGRESSION: {len(regressions)} metric(s) beyond tolerance", file=sys.stderr)
        for key, msg in regressions:
            print(f"  {'/'.join(key)}: {msg}", file=sys.stderr)
    if regressions or caveats or (missing and not a.allow_missing_rows):
        return 1
    print(f"baseline OK: {len(baseline)} rows, no regression, missing row, or blocking caveat "
          f"(ops-tol={a.ops_tol}, p99-tol={a.p99_tol})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
