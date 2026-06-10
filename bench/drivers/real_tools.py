#!/usr/bin/env python3
"""Real-tool orchestration for the NoKV-FS benchmark framework.

JuiceFS publishes numbers from the actual industry-standard tools — ``fio`` for
throughput, ``mdtest`` for metadata, and its own ``juicefs bench`` — so "follow
JuiceFS's design" means running those same binaries against the mount and
parsing their native output into the canonical schema. This module is the
co-equal counterpart to ``posix_bench.py`` (the self-contained driver).

Every tool is gated on its binary being present (overridable via env:
``NOKV_BENCH_FIO_BIN`` / ``NOKV_BENCH_MDTEST_BIN`` / ``NOKV_BENCH_MPIRUN_BIN`` /
``NOKV_JUICEFS_BIN``). A missing tool is never silently dropped — it surfaces as
an explicit ``caveat=tool-missing:<name>`` row so a reader can tell "not run"
from "ran and scored zero". Parse failures degrade the same way
(``caveat=parse-failed``) with the raw value preserved in ``shape``.

Run ``python3 real_tools.py --selftest`` to exercise the parsers against
captured sample output without the binaries installed.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema  # noqa: E402

MiB = 1024 * 1024
GiB = 1024 * MiB
KiB = 1024

_FIO_SIZE = {"smoke": "64m", "standard": "256m", "long": "1g"}
_MDTEST_TREE = {
    "smoke": ("3", "4", "2"),     # -b branches, -I items, -z depth
    "standard": ("6", "8", "3"),
    "long": ("6", "8", "4"),
}


def _bin(env_name: str, default: str) -> str | None:
    candidate = os.environ.get(env_name, default)
    return candidate if shutil.which(candidate) else None


def _fio_ioengine() -> str:
    return "posixaio" if platform.system() == "Darwin" else "libaio"


# --------------------------------------------------------------------------- #
# Parsers (pure functions, exercised by --selftest)
# --------------------------------------------------------------------------- #
def parse_fio(stdout: str):
    """Yield (phase, operations, seconds, bytes_total, p50_us, p99_us) from fio
    ``--output-format=json`` output."""
    data = json.loads(stdout)
    for job in data.get("jobs", []):
        runtime_s = job.get("job_runtime", 0) / 1000.0
        for op in ("read", "write"):
            o = job.get(op, {})
            if not o.get("io_bytes"):
                continue
            pct = o.get("clat_ns", {}).get("percentile", {}) or {}
            p50 = float(pct.get("50.000000", 0)) / 1000.0
            p99 = float(pct.get("99.000000", 0)) / 1000.0
            secs = (o.get("runtime", 0) / 1000.0) or runtime_s
            yield (op, int(o.get("total_ios", 0)), secs, int(o.get("io_bytes", 0)), p50, p99)


_MDTEST_OPS = (
    "Directory creation", "Directory stat", "Directory removal",
    "File creation", "File stat", "File read", "File removal",
    "Tree creation", "Tree removal",
)


def parse_mdtest(stdout: str):
    """Yield (phase, mean_ops_per_sec) from an mdtest SUMMARY block.

    mdtest prints ``Operation : Max Min Mean StdDev`` in some releases and
    ``Operation Max Min Mean StdDev`` in others; we take Mean.
    """
    for line in stdout.splitlines():
        for op in _MDTEST_OPS:
            m = re.match(
                rf"^\s*{re.escape(op)}\s*:?\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)",
                line,
            )
            if m:
                phase = op.lower().replace(" ", "-")
                yield (phase, float(m.group(3)))  # Mean
                break


def parse_juicefs_bench(stdout: str):
    """Yield (item, value, unit, cost) rows from a ``juicefs bench`` table."""
    for line in stdout.splitlines():
        m = re.match(r"^\|\s*(.+?)\s*\|\s*([\d.]+)\s*(\S+)\s*\|\s*(.+?)\s*\|\s*$", line)
        if m:
            yield (m.group(1).strip(), float(m.group(2)), m.group(3).strip(), m.group(4).strip())


def throughput_bytes_per_second(value: float, unit: str) -> int | None:
    """Convert a JuiceFS throughput unit to bytes/s, or None for op-rate units."""
    normalized = unit.strip().lower()
    if normalized.startswith("gib"):
        return int(value * GiB)
    if normalized.startswith("mib"):
        return int(value * MiB)
    if normalized.startswith("kib"):
        return int(value * KiB)
    if normalized in ("b/s", "byte/s", "bytes/s"):
        return int(value)
    return None


# --------------------------------------------------------------------------- #
# Runners
# --------------------------------------------------------------------------- #
def _row(args, **kw):
    base = dict(
        boundary=schema.L2_MOUNT, system=args.system, metadata_tier=args.metadata_tier,
        object_backend=args.object_backend, profile=args.profile,
        concurrency=args.concurrency,
    )
    base.update(kw)
    return schema.row(**base)


def run_fio(args) -> list[str]:
    binary = _bin("NOKV_BENCH_FIO_BIN", "fio")
    if not binary:
        return [schema.tool_missing_row(system=args.system, metadata_tier=args.metadata_tier,
                                        object_backend=args.object_backend, profile=args.profile,
                                        concurrency=args.concurrency,
                                        tool="fio", workload="fio")]
    base = Path(args.mount) / f"fio-{args.system}"
    base.mkdir(parents=True, exist_ok=True)
    size = _FIO_SIZE.get(args.profile, "64m")
    rows: list[str] = []
    try:
        for rw in ("write", "read", "randwrite", "randread"):
            cmd = [
                binary, f"--name={rw}", f"--directory={base}", "--bs=1m",
                f"--size={size}", f"--numjobs={max(1, args.concurrency)}", "--direct=1",
                f"--ioengine={_fio_ioengine()}", f"--rw={rw}", "--group_reporting",
                "--output-format=json",
            ]
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
            if out.returncode != 0:
                rows.append(_row(args, tool="fio", workload="fio", phase=rw, cache_state="cold",
                                 operations=0, seconds=0.0, caveat=f"fio-failed:{out.returncode}",
                                 shape=out.stderr.strip()[:160]))
                continue
            for phase, ops, secs, nbytes, p50, p99 in parse_fio(out.stdout):
                rows.append(_row(args, tool="fio", workload=f"fio-{rw}", phase=phase,
                                 cache_state="cold", operations=ops, seconds=secs,
                                 bytes_total=nbytes, p50_us=p50, p99_us=p99,
                                 shape=f"bs=1m size={size} numjobs={args.concurrency} direct=1"))
    finally:
        shutil.rmtree(base, ignore_errors=True)
    return rows


def run_mdtest(args) -> list[str]:
    binary = _bin("NOKV_BENCH_MDTEST_BIN", "mdtest")
    if not binary:
        return [schema.tool_missing_row(system=args.system, metadata_tier=args.metadata_tier,
                                        object_backend=args.object_backend, profile=args.profile,
                                        concurrency=args.concurrency,
                                        tool="mdtest", workload="mdtest")]
    branches, items, depth = _MDTEST_TREE.get(args.profile, _MDTEST_TREE["smoke"])
    try:
        target = Path(tempfile.mkdtemp(
            prefix=f"mdtest-{args.system}-p{args.concurrency}-",
            dir=args.mount,
        ))
    except OSError as err:
        return [_row(args, tool="mdtest", workload="mdtest", phase="n/a", cache_state="n/a",
                     operations=0, seconds=0.0,
                     caveat=f"mdtest-target-failed:{type(err).__name__}",
                     shape=str(err)[:160])]
    mpirun = _bin("NOKV_BENCH_MPIRUN_BIN", "mpirun")
    cmd = []
    if mpirun and args.concurrency > 1:
        cmd = [mpirun]
        if _mpirun_supports_oversubscribe(mpirun):
            cmd.append("--oversubscribe")
        cmd += ["-np", str(args.concurrency)]
    cmd += [binary, "-d", str(target), "-b", branches, "-I", items, "-z", depth, "-F", "-C", "-T", "-r"]
    shape = f"b={branches} I={items} z={depth} np={args.concurrency if mpirun else 1}"
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
        if out.returncode != 0:
            return [_row(args, tool="mdtest", workload="mdtest", phase="n/a", cache_state="n/a",
                         operations=0, seconds=0.0, caveat=f"mdtest-failed:{out.returncode}",
                         shape=out.stderr.strip()[:160])]
        rows = [
            _row(args, tool="mdtest", workload="mdtest", phase=phase, cache_state="warm",
                 operations=int(rate), seconds=1.0, shape=shape)
            for phase, rate in parse_mdtest(out.stdout)
        ]
        return rows or [_row(args, tool="mdtest", workload="mdtest", phase="n/a", cache_state="n/a",
                             operations=0, seconds=0.0, caveat="parse-failed", shape=shape)]
    finally:
        shutil.rmtree(target, ignore_errors=True)


def _mpirun_supports_oversubscribe(mpirun: str) -> bool:
    requested = os.environ.get("NOKV_BENCH_MPIRUN_OVERSUBSCRIBE", "1").lower()
    if requested in {"0", "false", "no"}:
        return False
    try:
        out = subprocess.run(
            [mpirun, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    text = f"{out.stdout}\n{out.stderr}"
    return "Open MPI" in text


def run_juicefs_bench(args) -> list[str]:
    if args.system != "juicefs":
        return []  # juicefs bench is juicefs-specific
    binary = _bin("NOKV_JUICEFS_BIN", "juicefs")
    if not binary:
        return [schema.tool_missing_row(system=args.system, metadata_tier=args.metadata_tier,
                                        object_backend=args.object_backend, profile=args.profile,
                                        concurrency=args.concurrency,
                                        tool="juicefs-bench", workload="juicefs-bench")]
    cmd = [binary, "bench", str(args.mount), "-p", str(max(1, args.concurrency))]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
    if out.returncode != 0:
        return [_row(args, tool="juicefs-bench", workload="juicefs-bench", phase="n/a",
                     cache_state="n/a", operations=0, seconds=0.0,
                     caveat=f"juicefs-bench-failed:{out.returncode}", shape=out.stderr.strip()[:160])]
    rows = []
    for item, value, unit, cost in parse_juicefs_bench(out.stdout):
        bytes_total = throughput_bytes_per_second(value, unit)
        is_throughput = bytes_total is not None
        rows.append(_row(
            args, tool="juicefs-bench", workload="juicefs-bench",
            phase=item.lower().replace(" ", "-"), cache_state="warm",
            operations=0 if is_throughput else int(value), seconds=1.0,
            bytes_total=bytes_total or 0, cost_breakdown=f"cost={cost}",
            shape=f"value={value} {unit}",
        ))
    return rows or [_row(args, tool="juicefs-bench", workload="juicefs-bench", phase="n/a",
                         cache_state="n/a", operations=0, seconds=0.0, caveat="parse-failed")]


TOOLS = {"fio": run_fio, "mdtest": run_mdtest, "juicefs-bench": run_juicefs_bench}


# --------------------------------------------------------------------------- #
# Self-test: validate parsers against captured sample output.
# --------------------------------------------------------------------------- #
_SAMPLE_FIO = json.dumps({
    "jobs": [{
        "job_runtime": 2000,
        "write": {"io_bytes": 268435456, "total_ios": 256, "runtime": 2000,
                  "clat_ns": {"percentile": {"50.000000": 1500000, "99.000000": 9000000}}},
        "read": {"io_bytes": 0, "total_ios": 0},
    }]
})
_SAMPLE_MDTEST = """
SUMMARY rate: (of 1 iterations)
   Operation                     Max            Min           Mean        Std Dev
   ---------                     ---            ---           ----        -------
   File creation             :       1410.30        1410.30        1410.30          0.00
   File stat                 :       5023.20        5023.20        5023.20          0.00
   File removal              :       2000.00        2000.00        2000.00          0.00
"""
_SAMPLE_MDTEST_NO_COLON = """
SUMMARY rate (in ops/sec): (of 1 iterations)
   Operation                     Max            Min           Mean        Std Dev
   ---------                     ---            ---           ----        -------
   File creation               18477.110      18477.110      18477.110          0.000
"""
_SAMPLE_JUICEFS = """
  Block Size: 1 MiB, Big File Size: 1024 MiB
+------------------+------------------+----------------+
|       ITEM       |       VALUE      |      COST      |
+------------------+------------------+----------------+
|   Write big file |    729.94 MiB/s  |   5.61 s/file  |
|    Read big file |      1.50 GiB/s  |   0.62 s/file  |
|      Read sample |    512.00 KiB/s  |   1.10 ms/file |
| Write small file |   1234.50 files/s|   3.24 ms/file |
|        Stat file |  12345.60 files/s|   0.32 ms/file |
+------------------+------------------+----------------+
"""


def _selftest() -> int:
    fio = list(parse_fio(_SAMPLE_FIO))
    assert fio == [("write", 256, 2.0, 268435456, 1500.0, 9000.0)], fio
    md = list(parse_mdtest(_SAMPLE_MDTEST))
    assert md == [("file-creation", 1410.30), ("file-stat", 5023.20), ("file-removal", 2000.00)], md
    md_no_colon = list(parse_mdtest(_SAMPLE_MDTEST_NO_COLON))
    assert md_no_colon == [("file-creation", 18477.110)], md_no_colon
    jf = list(parse_juicefs_bench(_SAMPLE_JUICEFS))
    assert jf[0] == ("Write big file", 729.94, "MiB/s", "5.61 s/file"), jf
    assert jf[1] == ("Read big file", 1.50, "GiB/s", "0.62 s/file"), jf
    assert jf[2] == ("Read sample", 512.00, "KiB/s", "1.10 ms/file"), jf
    assert throughput_bytes_per_second(1.50, "GiB/s") == int(1.50 * GiB)
    assert throughput_bytes_per_second(512.00, "KiB/s") == int(512.00 * KiB)
    assert throughput_bytes_per_second(1234.50, "files/s") is None
    assert jf[3][0] == "Write small file" and jf[3][2] == "files/s", jf
    print("real_tools selftest OK: fio, mdtest, juicefs-bench parsers")
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv):
    p = argparse.ArgumentParser(description="Real-tool benchmark orchestration (canonical schema).")
    p.add_argument("--selftest", action="store_true", help="run parser self-tests and exit")
    p.add_argument("--system", help="nokv | juicefs")
    p.add_argument("--mount", type=Path)
    p.add_argument("--metadata-tier")
    p.add_argument("--object-backend", default="rustfs")
    p.add_argument("--profile", default="smoke", choices=["smoke", "standard", "long"])
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--tools", default="fio,mdtest,juicefs-bench")
    p.add_argument("--timeout", type=int, default=1800)
    p.add_argument("--emit-header", type=int, default=1)
    return p.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    if args.selftest:
        return _selftest()
    for required in ("system", "mount", "metadata_tier"):
        if getattr(args, required) is None:
            print(f"error: --{required.replace('_', '-')} is required", file=sys.stderr)
            return 2
    if args.emit_header:
        print(schema.header(), flush=True)
    requested = [t.strip() for t in args.tools.split(",") if t.strip()]
    unknown = [t for t in requested if t not in TOOLS]
    if unknown:
        print(f"error: unknown tools: {', '.join(unknown)}", file=sys.stderr)
        return 2
    for name in requested:
        for line in TOOLS[name](args):
            print(line, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
