#!/usr/bin/env python3
"""Capture environment metadata for NoKV-FS benchmark runs."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import socket
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]

VERSION_COMMANDS = {
    "rustc": ["rustc", "--version"],
    "cargo": ["cargo", "--version"],
    "rustfs": [os.environ.get("NOKV_BENCH_RUSTFS_BIN", os.environ.get("NOKV_RUSTFS_BIN", "rustfs")), "--version"],
    "juicefs": [os.environ.get("NOKV_BENCH_JUICEFS_BIN", os.environ.get("NOKV_JUICEFS_BIN", "juicefs")), "--version"],
    "redis": [os.environ.get("NOKV_BENCH_REDIS_BIN", os.environ.get("NOKV_REDIS_BIN", "redis-server")), "--version"],
    "aws": [os.environ.get("NOKV_BENCH_AWS_BIN", os.environ.get("NOKV_AWS_BIN", "aws")), "--version"],
    "fio": [os.environ.get("NOKV_BENCH_FIO_BIN", "fio"), "--version"],
    "mdtest": [os.environ.get("NOKV_BENCH_MDTEST_BIN", str(ROOT_DIR / "third_party" / "ior" / "_install" / "bin" / "mdtest")), "-h"],
    "mpirun": [os.environ.get("NOKV_BENCH_MPIRUN_BIN", "mpirun"), "--version"],
}

BENCH_ENV_KEYS = (
    "NOKV_BENCH_PROFILE",
    "NOKV_BENCH_FSYNC",
    "NOKV_BENCH_BUILD_RELEASE",
    "NOKV_BENCH_RUSTFS_ENDPOINT",
    "NOKV_BENCH_RUSTFS_BUFFER_PROFILE",
    "NOKV_BENCH_NOKV_BUCKET",
    "NOKV_BENCH_JUICEFS_BUCKET",
    "NOKV_BENCH_REDIS_PORT",
    "NOKV_BENCH_SERVER_ADDRESS",
    "NOKV_BENCH_NOKV_FS_BIN",
    "NOKV_BENCH_FIO_BIN",
    "NOKV_BENCH_MDTEST_BIN",
    "NOKV_BENCH_MPIRUN_BIN",
    "NOKV_BENCH_JUICEFS_BIN",
)


def _run(cmd: list[str], cwd: Path | None = None, timeout: int = 5) -> str:
    try:
        out = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired) as err:
        return f"unavailable:{type(err).__name__}"
    text = (out.stdout or out.stderr or "").strip().splitlines()
    head = text[0] if text else ""
    if out.returncode != 0 and not head:
        return f"failed:{out.returncode}"
    return head[:240]


def _git(cwd: Path, *args: str) -> str:
    return _run(["git", *args], cwd=cwd)


def _git_dirty(cwd: Path) -> dict[str, object]:
    out = _run(["git", "status", "--porcelain"], cwd=cwd, timeout=10)
    if out.startswith(("unavailable:", "failed:")):
        return {"available": False, "summary": out}
    lines = out.splitlines() if out else []
    return {"available": True, "dirty": bool(lines), "changed_paths": len(lines)}


def capture(args: argparse.Namespace) -> dict[str, object]:
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    env_id = args.env_id or f"{now.replace(':', '').replace('-', '')}-{socket.gethostname().split('.')[0]}"
    versions = {name: _run(cmd) for name, cmd in VERSION_COMMANDS.items()}
    ior_dir = ROOT_DIR / "third_party" / "ior"
    selected_env = {key: os.environ[key] for key in BENCH_ENV_KEYS if key in os.environ}
    return {
        "env_id": env_id,
        "timestamp_utc": now,
        "host": {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "python": sys.version.replace("\n", " "),
        "repo": {
            "root": str(ROOT_DIR),
            "branch": _git(ROOT_DIR, "rev-parse", "--abbrev-ref", "HEAD"),
            "head": _git(ROOT_DIR, "rev-parse", "HEAD"),
            "dirty": _git_dirty(ROOT_DIR),
        },
        "third_party": {
            "ior_head": _git(ior_dir, "rev-parse", "HEAD") if ior_dir.exists() else "not-present",
        },
        "versions": versions,
        "benchmark": {
            "mode": args.mode,
            "profile": args.profile,
            "tiers": args.tiers,
            "concurrency": args.concurrency,
            "product_workloads": args.product_workloads,
            "primitive_workloads": args.primitive_workloads,
            "repeats": args.repeats,
        },
        "selected_env": selected_env,
    }


def _selftest() -> int:
    assert _run(["sh", "-c", "printf ok"]) == "ok"
    args = argparse.Namespace(
        env_id="selftest",
        mode="quick",
        profile="smoke",
        tiers="local",
        concurrency="1",
        product_workloads="metadata_create_list",
        primitive_workloads="metadata",
        repeats=1,
    )
    doc = capture(args)
    assert doc["env_id"] == "selftest"
    assert "repo" in doc and "versions" in doc
    print("fs-bench-env selftest OK")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Capture NoKV-FS benchmark environment metadata.")
    parser.add_argument("--out", help="output JSON path; default stdout")
    parser.add_argument("--env-id", default="")
    parser.add_argument("--mode", default="")
    parser.add_argument("--profile", default="")
    parser.add_argument("--tiers", default="")
    parser.add_argument("--concurrency", default="")
    parser.add_argument("--product-workloads", default="")
    parser.add_argument("--primitive-workloads", default="")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args(argv)
    if args.selftest:
        return _selftest()
    doc = capture(args)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as handle:
            json.dump(doc, handle, indent=2, sort_keys=True)
            handle.write("\n")
    else:
        json.dump(doc, sys.stdout, indent=2, sort_keys=True)
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
