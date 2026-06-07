#!/usr/bin/env python3
"""NoKV ``/stats`` latency decomposition.

The mounted driver only sees the filesystem, so the per-op FUSE / metadata /
object cost split is read out-of-band from the NoKV metadata server's ``/stats``
endpoint. This tool snapshots that endpoint and turns a before/after delta into
a ``cost_breakdown`` string (the canonical schema's decomposition column),
attributing time to metadata commit, Raft proposal, and object writeback, plus
the cold-read-revealing object GET count.

Usage:
  decompose.py --snapshot http://127.0.0.1:7841/stats --out before.json
  ... run a phase ...
  decompose.py --snapshot http://127.0.0.1:7841/stats --out after.json
  decompose.py --before before.json --after after.json   # -> cost_breakdown
  decompose.py --selftest
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request

# label -> server stat fields (nanoseconds) summed into that bucket.
_NS_FIELDS = {
    "meta_commit": ("metadata_atomic_apply_ns", "metadata_commit_prepare_ns"),
    "raft_propose": ("metadata_raft_proposal_ns",),
    "object_writeback": ("object_writeback_upload_ns",),
}
_COUNT_FIELDS = ("object_gets", "object_puts", "read_plan_cache_misses")


def fetch(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 (local trusted URL)
        return json.loads(resp.read().decode())


def flatten(doc: dict) -> dict:
    """Flatten nested stat objects into a single numeric map."""
    out: dict[str, float] = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            out.update(flatten(value))
        elif isinstance(value, (int, float)):
            out[key] = value
    return out


def cost_breakdown(before: dict, after: dict) -> str:
    b, a = flatten(before), flatten(after)
    parts = []
    for label, fields in _NS_FIELDS.items():
        delta_ns = sum(a.get(f, 0) - b.get(f, 0) for f in fields)
        if delta_ns:
            parts.append(f"{label}={delta_ns / 1000:.0f}us")
    for field in _COUNT_FIELDS:
        delta = a.get(field, 0) - b.get(field, 0)
        if delta:
            parts.append(f"{field}={int(delta)}")
    return ";".join(parts)


def _selftest() -> int:
    before = {"metadata_store": {"metadata_atomic_apply_ns": 1000, "metadata_commit_prepare_ns": 500},
              "object": {"object_gets": 10}}
    after = {"metadata_store": {"metadata_atomic_apply_ns": 3000, "metadata_commit_prepare_ns": 1500},
             "object": {"object_gets": 42}}
    cb = cost_breakdown(before, after)
    assert "meta_commit=3us" in cb, cb        # (2000 + 1000) ns -> 3us
    assert "object_gets=32" in cb, cb
    print("decompose selftest OK")
    return 0


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="NoKV /stats latency decomposition.")
    p.add_argument("--selftest", action="store_true")
    p.add_argument("--snapshot", help="GET this stats URL and write JSON to --out")
    p.add_argument("--out", help="output path for --snapshot (default stdout)")
    p.add_argument("--before", help="before snapshot JSON")
    p.add_argument("--after", help="after snapshot JSON")
    a = p.parse_args(argv)
    if a.selftest:
        return _selftest()
    if a.snapshot:
        data = fetch(a.snapshot)
        if a.out:
            with open(a.out, "w") as handle:
                json.dump(data, handle)
        else:
            json.dump(data, sys.stdout)
        return 0
    if a.before and a.after:
        with open(a.before) as bf, open(a.after) as af:
            print(cost_breakdown(json.load(bf), json.load(af)))
        return 0
    p.error("need --selftest, --snapshot, or --before/--after")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
