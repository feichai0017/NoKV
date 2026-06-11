#!/usr/bin/env python3
"""NoKV ``/stats`` latency decomposition.

The mounted driver only sees the filesystem, so the per-op FUSE / metadata /
object cost split is read out-of-band from NoKV ``/stats`` endpoints. For
metadata-server phases this can be the metadata server; for L2 FUSE read phases
it should be the mount-local stats endpoint so foreground object reads are
visible. This tool snapshots that endpoint and turns a before/after delta into a
``cost_breakdown`` string (the canonical schema's decomposition column).

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
    "object_writeback_collect": ("object_writeback_collect_ns",),
    "object_writeback_digest": ("object_writeback_digest_ns",),
    "object_writeback_store_put": ("object_writeback_store_put_ns",),
    "object_writeback_cache_put": ("object_writeback_cache_put_ns",),
    "tiered_hot_put": ("tiered_hot_put_ns",),
    "tiered_pending_cold_put": ("tiered_pending_cold_put_ns",),
    "tiered_cold_put_enqueue": ("tiered_cold_put_enqueue_ns",),
    "local_hot_put": ("local_hot_put_total_ns",),
    "local_hot_prepare": ("local_hot_put_prepare_ns",),
    "local_hot_write": ("local_hot_put_write_ns",),
    "local_hot_sync": ("local_hot_put_sync_ns",),
    "local_hot_rename": ("local_hot_put_rename_ns",),
    "local_hot_record": ("local_hot_put_record_ns",),
}
_COUNT_FIELDS = (
    "object_gets", "object_get_bytes", "object_puts", "object_put_bytes",
    "cache_hits", "cache_hit_bytes", "block_cache_hits",
    "block_cache_hit_bytes", "read_window_hits", "read_window_hit_bytes",
    "prefetch_object_gets",
    "prefetch_object_get_bytes", "read_plan_cache_misses",
    "local_hot_puts", "local_hot_put_bytes",
)


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
              "object": {"object_gets": 10, "object_writeback_digest_ns": 1000},
              "local_hot_put_total_ns": 1000, "local_hot_puts": 1}
    after = {"metadata_store": {"metadata_atomic_apply_ns": 3000, "metadata_commit_prepare_ns": 1500},
             "object": {"object_gets": 42, "object_writeback_digest_ns": 4000},
             "local_hot_put_total_ns": 6000, "local_hot_puts": 3}
    cb = cost_breakdown(before, after)
    assert "meta_commit=3us" in cb, cb        # (2000 + 1000) ns -> 3us
    assert "object_writeback_digest=3us" in cb, cb
    assert "local_hot_put=5us" in cb, cb
    assert "object_gets=32" in cb, cb
    assert "local_hot_puts=2" in cb, cb
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
