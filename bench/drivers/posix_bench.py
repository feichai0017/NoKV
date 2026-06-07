#!/usr/bin/env python3
"""Self-contained, JuiceFS-``bench``-shaped L2 mount driver.

Runs identical POSIX I/O through any mountpoint (NoKV or JuiceFS) and emits the
canonical benchmark schema (``bench/drivers/schema.py``). It only does
open/read/write/stat through the mount, so the *same* code measures both
filesystems at the *same* boundary (L2). The producer of every row records its
``boundary``/``system``/``metadata_tier``/``cache_state``/``concurrency`` so the
comparison can never silently mix a fast in-process path against a mounted one.

This module carries the product-thesis workloads (metadata create/list,
checkpoint write, training read) and the FS-primitive family
(bigfile/smallfile/metadata tree, à la ``juicefs bench``). Both families share
the same CLI and canonical CSV schema.

``training_read`` is the workload that motivated the framework: it is
read-after-write, so a single number conflates cold object reads with
cache-served reads. This driver emits BOTH a ``cold`` row (kernel page cache
bypassed via ``posix_fadvise(DONTNEED)`` on Linux / ``F_NOCACHE`` on macOS, so
the read reaches the filesystem and exposes block-cache write-through behaviour)
and a ``warm`` row (served from cache after a warm-up pass).
"""

from __future__ import annotations

import argparse
import fcntl
import os
import platform
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema  # noqa: E402  (local module, after sys.path injection)

_F_NOCACHE = getattr(fcntl, "F_NOCACHE", 48)  # macOS fcntl command
_READ_CHUNK = 1 << 20  # 1 MiB


# --------------------------------------------------------------------------- #
# I/O helpers
# --------------------------------------------------------------------------- #
def payload(seed: int, length: int) -> bytes:
    return bytes(((seed + offset) % 251 for offset in range(length)))


def write_file(path: Path, data: bytes, do_fsync: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(data)
        if do_fsync:
            handle.flush()
            os.fsync(handle.fileno())


def visible_entries(path: Path):
    return sorted(entry for entry in path.iterdir() if not entry.name.startswith("._"))


def read_file_warm(path: Path) -> int:
    with path.open("rb") as handle:
        return len(handle.read())


def read_file_cold(path: Path) -> int:
    """Read a file while bypassing the kernel page cache, so the read reaches
    the filesystem instead of being served by the OS. Returns bytes read."""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        if platform.system() == "Darwin":
            try:
                fcntl.fcntl(fd, _F_NOCACHE, 1)
            except OSError:
                pass
        if hasattr(os, "posix_fadvise"):
            try:
                # Evict any already-cached pages for this file before reading.
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            except OSError:
                pass
        total = 0
        while True:
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            total += len(chunk)
        return total
    finally:
        os.close(fd)


def _cold_read_mode() -> str:
    if platform.system() == "Darwin":
        return "f-nocache"
    if hasattr(os, "posix_fadvise"):
        return "fadvise-dontneed"
    return "buffered-fallback"


# --------------------------------------------------------------------------- #
# Workloads (product-thesis family). Each returns one or more canonical rows.
# --------------------------------------------------------------------------- #
def run_metadata_create_list(args, root: Path) -> list[str]:
    metadata_root = root / "metadata"
    create_latencies_us: list[float] = []
    create_start = time.perf_counter()
    op_start = time.perf_counter()
    metadata_root.mkdir()
    create_latencies_us.append((time.perf_counter() - op_start) * 1e6)
    created = 1
    for shard in range(args.dataset_dirs):
        shard_dir = metadata_root / f"dir-{shard:04d}"
        op_start = time.perf_counter()
        shard_dir.mkdir()
        create_latencies_us.append((time.perf_counter() - op_start) * 1e6)
        created += 1
        for file_index in range(args.files_per_dir):
            path = shard_dir / f"file-{file_index:05d}.bin"
            op_start = time.perf_counter()
            with path.open("wb"):
                pass
            create_latencies_us.append((time.perf_counter() - op_start) * 1e6)
            created += 1
    create_seconds = time.perf_counter() - create_start
    p50, p99 = schema.percentiles_us(create_latencies_us)
    base_shape = (
        f"dataset_dirs={args.dataset_dirs} files_per_dir={args.files_per_dir} "
        f"file_body=metadata-only"
    )

    list_latencies_us: list[float] = []
    list_ops = 0
    checksum = 0
    list_start = time.perf_counter()
    list_dirs = [metadata_root] + [
        metadata_root / f"dir-{shard:04d}" for shard in range(args.dataset_dirs)
    ]
    for directory in list_dirs:
        op_start = time.perf_counter()
        entries = visible_entries(directory)
        list_latencies_us.append((time.perf_counter() - op_start) * 1e6)
        checksum += len(entries)
        list_ops += 1
    list_seconds = time.perf_counter() - list_start
    list_p50, list_p99 = schema.percentiles_us(list_latencies_us)

    return [
        schema.row(
            boundary=schema.L2_MOUNT,
            system=args.system,
            metadata_tier=args.metadata_tier,
            object_backend=args.object_backend,
            cache_state="warm",
            concurrency=1,  # product-thesis workloads are sequential (latency, not a throughput sweep)
            tool="native",
            profile=args.profile,
            workload="metadata_create_list",
            phase="create",
            operations=created,
            seconds=create_seconds,
            p50_us=p50,
            p99_us=p99,
            shape=base_shape,
        ),
        schema.row(
            boundary=schema.L2_MOUNT,
            system=args.system,
            metadata_tier=args.metadata_tier,
            object_backend=args.object_backend,
            cache_state="warm",
            concurrency=1,  # product-thesis workloads are sequential (latency, not a throughput sweep)
            tool="native",
            profile=args.profile,
            workload="metadata_create_list",
            phase="list",
            operations=list_ops,
            seconds=list_seconds,
            p50_us=list_p50,
            p99_us=list_p99,
            shape=f"{base_shape} checksum={checksum}",
        ),
    ]


def run_checkpoint_write(args, root: Path) -> list[str]:
    checkpoints = root / "checkpoints"
    checkpoints.mkdir()
    write_file(checkpoints / "latest.ckpt", payload(0, args.checkpoint_bytes), args.fsync)
    latencies_us: list[float] = []
    start = time.perf_counter()
    checksum = 0
    for step in range(args.checkpoint_steps):
        stage = checkpoints / f".stage-{step:06d}"
        data = payload(step, args.checkpoint_bytes)
        op_start = time.perf_counter()
        write_file(stage, data, args.fsync)
        os.replace(stage, checkpoints / "latest.ckpt")
        latencies_us.append((time.perf_counter() - op_start) * 1e6)
        checksum += data[0] if data else 0
    seconds = time.perf_counter() - start
    p50, p99 = schema.percentiles_us(latencies_us)
    shape = f"checkpoint_steps={args.checkpoint_steps} checkpoint_bytes={args.checkpoint_bytes}"
    return [
        schema.row(
            boundary=schema.L2_MOUNT,
            system=args.system,
            metadata_tier=args.metadata_tier,
            object_backend=args.object_backend,
            cache_state="warm",
            concurrency=1,  # product-thesis workloads are sequential (latency, not a throughput sweep)
            tool="native",
            profile=args.profile,
            workload="checkpoint",
            phase="write",
            operations=args.checkpoint_steps,
            seconds=seconds,
            bytes_total=args.checkpoint_steps * args.checkpoint_bytes,
            p50_us=p50,
            p99_us=p99,
            shape=shape,
        )
    ]


def _seed_training_dataset(args, dataset: Path) -> None:
    for shard in range(args.dataset_dirs):
        shard_dir = dataset / f"shard-{shard:04d}"
        shard_dir.mkdir(parents=True, exist_ok=True)
        for file_index in range(args.files_per_dir):
            # fsync the seed so cold reads can actually evict clean pages.
            write_file(
                shard_dir / f"sample-{file_index:05d}.bin",
                payload(shard * 31 + file_index * 17, args.sample_bytes),
                do_fsync=True,
            )


def _training_read_pass(args, dataset: Path, reader) -> tuple[int, int, list[float]]:
    samples = 0
    total_bytes = 0
    latencies_us: list[float] = []
    for shard in range(args.dataset_dirs):
        shard_dir = dataset / f"shard-{shard:04d}"
        for entry in visible_entries(shard_dir):
            op_start = time.perf_counter()
            total_bytes += reader(entry)
            latencies_us.append((time.perf_counter() - op_start) * 1e6)
            samples += 1
    return samples, total_bytes, latencies_us


def run_training_read(args, root: Path) -> list[str]:
    """Emit a cold row (page-cache bypassed) and a warm row (cache-served).

    The gap between them is exactly the block-cache write-through behaviour: a
    filesystem that caches written blocks locally serves the cold pass nearly as
    fast as the warm pass; one that only uploads-and-forgets pays a full object
    GET per sample on the cold pass.
    """
    dataset = root / "dataset"
    dataset.mkdir()
    _seed_training_dataset(args, dataset)
    shape = (
        f"dataset_dirs={args.dataset_dirs} files_per_dir={args.files_per_dir} "
        f"sample_bytes={args.sample_bytes}"
    )

    rows: list[str] = []

    # Cold: bypass the kernel page cache so the read reaches the filesystem.
    cold_start = time.perf_counter()
    cold_samples, cold_bytes, cold_lat = _training_read_pass(args, dataset, read_file_cold)
    cold_seconds = time.perf_counter() - cold_start
    p50, p99 = schema.percentiles_us(cold_lat)
    rows.append(
        schema.row(
            boundary=schema.L2_MOUNT,
            system=args.system,
            metadata_tier=args.metadata_tier,
            object_backend=args.object_backend,
            cache_state="cold",
            concurrency=1,  # product-thesis workloads are sequential (latency, not a throughput sweep)
            tool="native",
            profile=args.profile,
            workload="training_read",
            phase="read",
            operations=cold_samples,
            seconds=cold_seconds,
            bytes_total=cold_bytes,
            p50_us=p50,
            p99_us=p99,
            cost_breakdown=f"read_mode={_cold_read_mode()}",
            shape=shape,
        )
    )

    # Warm: a warm-up pass, then time a cache-served pass.
    _training_read_pass(args, dataset, read_file_warm)
    warm_start = time.perf_counter()
    warm_samples, warm_bytes, warm_lat = _training_read_pass(args, dataset, read_file_warm)
    warm_seconds = time.perf_counter() - warm_start
    p50, p99 = schema.percentiles_us(warm_lat)
    rows.append(
        schema.row(
            boundary=schema.L2_MOUNT,
            system=args.system,
            metadata_tier=args.metadata_tier,
            object_backend=args.object_backend,
            cache_state="warm",
            concurrency=1,  # product-thesis workloads are sequential (latency, not a throughput sweep)
            tool="native",
            profile=args.profile,
            workload="training_read",
            phase="read",
            operations=warm_samples,
            seconds=warm_seconds,
            bytes_total=warm_bytes,
            p50_us=p50,
            p99_us=p99,
            cost_breakdown="read_mode=buffered",
            shape=shape,
        )
    )
    return rows


# --------------------------------------------------------------------------- #
# FS-primitive family (juicefs-bench-shaped): bigfile / smallfile / stat /
# metadata tree. Each runs under a thread pool of `--concurrency` workers and
# reports per-operation p50/p99.
# --------------------------------------------------------------------------- #
KiB = 1024
MiB = 1024 * 1024

_FSPRIM_PROFILES = {
    "smoke": dict(bigfile_bytes=16 * MiB, smallfile_bytes=128 * KiB, smallfiles=100,
                  mdtest_b=3, mdtest_i=4, mdtest_z=2),
    "standard": dict(bigfile_bytes=256 * MiB, smallfile_bytes=128 * KiB, smallfiles=400,
                     mdtest_b=6, mdtest_i=8, mdtest_z=3),
    "long": dict(bigfile_bytes=1024 * MiB, smallfile_bytes=128 * KiB, smallfiles=1000,
                 mdtest_b=6, mdtest_i=8, mdtest_z=4),
}


def _fsprim(args):
    return _FSPRIM_PROFILES.get(args.profile, _FSPRIM_PROFILES["smoke"])


def run_pool(items, fn, concurrency):
    """Run fn(item) over items with `concurrency` workers.

    Returns (elapsed_seconds, [latency_us, ...]). Single worker stays in-thread
    so the simplest case has no executor overhead.
    """
    if concurrency <= 1:
        latencies = []
        start = time.perf_counter()
        for item in items:
            op = time.perf_counter()
            fn(item)
            latencies.append((time.perf_counter() - op) * 1e6)
        return time.perf_counter() - start, latencies

    from concurrent.futures import ThreadPoolExecutor

    def timed(item):
        op = time.perf_counter()
        fn(item)
        return (time.perf_counter() - op) * 1e6

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        latencies = list(pool.map(timed, items))
    return time.perf_counter() - start, latencies


def _fsprim_row(args, workload, phase, cache_state, operations, seconds, latencies,
                bytes_total=0, cost="", extra_shape=""):
    p50, p99 = schema.percentiles_us(latencies)
    return schema.row(
        boundary=schema.L2_MOUNT, system=args.system, metadata_tier=args.metadata_tier,
        object_backend=args.object_backend, cache_state=cache_state,
        concurrency=args.concurrency, tool="native", profile=args.profile,
        workload=workload, phase=phase, operations=operations, seconds=seconds,
        bytes_total=bytes_total, p50_us=p50, p99_us=p99, cost_breakdown=cost,
        shape=extra_shape,
    )


def _read_phases(args, files, total_bytes, workload, shape):
    """Cold (page-cache bypass) then warm read rows shared by big/small file."""
    rows = []
    csec, clat = run_pool(files, read_file_cold, args.concurrency)
    rows.append(_fsprim_row(args, workload, "read", "cold", len(files), csec, clat,
                            total_bytes, cost=f"read_mode={_cold_read_mode()}", extra_shape=shape))
    run_pool(files, read_file_warm, args.concurrency)  # warm-up
    wsec, wlat = run_pool(files, read_file_warm, args.concurrency)
    rows.append(_fsprim_row(args, workload, "read", "warm", len(files), wsec, wlat,
                            total_bytes, cost="read_mode=buffered", extra_shape=shape))
    return rows


def run_bigfile(args, root):
    sh = _fsprim(args)
    base = root / "bigfile"
    base.mkdir()
    block = payload(7, MiB)
    nblocks = max(1, sh["bigfile_bytes"] // MiB)
    workers = max(1, args.concurrency)
    files = [base / f"big-{i:03d}.bin" for i in range(workers)]
    per_file = nblocks * MiB
    total = per_file * len(files)
    shape = f"bigfile_bytes={per_file} files={len(files)} io=1MiB"

    def writer(path):
        with path.open("wb") as handle:
            for _ in range(nblocks):
                handle.write(block)
            if args.fsync:
                handle.flush()
                os.fsync(handle.fileno())

    wsec, wlat = run_pool(files, writer, args.concurrency)
    rows = [_fsprim_row(args, "bigfile", "write", "warm", len(files), wsec, wlat, total, extra_shape=shape)]
    rows.extend(_read_phases(args, files, total, "bigfile", shape))
    return rows


def run_smallfile(args, root):
    sh = _fsprim(args)
    base = root / "smallfile"
    base.mkdir()
    data = payload(11, sh["smallfile_bytes"])
    count = sh["smallfiles"] * max(1, args.concurrency)
    files = [base / f"small-{i:06d}.bin" for i in range(count)]
    total = count * sh["smallfile_bytes"]
    shape = f"smallfile_bytes={sh['smallfile_bytes']} files={count}"

    def writer(path):
        with path.open("wb") as handle:
            handle.write(data)
            if args.fsync:
                handle.flush()
                os.fsync(handle.fileno())

    wsec, wlat = run_pool(files, writer, args.concurrency)
    rows = [_fsprim_row(args, "smallfile", "write", "warm", count, wsec, wlat, total, extra_shape=shape)]
    rows.extend(_read_phases(args, files, total, "smallfile", shape))
    ssec, slat = run_pool(files, os.stat, args.concurrency)
    rows.append(_fsprim_row(args, "smallfile", "stat", "warm", count, ssec, slat, extra_shape=shape))
    return rows


def _mdtest_dir_levels(base, branches, depth):
    """Directory paths of a balanced tree grouped by depth level."""
    levels = [[base]]
    frontier = [base]
    for _ in range(depth):
        nxt = [parent / f"d{i}" for parent in frontier for i in range(branches)]
        levels.append(nxt)
        frontier = nxt
    return levels


def _mdtest_dirs(base, branches, depth):
    """Directory paths of a balanced tree (branching `branches`, depth `depth`)."""
    dirs = []
    for level in _mdtest_dir_levels(base, branches, depth):
        dirs.extend(level)
    return dirs


def run_metadata_tree(args, root):
    sh = _fsprim(args)
    base = root / "mdtree"
    branches, depth, items = sh["mdtest_b"], sh["mdtest_z"], sh["mdtest_i"]
    shape = f"mdtest b={branches} i={items} z={depth}"
    dir_levels = _mdtest_dir_levels(base, branches, depth)
    dirs = [directory for level in dir_levels for directory in level]

    csec = 0.0
    clat: list[float] = []
    for level in dir_levels:
        level_sec, level_lat = run_pool(level, lambda d: d.mkdir(), args.concurrency)
        csec += level_sec
        clat.extend(level_lat)
    rows = [_fsprim_row(args, "metadata", "dir-create", "warm", len(dirs), csec, clat, extra_shape=shape)]

    files = [d / f"f{i:03d}" for d in dirs for i in range(items)]

    def touch(path):
        with path.open("wb"):
            pass

    fsec, flat = run_pool(files, touch, args.concurrency)
    rows.append(_fsprim_row(args, "metadata", "file-create", "warm", len(files), fsec, flat, extra_shape=shape))
    ssec, slat = run_pool(files, os.stat, args.concurrency)
    rows.append(_fsprim_row(args, "metadata", "file-stat", "warm", len(files), ssec, slat, extra_shape=shape))
    lsec, llat = run_pool(dirs, lambda d: list(d.iterdir()), args.concurrency)
    rows.append(_fsprim_row(args, "metadata", "dir-list", "warm", len(dirs), lsec, llat, extra_shape=shape))
    dsec, dlat = run_pool(files, lambda p: p.unlink(), args.concurrency)
    rows.append(_fsprim_row(args, "metadata", "file-delete", "warm", len(files), dsec, dlat, extra_shape=shape))
    return rows


WORKLOADS = {
    # Product-thesis family (the AI-training shapes; the default set).
    "metadata_create_list": run_metadata_create_list,
    "checkpoint": run_checkpoint_write,
    "training_read": run_training_read,
    # FS-primitive family (juicefs-bench-shaped; selected via --workloads).
    "bigfile": run_bigfile,
    "smallfile": run_smallfile,
    "metadata": run_metadata_tree,
}


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="L2 mount benchmark driver (canonical schema).")
    parser.add_argument("--system", required=True, help="nokv | juicefs (label only)")
    parser.add_argument("--mount", required=True, type=Path, help="mountpoint to drive")
    parser.add_argument("--metadata-tier", required=True, help="durability/consensus tier label")
    parser.add_argument("--object-backend", default="rustfs", help="object backend label")
    parser.add_argument("--profile", default="smoke", choices=["smoke", "standard", "long"])
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument(
        "--workloads",
        default="metadata_create_list,checkpoint,training_read",
        help="comma-separated subset of: " + ",".join(WORKLOADS),
    )
    parser.add_argument("--dataset-dirs", type=int, required=True)
    parser.add_argument("--files-per-dir", type=int, required=True)
    parser.add_argument("--sample-bytes", type=int, required=True)
    parser.add_argument("--checkpoint-bytes", type=int, required=True)
    parser.add_argument("--checkpoint-steps", type=int, required=True)
    parser.add_argument("--fsync", type=int, default=0)
    parser.add_argument("--emit-header", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    import shutil
    import tempfile

    if args.emit_header:
        print(schema.header(), flush=True)

    requested = [name.strip() for name in args.workloads.split(",") if name.strip()]
    unknown = [name for name in requested if name not in WORKLOADS]
    if unknown:
        print(f"error: unknown workloads: {', '.join(unknown)}", file=sys.stderr)
        return 2

    mount = args.mount.resolve()
    root = Path(tempfile.mkdtemp(prefix=f"nokv-bench-{args.system}-", dir=str(mount)))
    try:
        for name in requested:
            try:
                for line in WORKLOADS[name](args, root):
                    print(line, flush=True)
            except Exception as err:  # noqa: BLE001 — one workload must not abort the run
                # A workload can fail on a backend missing an S3 feature (e.g.
                # multipart upload for large objects). Record it as an explicit
                # caveat row so the rest of the matrix still completes and the
                # failure is visible rather than silently dropped.
                print(
                    schema.row(
                        boundary=schema.L2_MOUNT,
                        system=args.system,
                        metadata_tier=args.metadata_tier,
                        object_backend=args.object_backend,
                        cache_state="n/a",
                        concurrency=args.concurrency,
                        tool="native",
                        profile=args.profile,
                        workload=name,
                        phase="n/a",
                        operations=0,
                        seconds=0.0,
                        caveat=f"workload-failed:{type(err).__name__}:{str(err)[:120]}",
                    ),
                    flush=True,
                )
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
