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
cache-served reads. This driver emits a ``cold`` row (kernel page cache bypassed
via ``posix_fadvise(DONTNEED)`` on Linux / ``F_NOCACHE`` on macOS, so the read
reaches the filesystem), a ``warm`` row (served from cache after a buffered
warm-up pass), and an optional ``hot`` row (kernel-cache-bypassed warm-up, then
kernel-cache-bypassed measured pass, so the filesystem/client cache is warmed
without letting the kernel page cache hide the mounted data path).
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import os
import platform
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import decompose  # noqa: E402  (local module, after sys.path injection)
import schema  # noqa: E402  (local module, after sys.path injection)

_F_NOCACHE = getattr(fcntl, "F_NOCACHE", 48)  # macOS fcntl command
_READ_CHUNK = 1 << 20  # 1 MiB
_VALID_CACHE_STATES = {"cold", "warm", "hot"}


# --------------------------------------------------------------------------- #
# I/O helpers
# --------------------------------------------------------------------------- #
def payload(seed: int, length: int) -> bytes:
    return bytes(((seed + offset) % 251 for offset in range(length)))


def write_file(path: Path, data: bytes, do_fsync: bool, cache_data: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        apply_write_cache_hints(handle.fileno(), cache_data)
        handle.write(data)
        if do_fsync:
            handle.flush()
            os.fsync(handle.fileno())
        drop_file_cache_hints(handle.fileno(), cache_data)


def visible_entries(path: Path):
    return sorted(entry for entry in path.iterdir() if not entry.name.startswith("._"))


def _read_file_loop(path: Path, bypass_kernel_cache: bool) -> int:
    expected_size = path.stat().st_size
    fd = os.open(str(path), os.O_RDONLY)
    try:
        apply_read_cache_hints(fd, bypass_kernel_cache)
        total = 0
        while True:
            try:
                chunk = os.read(fd, _READ_CHUNK)
            except OSError as err:
                if err.errno == errno.ENXIO and total >= expected_size:
                    break
                raise
            if not chunk:
                break
            total += len(chunk)
        return total
    finally:
        os.close(fd)


def apply_read_cache_hints(fd: int, bypass_kernel_cache: bool) -> None:
    if bypass_kernel_cache and platform.system() == "Darwin":
        try:
            fcntl.fcntl(fd, _F_NOCACHE, 1)
        except OSError:
            pass
    if bypass_kernel_cache and hasattr(os, "posix_fadvise"):
        try:
            # Evict any already-cached pages for this file before reading.
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass


def apply_write_cache_hints(fd: int, cache_data: bool) -> None:
    if not cache_data and platform.system() == "Darwin":
        try:
            fcntl.fcntl(fd, _F_NOCACHE, 1)
        except OSError:
            pass


def drop_file_cache_hints(fd: int, cache_data: bool) -> None:
    if cache_data:
        return
    if hasattr(os, "posix_fadvise"):
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass


def read_file_warm(path: Path) -> int:
    return _read_file_loop(path, bypass_kernel_cache=False)


def read_file_cold(path: Path) -> int:
    """Read a file while bypassing the kernel page cache, so the read reaches
    the filesystem instead of being served by the OS. Returns bytes read."""
    return _read_file_loop(path, bypass_kernel_cache=True)


def _cold_read_mode() -> str:
    if platform.system() == "Darwin":
        return "f-nocache"
    if hasattr(os, "posix_fadvise"):
        return "fadvise-dontneed"
    return "buffered-fallback"


def _hot_read_mode() -> str:
    mode = _cold_read_mode()
    return f"{mode} after_{mode}_warmup=1"


def _cache_state_enabled(args, state: str) -> bool:
    return state in args.cache_states


def _stats_snapshot(args) -> dict | None:
    if not args.stats_url:
        return None
    try:
        return decompose.fetch(args.stats_url)
    except Exception as err:  # noqa: BLE001 - stats are diagnostic only.
        print(f"warning: failed to snapshot stats from {args.stats_url}: {err}", file=sys.stderr)
        return None


def _stats_delta(before: dict | None, after: dict | None) -> str | None:
    if before is None or after is None:
        return None
    return decompose.cost_breakdown(before, after) or "none"


def _stats_field_delta(before: dict | None, after: dict | None, field: str) -> int | None:
    if before is None or after is None:
        return None
    flat_before = decompose.flatten(before)
    flat_after = decompose.flatten(after)
    delta = flat_after.get(field, 0) - flat_before.get(field, 0)
    return max(0, int(delta))


def _stats_read_coverage(
    before: dict | None,
    after: dict | None,
    expected_requests: int,
) -> str | None:
    observed = _stats_field_delta(before, after, "fuse_read_requests")
    if observed is None:
        return None
    expected = max(0, expected_requests)
    coverage = observed / expected if expected else 0.0
    return f"observed_fuse_read_requests={observed} expected_fuse_read_requests={expected} coverage={coverage:.4f}"


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
    cache_seed = not _cache_state_enabled(args, "cold")
    for shard in range(args.dataset_dirs):
        shard_dir = dataset / f"shard-{shard:04d}"
        shard_dir.mkdir(parents=True, exist_ok=True)
        for file_index in range(args.files_per_dir):
            # The seed write is outside the timed phase. Keep cold rows honest
            # by making the data clean and avoiding kernel-cache residency.
            write_file(
                shard_dir / f"sample-{file_index:05d}.bin",
                payload(shard * 31 + file_index * 17, args.sample_bytes),
                do_fsync=True,
                cache_data=cache_seed,
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

    if _cache_state_enabled(args, "cold"):
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

    if _cache_state_enabled(args, "warm"):
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

    if _cache_state_enabled(args, "hot"):
        # Hot: warm filesystem/client caches through the mounted filesystem,
        # then bypass the kernel page cache again for the measured pass.
        _training_read_pass(args, dataset, read_file_cold)
        hot_start = time.perf_counter()
        hot_samples, hot_bytes, hot_lat = _training_read_pass(args, dataset, read_file_cold)
        hot_seconds = time.perf_counter() - hot_start
        p50, p99 = schema.percentiles_us(hot_lat)
        rows.append(
            schema.row(
                boundary=schema.L2_MOUNT,
                system=args.system,
                metadata_tier=args.metadata_tier,
                object_backend=args.object_backend,
                cache_state="hot",
                concurrency=1,  # product-thesis workloads are sequential (latency, not a throughput sweep)
                tool="native",
                profile=args.profile,
                workload="training_read",
                phase="read",
                operations=hot_samples,
                seconds=hot_seconds,
                bytes_total=hot_bytes,
                p50_us=p50,
                p99_us=p99,
                cost_breakdown=f"read_mode={_hot_read_mode()}",
                shape=shape,
            )
        )
    return rows


def _seed_shard_dataset(args, dataset: Path) -> list[Path]:
    shard_paths: list[Path] = []
    cache_seed = not _cache_state_enabled(args, "cold")
    for shard in range(args.dataset_dirs):
        path = dataset / f"shard-{shard:04d}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            apply_write_cache_hints(handle.fileno(), cache_seed)
            for sample in range(args.files_per_dir):
                handle.write(payload(shard * 31 + sample * 17, args.sample_bytes))
            if args.fsync or not cache_seed:
                handle.flush()
                os.fsync(handle.fileno())
            drop_file_cache_hints(handle.fileno(), cache_seed)
        shard_paths.append(path)
    return shard_paths


def _shard_range_windows(args) -> tuple[list[tuple[int, int]], int]:
    stride = max(1, args.range_stride)
    selected = list(range(0, args.files_per_dir, stride))
    raw = [(sample * args.sample_bytes, args.sample_bytes) for sample in selected]
    if not raw:
        return [], 0

    max_gap = max(0, args.range_coalesce_gap_bytes)
    merged: list[tuple[int, int]] = []
    start, length = raw[0]
    end = start + length
    for next_start, next_length in raw[1:]:
        next_end = next_start + next_length
        if next_start - end <= max_gap:
            end = max(end, next_end)
        else:
            merged.append((start, end - start))
            start, end = next_start, next_end
    merged.append((start, end - start))
    return merged, len(selected)


def _pread_full(fd: int, offset: int, length: int) -> tuple[int, int]:
    total = 0
    checksum = 0
    while total < length:
        size = min(_READ_CHUNK, length - total)
        if hasattr(os, "pread"):
            data = os.pread(fd, size, offset + total)
        else:
            os.lseek(fd, offset + total, os.SEEK_SET)
            data = os.read(fd, size)
        if not data:
            raise OSError(errno.EIO, f"short pread at offset {offset + total}")
        total += len(data)
        checksum = (checksum + data[0] + data[-1] + len(data)) & 0xFFFFFFFF
    return total, checksum


def _shard_range_read_pass(
    shard_paths: list[Path],
    windows: list[tuple[int, int]],
    bypass_kernel_cache: bool,
    concurrency: int,
) -> tuple[int, int, list[float], int]:
    def read_shard(path: Path) -> tuple[int, int, list[float], int]:
        shard_ranges = 0
        shard_bytes = 0
        shard_checksum = 0
        shard_latencies_us: list[float] = []
        fd = os.open(str(path), os.O_RDONLY)
        try:
            apply_read_cache_hints(fd, bypass_kernel_cache)
            for offset, length in windows:
                op_start = time.perf_counter()
                read_bytes, partial = _pread_full(fd, offset, length)
                shard_latencies_us.append((time.perf_counter() - op_start) * 1e6)
                shard_ranges += 1
                shard_bytes += read_bytes
                shard_checksum = (shard_checksum + partial) & 0xFFFFFFFF
        finally:
            os.close(fd)
        return shard_ranges, shard_bytes, shard_latencies_us, shard_checksum

    if concurrency <= 1:
        results = [read_shard(path) for path in shard_paths]
    else:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(read_shard, shard_paths))

    physical_ranges = 0
    physical_bytes = 0
    checksum = 0
    latencies_us: list[float] = []
    for ranges, bytes_read, shard_latencies_us, partial in results:
        physical_ranges += ranges
        physical_bytes += bytes_read
        latencies_us.extend(shard_latencies_us)
        checksum = (checksum + partial) & 0xFFFFFFFF
    return physical_ranges, physical_bytes, latencies_us, checksum


def _ai_shard_range_row(
    args,
    cache_state: str,
    logical_samples: int,
    logical_bytes: int,
    seconds: float,
    latencies_us: list[float],
    read_mode: str,
    shape: str,
    physical_ranges: int,
    physical_bytes: int,
    checksum: int,
    warmup_physical_ranges: int | None = None,
    warmup_physical_bytes: int | None = None,
    warmup_checksum: int | None = None,
    warmup_stats: str | None = None,
    measured_stats: str | None = None,
    warmup_stats_coverage: str | None = None,
    measured_stats_coverage: str | None = None,
) -> str:
    p50, p99 = schema.percentiles_us(latencies_us)
    amplification = physical_bytes / logical_bytes if logical_bytes else 0.0
    cost_parts = [
        f"read_mode={read_mode}",
        f"physical_ranges={physical_ranges}",
        f"physical_read_bytes={physical_bytes}",
        f"read_amplification={amplification:.4f}",
        f"checksum={checksum}",
    ]
    if warmup_physical_ranges is not None:
        cost_parts.extend([
            f"warmup_physical_ranges={warmup_physical_ranges}",
            f"warmup_physical_read_bytes={warmup_physical_bytes or 0}",
            f"warmup_checksum={warmup_checksum or 0}",
        ])
    if warmup_stats is not None:
        cost_parts.append(f"warmup_stats=[{warmup_stats}]")
    if warmup_stats_coverage is not None:
        cost_parts.append(f"warmup_stats_coverage=[{warmup_stats_coverage}]")
    if measured_stats is not None:
        cost_parts.append(f"measured_stats=[{measured_stats}]")
    if measured_stats_coverage is not None:
        cost_parts.append(f"measured_stats_coverage=[{measured_stats_coverage}]")
    return schema.row(
        boundary=schema.L2_MOUNT,
        system=args.system,
        metadata_tier=args.metadata_tier,
        object_backend=args.object_backend,
        cache_state=cache_state,
        concurrency=args.concurrency,
        tool="native",
        profile=args.profile,
        workload="ai_shard_range_read",
        phase="read",
        operations=logical_samples,
        seconds=seconds,
        bytes_total=logical_bytes,
        p50_us=p50,
        p99_us=p99,
        cost_breakdown=" ".join(cost_parts),
        shape=shape,
    )


def run_ai_shard_range_read(args, root: Path) -> list[str]:
    dataset = root / "shards"
    dataset.mkdir()
    shard_paths = _seed_shard_dataset(args, dataset)
    windows, selected_per_shard = _shard_range_windows(args)
    logical_samples = len(shard_paths) * selected_per_shard
    logical_bytes = logical_samples * args.sample_bytes
    shape = (
        f"shard_count={len(shard_paths)} samples_per_shard={args.files_per_dir} "
        f"selected_samples_per_shard={selected_per_shard} sample_bytes={args.sample_bytes} "
        f"range_stride={max(1, args.range_stride)} "
        f"max_gap_bytes={max(0, args.range_coalesce_gap_bytes)} "
        f"posix_pread_windows_per_shard={len(windows)} "
        f"shard_read_workers={max(1, args.concurrency)}"
    )

    rows: list[str] = []
    if _cache_state_enabled(args, "cold"):
        cold_start = time.perf_counter()
        physical_ranges, physical_bytes, cold_lat, checksum = _shard_range_read_pass(
            shard_paths,
            windows,
            bypass_kernel_cache=True,
            concurrency=max(1, args.concurrency),
        )
        rows.append(
            _ai_shard_range_row(
                args,
                "cold",
                logical_samples,
                logical_bytes,
                time.perf_counter() - cold_start,
                cold_lat,
                _cold_read_mode(),
                shape,
                physical_ranges,
                physical_bytes,
                checksum,
            )
        )

    if _cache_state_enabled(args, "warm"):
        _shard_range_read_pass(
            shard_paths,
            windows,
            bypass_kernel_cache=False,
            concurrency=max(1, args.concurrency),
        )
        warm_start = time.perf_counter()
        physical_ranges, physical_bytes, warm_lat, checksum = _shard_range_read_pass(
            shard_paths,
            windows,
            bypass_kernel_cache=False,
            concurrency=max(1, args.concurrency),
        )
        rows.append(
            _ai_shard_range_row(
                args,
                "warm",
                logical_samples,
                logical_bytes,
                time.perf_counter() - warm_start,
                warm_lat,
                "buffered",
                shape,
                physical_ranges,
                physical_bytes,
                checksum,
            )
        )
    if _cache_state_enabled(args, "hot"):
        stats_before_warmup = _stats_snapshot(args)
        warmup_physical_ranges, warmup_physical_bytes, _, warmup_checksum = _shard_range_read_pass(
            shard_paths,
            windows,
            bypass_kernel_cache=True,
            concurrency=max(1, args.concurrency),
        )
        stats_before_measured = _stats_snapshot(args)
        hot_start = time.perf_counter()
        physical_ranges, physical_bytes, hot_lat, checksum = _shard_range_read_pass(
            shard_paths,
            windows,
            bypass_kernel_cache=True,
            concurrency=max(1, args.concurrency),
        )
        stats_after_measured = _stats_snapshot(args)
        rows.append(
            _ai_shard_range_row(
                args,
                "hot",
                logical_samples,
                logical_bytes,
                time.perf_counter() - hot_start,
                hot_lat,
                _hot_read_mode(),
                shape,
                physical_ranges,
                physical_bytes,
                checksum,
                warmup_physical_ranges,
                warmup_physical_bytes,
                warmup_checksum,
                _stats_delta(stats_before_warmup, stats_before_measured),
                _stats_delta(stats_before_measured, stats_after_measured),
                _stats_read_coverage(
                    stats_before_warmup,
                    stats_before_measured,
                    warmup_physical_ranges,
                ),
                _stats_read_coverage(
                    stats_before_measured,
                    stats_after_measured,
                    physical_ranges,
                ),
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
    """Cold, warm, and hot read rows shared by big/small file."""
    rows = []
    if _cache_state_enabled(args, "cold"):
        csec, clat = run_pool(files, read_file_cold, args.concurrency)
        rows.append(_fsprim_row(args, workload, "read", "cold", len(files), csec, clat,
                                total_bytes, cost=f"read_mode={_cold_read_mode()}", extra_shape=shape))
    if _cache_state_enabled(args, "warm"):
        run_pool(files, read_file_warm, args.concurrency)  # warm-up
        wsec, wlat = run_pool(files, read_file_warm, args.concurrency)
        rows.append(_fsprim_row(args, workload, "read", "warm", len(files), wsec, wlat,
                                total_bytes, cost="read_mode=buffered", extra_shape=shape))
    if _cache_state_enabled(args, "hot"):
        run_pool(files, read_file_cold, args.concurrency)  # filesystem/client cache warm-up
        hsec, hlat = run_pool(files, read_file_cold, args.concurrency)
        rows.append(_fsprim_row(args, workload, "read", "hot", len(files), hsec, hlat,
                                total_bytes, cost=f"read_mode={_hot_read_mode()}", extra_shape=shape))
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
    "ai_shard_range_read": run_ai_shard_range_read,
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
    parser.add_argument("--range-stride", type=int, default=2)
    parser.add_argument("--range-coalesce-gap-bytes", type=int, default=512)
    parser.add_argument(
        "--cache-states",
        default="cold,warm",
        help="comma-separated read cache states to emit: cold,warm,hot",
    )
    parser.add_argument("--fsync", type=int, default=0)
    parser.add_argument("--emit-header", type=int, default=1)
    parser.add_argument(
        "--stats-url",
        default="",
        help="optional NoKV stats endpoint for hot-row warmup/measured diagnostics",
    )
    args = parser.parse_args(argv)
    states = [state.strip() for state in args.cache_states.split(",") if state.strip()]
    unknown = [state for state in states if state not in _VALID_CACHE_STATES]
    if not states or unknown:
        parser.error("--cache-states must be a non-empty subset of: cold,warm,hot")
    args.cache_states = frozenset(states)
    return args


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
    for name in requested:
        root: Path | None = None
        try:
            root = Path(tempfile.mkdtemp(
                prefix=f"nokv-bench-{args.system}-{name}-",
                dir=str(mount),
            ))
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
            if _mount_unavailable(mount):
                break
        finally:
            if root is not None:
                shutil.rmtree(root, ignore_errors=True)
    return 0


def _mount_unavailable(mount: Path) -> bool:
    try:
        return not mount.is_dir()
    except OSError:
        return True


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
