#!/usr/bin/env python3
"""L1 native Python/fsspec benchmark for NoKV packed-shard range reads."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import PurePosixPath
from typing import Iterable, Sequence

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema  # noqa: E402

_VALID_CACHE_STATES = {"cold", "warm", "hot"}
_VALID_READ_SHAPES = {
    "ranges",
    "packed",
    "into",
    "buffer",
    "planned_buffer",
    "batch_reader",
    "epoch_reader",
}
_VALID_READ_BUFFER_MEMORY_KINDS = {"system", "page_locked"}
_EPOCH_READER_MAX_PARALLELISM = 2
_STATS_FIELDS = (
    "object_gets",
    "object_get_bytes",
    "coalesced_gets",
    "coalesced_get_bytes",
    "cache_hits",
    "cache_hit_bytes",
    "prefetch_enqueued",
    "prefetch_dropped",
    "prefetch_completed",
    "prefetch_failed",
    "prefetch_object_gets",
    "prefetch_object_get_bytes",
    "prefetch_cache_hits",
    "prefetch_cache_hit_bytes",
    "read_plan_cache_hits",
    "read_plan_cache_misses",
    "data_fabric_planned_blocks",
    "data_fabric_local_nvme_hits",
    "data_fabric_object_fallbacks",
    "data_fabric_object_gets",
    "data_fabric_object_get_bytes",
    "data_fabric_coalesced_ranges",
    "data_fabric_coalesced_range_bytes",
    "data_fabric_cache_hits",
    "data_fabric_cache_hit_bytes",
)


def payload(seed: int, length: int) -> bytes:
    return bytes(((seed + offset) % 251 for offset in range(length)))


def sample_seed(shard: int, sample: int) -> int:
    return shard * 31 + sample * 17


def shard_path(dataset_root: str, shard: int) -> str:
    return str(PurePosixPath(dataset_root) / f"shard-{shard:04d}.bin")


def selected_samples(files_per_dir: int, range_stride: int) -> list[int]:
    return list(range(0, files_per_dir, max(1, range_stride)))


def sample_ranges(samples: Sequence[int], sample_bytes: int) -> list[tuple[int, int]]:
    return [(sample * sample_bytes, sample_bytes) for sample in samples]


def make_fs(args):
    from nokv.fsspec import NoKVFileSystem

    return NoKVFileSystem(
        metadata_addr=args.metadata_addr,
        bucket=args.bucket,
        endpoint=args.endpoint,
        access_key_id=args.access_key_id,
        secret_access_key=args.secret_access_key,
        region=args.region,
        root=args.root,
        session_token=args.session_token,
        virtual_host_style=args.virtual_host_style,
        skip_signature=args.skip_signature,
        hot_object_root=args.hot_object_root,
        block_cache=bool(args.block_cache),
    )


def batched(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    size = max(1, size)
    for start in range(0, len(items), size):
        yield items[start:start + size]


def checksum_bytes(data: bytes) -> int:
    if not data:
        return 0
    return (data[0] + data[-1] + len(data)) & 0xFFFFFFFF


def checksum_window(data: bytes, start: int, length: int) -> int:
    if length == 0:
        return 0
    return (data[start] + data[start + length - 1] + length) & 0xFFFFFFFF


def stats_snapshot(fs) -> dict[str, int] | None:
    stats = getattr(fs, "stats", None)
    if stats is None:
        return None
    try:
        return {key: int(value) for key, value in stats().items()}
    except Exception as err:
        print(f"warning: failed to snapshot native stats: {err}", file=sys.stderr)
        return None


def stats_delta(before: dict[str, int] | None, after: dict[str, int] | None) -> str | None:
    if before is None or after is None:
        return None
    parts = []
    for field in _STATS_FIELDS:
        delta = int(after.get(field, 0)) - int(before.get(field, 0))
        if delta:
            parts.append(f"{field}={delta}")
    return ";".join(parts)


def verify_dataset(args) -> None:
    fs = make_fs(args)
    samples = sorted({
        0,
        min(args.files_per_dir - 1, args.files_per_dir // 2),
        args.files_per_dir - 1,
    })
    ranges = sample_ranges(samples, args.sample_bytes)
    path = shard_path(args.dataset_root, 0)
    reads = fs.read_ranges_batch([
        (path, ranges, None, args.range_coalesce_gap_bytes),
    ])
    if len(reads) != 1 or len(reads[0]) != len(samples):
        raise RuntimeError("verification read returned the wrong result shape")
    for sample, data in zip(samples, reads[0]):
        expected = payload(sample_seed(0, sample), args.sample_bytes)
        if data != expected:
            raise RuntimeError(f"verification mismatch for {path} sample={sample}")


def range_requests(args, path_batch: Sequence[str], ranges: Sequence[tuple[int, int]]):
    return [
        (path, list(ranges), None, args.range_coalesce_gap_bytes)
        for path in path_batch
    ]


def prepare_path_batches(fs, args, paths: Sequence[str], ranges: Sequence[tuple[int, int]]):
    prepared = []
    for path_batch in batched(paths, args.concurrency):
        requests = range_requests(args, path_batch, ranges)
        plan = fs.prepare_range_batch(requests)
        prepared.append((tuple(path_batch), plan))
    return prepared


def prepare_path_readers(fs, args, paths: Sequence[str], ranges: Sequence[tuple[int, int]]):
    prepared = []
    for path_batch in batched(paths, args.concurrency):
        requests = range_requests(args, path_batch, ranges)
        reader = fs.prepare_range_batch_reader(requests, args.read_buffer_memory_kind)
        if reader.memory_kind() != args.read_buffer_memory_kind:
            raise RuntimeError(
                f"RangeBatchReader memory kind {reader.memory_kind()} did not match "
                f"{args.read_buffer_memory_kind}"
            )
        prepared.append((tuple(path_batch), reader, reader.buffer(), reader.layout()))
    return prepared


def prepare_epoch_reader(fs, args, paths: Sequence[str], ranges: Sequence[tuple[int, int]]):
    path_batches = [tuple(path_batch) for path_batch in batched(paths, args.concurrency)]
    request_batches = [range_requests(args, path_batch, ranges) for path_batch in path_batches]
    epoch = fs.prepare_range_batch_epoch(request_batches, args.read_buffer_memory_kind)
    if epoch.batch_count() != len(path_batches):
        raise RuntimeError(
            f"RangeBatchEpochReader has {epoch.batch_count()} batches, expected {len(path_batches)}"
        )
    buffers = []
    layouts = []
    for index in range(epoch.batch_count()):
        if epoch.memory_kind(index) != args.read_buffer_memory_kind:
            raise RuntimeError(
                f"RangeBatchEpochReader memory kind {epoch.memory_kind(index)} did not match "
                f"{args.read_buffer_memory_kind}"
            )
        buffers.append(epoch.buffer(index))
        layouts.append(epoch.layout(index))
    return path_batches, epoch, buffers, layouts, epoch.worker_count()


def epoch_reader_parallelism(batch_count: int) -> int:
    return min(batch_count, _EPOCH_READER_MAX_PARALLELISM)


def consume_buffered_reads(
    path_batch: Sequence[str],
    reads: Sequence[tuple[int, int]],
    read_buffer,
    expected_path_len: int,
    ranges: Sequence[tuple[int, int]],
) -> tuple[int, int, int]:
    if len(reads) != len(path_batch):
        raise RuntimeError("range batch read returned the wrong path count")
    logical_reads = 0
    logical_bytes = 0
    checksum = 0
    for path, (path_start, path_len) in zip(path_batch, reads):
        if path_len != expected_path_len:
            raise RuntimeError(
                f"{path} returned {path_len} buffered bytes, expected {expected_path_len}"
            )
        cursor = 0
        for _, length in ranges:
            logical_reads += 1
            logical_bytes += length
            checksum = (
                checksum + checksum_window(read_buffer, path_start + cursor, length)
            ) & 0xFFFFFFFF
            cursor += length
    return logical_reads, logical_bytes, checksum


def read_pass(
    fs,
    args,
    paths: Sequence[str],
    ranges: Sequence[tuple[int, int]],
    prepared_batches=None,
):
    latencies_us: list[float] = []
    logical_reads = 0
    logical_bytes = 0
    checksum = 0
    batch_calls = 0
    consume_us = 0.0
    expected_path_len = sum(length for _, length in ranges)
    into_buffer = bytearray(expected_path_len * max(1, args.concurrency))
    read_buffer = None
    if args.read_shape in {"buffer", "planned_buffer"}:
        read_buffer = fs.new_read_buffer(
            expected_path_len * max(1, args.concurrency),
            memory_kind=args.read_buffer_memory_kind,
        )
        if read_buffer.memory_kind() != args.read_buffer_memory_kind:
            raise RuntimeError(
                f"ReadBuffer memory kind {read_buffer.memory_kind()} did not match "
                f"{args.read_buffer_memory_kind}"
            )

    if args.read_shape in {"planned_buffer", "batch_reader", "epoch_reader"}:
        if prepared_batches is None:
            if args.read_shape == "epoch_reader":
                prepared_batches = prepare_epoch_reader(fs, args, paths, ranges)
            elif args.read_shape == "batch_reader":
                prepared_batches = prepare_path_readers(fs, args, paths, ranges)
            else:
                prepared_batches = prepare_path_batches(fs, args, paths, ranges)
        if args.read_shape == "epoch_reader":
            path_batches = prepared_batches[0]
            prepared_batches[1].reset()
        elif args.read_shape == "batch_reader":
            path_batches = [path_batch for path_batch, _, _, _ in prepared_batches]
        else:
            path_batches = [path_batch for path_batch, _ in prepared_batches]
    else:
        path_batches = list(batched(paths, args.concurrency))

    if args.read_shape == "epoch_reader":
        _, epoch, buffers, layouts, _ = prepared_batches
        epoch.reset()
        start = time.perf_counter()
        read_order = epoch.read_all()
        latencies_us.append((time.perf_counter() - start) * 1e6)
        batch_calls += 1
        expected_order = list(range(len(path_batches)))
        if read_order != expected_order:
            raise RuntimeError(
                f"RangeBatchEpochReader returned batch order {read_order}, "
                f"expected {expected_order}"
            )
        consume_start = time.perf_counter()
        for epoch_index in read_order:
            path_batch = path_batches[epoch_index]
            batch_reads, batch_bytes, batch_checksum = consume_buffered_reads(
                path_batch,
                layouts[epoch_index],
                buffers[epoch_index],
                expected_path_len,
                ranges,
            )
            logical_reads += batch_reads
            logical_bytes += batch_bytes
            checksum = (checksum + batch_checksum) & 0xFFFFFFFF
        consume_us += (time.perf_counter() - consume_start) * 1e6
        return logical_reads, logical_bytes, latencies_us, checksum, batch_calls, consume_us

    for batch_index, path_batch in enumerate(path_batches):
        requests = None
        needed_into_bytes = expected_path_len * len(path_batch)
        if args.read_shape == "into" and len(into_buffer) < needed_into_bytes:
            into_buffer = bytearray(needed_into_bytes)
        start = time.perf_counter()
        if args.read_shape == "ranges":
            requests = range_requests(args, path_batch, ranges)
            reads = fs.read_ranges_batch(requests)
        elif args.read_shape == "packed":
            requests = range_requests(args, path_batch, ranges)
            reads = fs.read_ranges_batch_packed(requests)
        elif args.read_shape == "buffer":
            if read_buffer is None:
                raise RuntimeError("buffer read shape requires a ReadBuffer")
            requests = range_requests(args, path_batch, ranges)
            _, reads = fs.read_ranges_batch_buffer(requests, read_buffer)
        elif args.read_shape == "planned_buffer":
            if read_buffer is None:
                raise RuntimeError("planned_buffer read shape requires a ReadBuffer")
            plan = prepared_batches[batch_index][1]
            _, reads = fs.read_range_batch_plan_buffer(plan, read_buffer)
        elif args.read_shape == "batch_reader":
            reader, read_buffer, reads = prepared_batches[batch_index][1:]
            reader.read()
        else:
            requests = range_requests(args, path_batch, ranges)
            _, reads = fs.read_ranges_batch_into(requests, into_buffer)
        latencies_us.append((time.perf_counter() - start) * 1e6)
        batch_calls += 1
        if len(reads) != len(path_batch):
            raise RuntimeError("range batch read returned the wrong path count")
        consume_start = time.perf_counter()
        if args.read_shape == "ranges":
            for path, path_reads in zip(path_batch, reads):
                if len(path_reads) != len(ranges):
                    raise RuntimeError(f"{path} returned the wrong range count")
                for data in path_reads:
                    logical_reads += 1
                    logical_bytes += len(data)
                    checksum = (checksum + checksum_bytes(data)) & 0xFFFFFFFF
        elif args.read_shape == "packed":
            expected_len = expected_path_len
            for path, packed in zip(path_batch, reads):
                if len(packed) != expected_len:
                    raise RuntimeError(
                        f"{path} returned {len(packed)} packed bytes, expected {expected_len}"
                    )
                cursor = 0
                for _, length in ranges:
                    logical_reads += 1
                    logical_bytes += length
                    checksum = (checksum + checksum_window(packed, cursor, length)) & 0xFFFFFFFF
                    cursor += length
        elif args.read_shape == "into":
            for path, (path_start, path_len) in zip(path_batch, reads):
                if path_len != expected_path_len:
                    raise RuntimeError(
                        f"{path} returned {path_len} staged bytes, expected {expected_path_len}"
                    )
                cursor = 0
                for _, length in ranges:
                    logical_reads += 1
                    logical_bytes += length
                    checksum = (
                        checksum + checksum_window(into_buffer, path_start + cursor, length)
                    ) & 0xFFFFFFFF
                    cursor += length
        elif args.read_shape in {"buffer", "planned_buffer", "batch_reader"}:
            batch_reads, batch_bytes, batch_checksum = consume_buffered_reads(
                path_batch,
                reads,
                read_buffer,
                expected_path_len,
                ranges,
            )
            logical_reads += batch_reads
            logical_bytes += batch_bytes
            checksum = (checksum + batch_checksum) & 0xFFFFFFFF
        else:
            raise RuntimeError(f"unsupported read shape {args.read_shape}")
        consume_us += (time.perf_counter() - consume_start) * 1e6

    return logical_reads, logical_bytes, latencies_us, checksum, batch_calls, consume_us


def read_mode(args, cache_state: str) -> str:
    if cache_state == "cold":
        return "new-python-client"
    if cache_state == "hot" and args.hot_object_root:
        return "new-python-client-after-tiered-hot-warmup"
    return "new-python-client-after-sdk-cache-warmup"


def state_row(args, cache_state: str, paths: Sequence[str], ranges: Sequence[tuple[int, int]], shape: str) -> str:
    fs = make_fs(args)
    warmup = None
    prepared_batches = None
    if args.read_shape == "planned_buffer":
        prepared_batches = prepare_path_batches(fs, args, paths, ranges)
    elif args.read_shape == "batch_reader":
        prepared_batches = prepare_path_readers(fs, args, paths, ranges)
    elif args.read_shape == "epoch_reader":
        prepared_batches = prepare_epoch_reader(fs, args, paths, ranges)
    stats_before_warmup = stats_snapshot(fs)
    if cache_state in {"warm", "hot"}:
        warmup = read_pass(fs, args, paths, ranges, prepared_batches=prepared_batches)
    stats_before_measured = stats_snapshot(fs)

    start = time.perf_counter()
    (
        logical_reads,
        logical_bytes,
        latencies_us,
        checksum,
        batch_calls,
        consume_us,
    ) = read_pass(fs, args, paths, ranges, prepared_batches=prepared_batches)
    seconds = time.perf_counter() - start
    stats_after_measured = stats_snapshot(fs)
    p50, p99 = schema.percentiles_us(latencies_us)

    cost_parts = [
        "sdk=python-fsspec",
        "range_batch_open=true",
        f"read_shape={args.read_shape}",
        f"read_mode={read_mode(args, cache_state)}",
        f"batch_calls={batch_calls}",
        f"semantic_ranges={logical_reads}",
        f"semantic_read_bytes={logical_bytes}",
        f"checksum={checksum}",
        f"max_gap_bytes={max(0, args.range_coalesce_gap_bytes)}",
        f"block_cache={int(bool(args.block_cache))}",
    ]
    if warmup is not None:
        (
            warmup_reads,
            warmup_bytes,
            warmup_latencies_us,
            warmup_checksum,
            warmup_calls,
            warmup_consume_us,
        ) = warmup
        cost_parts.extend([
            f"warmup_batch_calls={warmup_calls}",
            f"warmup_semantic_ranges={warmup_reads}",
            f"warmup_semantic_read_bytes={warmup_bytes}",
            f"warmup_checksum={warmup_checksum}",
            f"warmup_native_read_us={sum(warmup_latencies_us):.2f}",
            f"warmup_python_consume_us={warmup_consume_us:.2f}",
        ])
    warmup_stats = stats_delta(stats_before_warmup, stats_before_measured)
    if warmup_stats:
        cost_parts.append(f"warmup_stats=[{warmup_stats}]")
    measured_stats = stats_delta(stats_before_measured, stats_after_measured)
    if measured_stats:
        cost_parts.append(f"measured_stats=[{measured_stats}]")
    if args.hot_object_root:
        cost_parts.append("hot_object_root=set")
    cost_parts.append(f"native_read_us={sum(latencies_us):.2f}")
    cost_parts.append(f"python_consume_us={consume_us:.2f}")
    if args.read_shape in {"buffer", "planned_buffer", "batch_reader", "epoch_reader"}:
        cost_parts.append(f"read_buffer_memory_kind={args.read_buffer_memory_kind}")
    if args.read_shape in {"planned_buffer", "batch_reader", "epoch_reader"}:
        cost_parts.append("range_batch_plan=true")
    if args.read_shape in {"batch_reader", "epoch_reader"}:
        cost_parts.append("range_batch_reader=true")
    if args.read_shape == "epoch_reader":
        cost_parts.append("range_batch_epoch=true")
        epoch_batches = (len(paths) + args.concurrency - 1) // args.concurrency
        cost_parts.append(f"range_batch_epoch_batches={epoch_batches}")
        cost_parts.append("range_batch_epoch_read_all=true")
        cost_parts.append("range_batch_epoch_parallel=true")
        cost_parts.append("range_batch_epoch_persistent_workers=true")
        cost_parts.append(f"range_batch_epoch_parallelism={prepared_batches[4]}")

    return schema.row(
        boundary=schema.L1_SERVICE,
        system=args.system,
        metadata_tier=args.metadata_tier,
        object_backend=args.object_backend,
        cache_state=cache_state,
        concurrency=args.concurrency,
        tool="native",
        profile=args.profile,
        workload="ai_shard_range_read",
        phase="read",
        operations=logical_reads,
        seconds=seconds,
        bytes_total=logical_bytes,
        p50_us=p50,
        p99_us=p99,
        cost_breakdown=" ".join(cost_parts),
        shape=shape,
        caveat=(
            "L1 native Python/fsspec path; client-cache states only; "
            "not an L2 mounted NoKV-vs-JuiceFS comparison"
        ),
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NoKV native Python/fsspec L1 benchmark.")
    parser.add_argument("--metadata-addr", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--endpoint")
    parser.add_argument("--access-key-id")
    parser.add_argument("--secret-access-key")
    parser.add_argument("--region", default="auto")
    parser.add_argument("--root")
    parser.add_argument("--session-token")
    parser.add_argument("--virtual-host-style", action="store_true")
    parser.add_argument("--skip-signature", action="store_true")
    parser.add_argument("--hot-object-root")
    parser.add_argument("--block-cache", type=int, default=1)
    parser.add_argument("--system", default="nokv")
    parser.add_argument("--metadata-tier", default="nokv-direct-wal-async")
    parser.add_argument("--object-backend", default="rustfs")
    parser.add_argument("--profile", default="smoke", choices=["smoke", "standard", "long"])
    parser.add_argument("--dataset-root", default="/dataset/shards")
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--files-per-dir", type=int, required=True)
    parser.add_argument("--sample-bytes", type=int, required=True)
    parser.add_argument("--range-stride", type=int, default=2)
    parser.add_argument("--range-coalesce-gap-bytes", type=int, default=512)
    parser.add_argument("--concurrency", type=int, default=4, help="shard requests per batch call")
    parser.add_argument("--read-shape", default="ranges", choices=sorted(_VALID_READ_SHAPES))
    parser.add_argument(
        "--read-buffer-memory-kind",
        default="system",
        choices=sorted(_VALID_READ_BUFFER_MEMORY_KINDS),
        help="memory kind for --read-shape buffer or planned_buffer",
    )
    parser.add_argument("--cache-states", default="cold,warm")
    parser.add_argument("--verify", type=int, default=1)
    parser.add_argument("--emit-header", type=int, default=1)
    args = parser.parse_args(argv)

    if args.shard_count <= 0:
        parser.error("--shard-count must be positive")
    if args.files_per_dir <= 0:
        parser.error("--files-per-dir must be positive")
    if args.sample_bytes <= 0:
        parser.error("--sample-bytes must be positive")
    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")
    if (
        args.read_shape not in {"buffer", "planned_buffer", "batch_reader", "epoch_reader"}
        and args.read_buffer_memory_kind != "system"
    ):
        parser.error(
            "--read-buffer-memory-kind is only meaningful with --read-shape buffer, "
            "planned_buffer, batch_reader, or epoch_reader"
        )

    states = [state.strip() for state in args.cache_states.split(",") if state.strip()]
    unknown = [state for state in states if state not in _VALID_CACHE_STATES]
    if not states or unknown:
        parser.error("--cache-states must be a non-empty subset of: cold,warm,hot")
    args.cache_states = tuple(states)
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.verify:
        verify_dataset(args)

    samples = selected_samples(args.files_per_dir, args.range_stride)
    ranges = sample_ranges(samples, args.sample_bytes)
    paths = [shard_path(args.dataset_root, shard) for shard in range(args.shard_count)]
    shape = (
        f"python_fsspec=true shard_count={args.shard_count} "
        f"samples_per_shard={args.files_per_dir} selected_samples_per_shard={len(samples)} "
        f"sample_bytes={args.sample_bytes} range_stride={max(1, args.range_stride)} "
        f"max_gap_bytes={max(0, args.range_coalesce_gap_bytes)} "
        f"range_batch_size={max(1, args.concurrency)} "
        f"read_shape={args.read_shape}"
    )
    if args.read_shape in {"buffer", "planned_buffer", "batch_reader", "epoch_reader"}:
        shape += f" read_buffer_memory_kind={args.read_buffer_memory_kind}"
    if args.read_shape in {"planned_buffer", "batch_reader", "epoch_reader"}:
        shape += " range_batch_plan=true"
    if args.read_shape in {"batch_reader", "epoch_reader"}:
        shape += " range_batch_reader=true"
    if args.read_shape == "epoch_reader":
        shape += " range_batch_epoch=true"
        shape += " range_batch_epoch_read_all=true"
        shape += " range_batch_epoch_parallel=true"
        shape += " range_batch_epoch_persistent_workers=true"
        epoch_batches = (args.shard_count + args.concurrency - 1) // args.concurrency
        shape += f" range_batch_epoch_parallelism={epoch_reader_parallelism(epoch_batches)}"

    if args.emit_header:
        print(schema.header(), flush=True)
    for cache_state in args.cache_states:
        print(state_row(args, cache_state, paths, ranges, shape), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
