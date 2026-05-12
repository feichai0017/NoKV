from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import RLock
from typing import Any

from langgraph.checkpoint.nokv.fsmeta_client import InodeType

UNSCOPED_PHASE = "unscoped_phase"
WRITE_PHASE = "write_phase"
GET_STATE_PHASE = "get_state_phase"
DELTA_HISTORY_PHASE = "delta_history_phase"
STORAGE_COUNT_PHASE = "storage_count_phase"

CATEGORY_CHECKPOINT_ATTRS = "checkpoint_attrs"
CATEGORY_CHANNEL_BLOB = "channel_blob"
CATEGORY_HEAD = "head"
CATEGORY_TOMBSTONE = "tombstone"
CATEGORY_WRITE_ENTRY = "write_entry"
CATEGORY_DELTA_INDEX = "delta_index"
CATEGORY_DIRECTORY = "directory"
CATEGORY_MIXED_LOOKUP = "mixed_lookup"
CATEGORY_OTHER = "other"


@dataclass
class OperationStats:
    durations_s: list[float] = field(default_factory=list)

    def add(self, duration_s: float) -> None:
        self.durations_s.append(duration_s)

    def to_json(self) -> dict[str, float | int]:
        if not self.durations_s:
            return {
                "count": 0,
                "total_ms": 0.0,
                "avg_ms": 0.0,
                "p50_ms": 0.0,
                "p95_ms": 0.0,
                "max_ms": 0.0,
            }
        ordered = sorted(self.durations_s)
        total = sum(ordered)
        return {
            "count": len(ordered),
            "total_ms": total * 1000,
            "avg_ms": total / len(ordered) * 1000,
            "p50_ms": _percentile(ordered, 0.50) * 1000,
            "p95_ms": _percentile(ordered, 0.95) * 1000,
            "max_ms": ordered[-1] * 1000,
        }


@dataclass
class PayloadOperationStats:
    durations_s: list[float] = field(default_factory=list)
    bytes_total: int = 0
    items_total: int = 0

    def add(self, duration_s: float = 0.0, *, bytes_count: int = 0, items: int = 1) -> None:
        self.durations_s.append(duration_s)
        self.bytes_total += bytes_count
        self.items_total += items

    def to_json(self) -> dict[str, float | int]:
        timing = OperationStats(self.durations_s).to_json()
        count = int(timing["count"])
        timing["bytes_total"] = self.bytes_total
        timing["items_total"] = self.items_total
        timing["avg_bytes"] = self.bytes_total / count if count else 0.0
        timing["avg_items"] = self.items_total / count if count else 0.0
        return timing


class PhaseTracker:
    def __init__(self) -> None:
        self._phase = UNSCOPED_PHASE

    @property
    def current(self) -> str:
        return self._phase

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        previous = self._phase
        self._phase = name
        try:
            yield
        finally:
            self._phase = previous


class InstrumentedFsMetaClient:
    def __init__(
        self,
        inner: Any,
        *,
        root_inode: int = 1,
        phase_tracker: PhaseTracker | None = None,
    ) -> None:
        self.inner = inner
        self.operations: dict[str, OperationStats] = {}
        self._by_phase: dict[str, dict[str, OperationStats]] = {}
        self._by_category: dict[str, dict[str, OperationStats]] = {}
        self._by_phase_category: dict[
            str, dict[str, dict[str, OperationStats]]
        ] = {}
        self._phase_tracker = phase_tracker or PhaseTracker()
        self._lock = RLock()
        self._inode_paths: dict[int, tuple[str, ...]] = {root_inode: ()}

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        with self._phase_tracker.phase(name):
            yield

    def close(self) -> None:
        self.inner.close()

    def wait_ready(self, timeout: float | None = None) -> None:
        self.inner.wait_ready(timeout=timeout)

    def create(self, **kwargs: Any) -> Any:
        return self._call("create", self.inner.create, **kwargs)

    def update_inode(self, **kwargs: Any) -> Any:
        return self._call("update_inode", self.inner.update_inode, **kwargs)

    def lookup(self, **kwargs: Any) -> Any:
        return self._call("lookup", self.inner.lookup, **kwargs)

    def lookup_plus(self, **kwargs: Any) -> Any:
        return self._call("lookup_plus", self.inner.lookup_plus, **kwargs)

    def batch_lookup_plus(self, **kwargs: Any) -> Any:
        return self._call("batch_lookup_plus", self.inner.batch_lookup_plus, **kwargs)

    def read_dir_plus(self, **kwargs: Any) -> Any:
        return self._call("read_dir_plus", self.inner.read_dir_plus, **kwargs)

    def rename(self, **kwargs: Any) -> Any:
        return self._call("rename", self.inner.rename, **kwargs)

    def unlink(self, **kwargs: Any) -> Any:
        return self._call("unlink", self.inner.unlink, **kwargs)

    def snapshot_subtree(self, **kwargs: Any) -> Any:
        return self._call("snapshot_subtree", self.inner.snapshot_subtree, **kwargs)

    def _call(self, op_name: str, fn: Callable[..., Any], **kwargs: Any) -> Any:
        phase = self._phase_tracker.current
        category = self._category_for_request(op_name, kwargs)
        start = time.perf_counter()
        try:
            result = fn(**kwargs)
        except BaseException:
            self._record(op_name, phase, category, time.perf_counter() - start)
            raise
        self._remember_result(op_name, result)
        self._record(op_name, phase, category, time.perf_counter() - start)
        return result

    def _record(
        self, op_name: str, phase: str, category: str, duration_s: float
    ) -> None:
        with self._lock:
            self.operations.setdefault(op_name, OperationStats()).add(duration_s)
            self._by_phase.setdefault(phase, {}).setdefault(
                op_name, OperationStats()
            ).add(duration_s)
            self._by_category.setdefault(category, {}).setdefault(
                op_name, OperationStats()
            ).add(duration_s)
            self._by_phase_category.setdefault(phase, {}).setdefault(
                category, {}
            ).setdefault(op_name, OperationStats()).add(duration_s)

    def _remember_result(self, op_name: str, result: Any) -> None:
        if op_name == "create":
            self._remember_pair(result)
            return
        if op_name == "lookup":
            self._remember_dentry(result)
            return
        if op_name == "lookup_plus":
            self._remember_pair(result)
            return
        if op_name == "batch_lookup_plus":
            for item in result:
                if getattr(item, "found", False):
                    self._remember_pair(getattr(item, "entry", None))
            return
        if op_name == "read_dir_plus":
            for pair in result:
                self._remember_pair(pair)

    def _remember_pair(self, result: Any) -> None:
        dentry = getattr(result, "dentry", None)
        inode = getattr(result, "inode", None)
        inode_id = getattr(inode, "inode", None)
        if dentry is None or inode_id is None:
            return
        self._remember_child_path(dentry, int(inode_id))

    def _remember_dentry(self, dentry: Any) -> None:
        inode_id = getattr(dentry, "inode", None)
        if inode_id is None:
            return
        self._remember_child_path(dentry, int(inode_id))

    def _remember_child_path(self, dentry: Any, inode_id: int) -> None:
        parent = int(getattr(dentry, "parent", 0))
        name = str(getattr(dentry, "name", ""))
        with self._lock:
            parent_path = self._inode_paths.get(parent)
            if parent_path is None:
                return
            self._inode_paths[inode_id] = (*parent_path, name)

    def _category_for_request(self, op_name: str, kwargs: dict[str, Any]) -> str:
        if op_name == "batch_lookup_plus":
            return self._category_for_batch_lookup_plus(kwargs)

        if op_name == "read_dir_plus":
            parent = _int_or_none(kwargs.get("parent"))
            if parent is None:
                return CATEGORY_OTHER
            with self._lock:
                path = self._inode_paths.get(parent)
            return _category_for_directory_path(path)

        parent = _int_or_none(kwargs.get("parent"))
        name = kwargs.get("name")
        if parent is None or name is None:
            return CATEGORY_OTHER
        with self._lock:
            parent_path = self._inode_paths.get(parent)
        if parent_path is None:
            return CATEGORY_OTHER

        full_path = (*parent_path, str(name))
        inode_type = kwargs.get("inode_type")
        if inode_type is InodeType.DIRECTORY:
            return CATEGORY_DIRECTORY
        return _category_for_entry_path(full_path)

    def _category_for_batch_lookup_plus(self, kwargs: dict[str, Any]) -> str:
        categories: set[str] = set()
        for lookup in kwargs.get("lookups") or ():
            parent = _int_or_none(getattr(lookup, "parent", None))
            name = getattr(lookup, "name", None)
            if parent is None or name is None:
                categories.add(CATEGORY_OTHER)
                continue
            with self._lock:
                parent_path = self._inode_paths.get(parent)
            path = (*parent_path, str(name)) if parent_path is not None else None
            categories.add(_category_for_entry_path(path))
        if not categories:
            return CATEGORY_OTHER
        if len(categories) == 1:
            return next(iter(categories))
        return CATEGORY_MIXED_LOOKUP

    def to_json(self) -> dict[str, Any]:
        with self._lock:
            out: dict[str, Any] = {
                name: stats.to_json()
                for name, stats in sorted(
                    self.operations.items(), key=lambda item: item[0]
                )
            }
            out["by_phase"] = _stats_by_operation_to_json(self._by_phase)
            out["by_category"] = _stats_by_operation_to_json(self._by_category)
            out["by_phase_category"] = _stats_by_phase_category_to_json(
                self._by_phase_category
            )
            return out


class InstrumentedCheckpointBodyStore:
    """Benchmark-only wrapper for body-store IO timing and byte counts."""

    empty_type = "empty"

    def __init__(self, inner: Any, *, phase_tracker: PhaseTracker) -> None:
        self.inner = inner
        self._phase_tracker = phase_tracker
        self.operations: dict[str, PayloadOperationStats] = {}
        self._by_phase: dict[str, dict[str, PayloadOperationStats]] = {}
        self._lock = RLock()
        self.empty_type = getattr(inner, "empty_type", self.empty_type)

    def put_typed(self, type_tag: str, data: bytes | None) -> Any:
        start = time.perf_counter()
        result = self.inner.put_typed(type_tag, data)
        self._record(
            "put_typed",
            time.perf_counter() - start,
            bytes_count=_bytes_len(data),
        )
        return result

    def get_typed(self, body: Any) -> tuple[str, bytes | None]:
        start = time.perf_counter()
        type_tag, data = self.inner.get_typed(body)
        self._record(
            "get_typed",
            time.perf_counter() - start,
            bytes_count=_bytes_len(data),
        )
        return type_tag, data

    def _record(self, op_name: str, duration_s: float, *, bytes_count: int) -> None:
        phase = self._phase_tracker.current
        with self._lock:
            self.operations.setdefault(op_name, PayloadOperationStats()).add(
                duration_s,
                bytes_count=bytes_count,
            )
            self._by_phase.setdefault(phase, {}).setdefault(
                op_name,
                PayloadOperationStats(),
            ).add(duration_s, bytes_count=bytes_count)

    def to_json(self) -> dict[str, Any]:
        with self._lock:
            return {
                "operations": _payload_stats_to_json(self.operations),
                "by_phase": _payload_stats_by_operation_to_json(self._by_phase),
            }


class InstrumentedSerde:
    """Benchmark-only wrapper for serializer/deserializer hydrate timing."""

    def __init__(self, inner: Any, *, phase_tracker: PhaseTracker) -> None:
        self.inner = inner
        self._phase_tracker = phase_tracker
        self.operations: dict[str, PayloadOperationStats] = {}
        self._by_phase: dict[str, dict[str, PayloadOperationStats]] = {}
        self._lock = RLock()

    def dumps_typed(self, value: Any) -> tuple[str, bytes]:
        start = time.perf_counter()
        type_tag, data = self.inner.dumps_typed(value)
        self._record(
            "dumps_typed",
            time.perf_counter() - start,
            bytes_count=_bytes_len(data),
        )
        return type_tag, data

    def loads_typed(self, value: tuple[str, bytes | None]) -> Any:
        start = time.perf_counter()
        result = self.inner.loads_typed(value)
        self._record(
            "loads_typed",
            time.perf_counter() - start,
            bytes_count=_bytes_len(value[1]),
        )
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def _record(self, op_name: str, duration_s: float, *, bytes_count: int) -> None:
        phase = self._phase_tracker.current
        with self._lock:
            self.operations.setdefault(op_name, PayloadOperationStats()).add(
                duration_s,
                bytes_count=bytes_count,
            )
            self._by_phase.setdefault(phase, {}).setdefault(
                op_name,
                PayloadOperationStats(),
            ).add(duration_s, bytes_count=bytes_count)

    def to_json(self) -> dict[str, Any]:
        with self._lock:
            return {
                "operations": _payload_stats_to_json(self.operations),
                "by_phase": _payload_stats_by_operation_to_json(self._by_phase),
            }


class InstrumentedSaverMetrics:
    """Benchmark-only counters for saver-level hydrated pending writes."""

    def __init__(self, *, phase_tracker: PhaseTracker) -> None:
        self._phase_tracker = phase_tracker
        self.operations: dict[str, PayloadOperationStats] = {}
        self._by_phase: dict[str, dict[str, PayloadOperationStats]] = {}
        self._lock = RLock()

    def record_items(self, op_name: str, items: int, duration_s: float = 0.0) -> None:
        phase = self._phase_tracker.current
        with self._lock:
            self.operations.setdefault(op_name, PayloadOperationStats()).add(
                duration_s,
                items=items,
            )
            self._by_phase.setdefault(phase, {}).setdefault(
                op_name,
                PayloadOperationStats(),
            ).add(duration_s, items=items)

    def wrap_nokv_saver(self, saver: Any) -> None:
        original = saver._load_pending_writes

        def measured_load_pending_writes(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            result = original(*args, **kwargs)
            self.record_items(
                "load_pending_writes",
                len(result),
                time.perf_counter() - start,
            )
            return result

        saver._load_pending_writes = measured_load_pending_writes

    def wrap_postgres_saver(self, saver: Any) -> None:
        original = saver._load_writes

        def measured_load_writes(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            result = original(*args, **kwargs)
            self.record_items(
                "load_pending_writes",
                len(result),
                time.perf_counter() - start,
            )
            return result

        saver._load_writes = measured_load_writes

    def record_delta_history_result(
        self, result: Mapping[str, Mapping[str, Any]]
    ) -> None:
        self.record_items(
            "delta_history_writes",
            sum(len(history.get("writes", ())) for history in result.values()),
        )
        self.record_items(
            "delta_history_seeds",
            sum(1 for history in result.values() if "seed" in history),
        )

    def to_json(self) -> dict[str, Any]:
        with self._lock:
            return {
                "operations": _payload_stats_to_json(self.operations),
                "by_phase": _payload_stats_by_operation_to_json(self._by_phase),
            }


def _category_for_entry_path(path: tuple[str, ...] | None) -> str:
    if not path:
        return CATEGORY_OTHER
    name = path[-1]
    parent = path[:-1]

    if name == "thread-tombstone":
        return CATEGORY_TOMBSTONE
    if parent and parent[-1] == "heads" and name == "latest":
        return CATEGORY_HEAD
    if parent and parent[-1] == "checkpoints" and name.startswith("c~"):
        return CATEGORY_CHECKPOINT_ATTRS
    if len(parent) >= 2 and parent[-2] == "blobs" and name.startswith("v~"):
        return CATEGORY_CHANNEL_BLOB
    if (
        len(parent) >= 2
        and parent[-2] == "writes"
        and parent[-1].startswith("c~")
        and name.startswith("w~")
    ):
        return CATEGORY_WRITE_ENTRY
    if (
        len(parent) >= 2
        and parent[-2] == "delta_channels"
        and name.startswith("dw~")
    ):
        return CATEGORY_DELTA_INDEX
    return CATEGORY_DIRECTORY


def _category_for_directory_path(path: tuple[str, ...] | None) -> str:
    if not path:
        return CATEGORY_OTHER
    name = path[-1]
    parent = path[:-1]

    if name == "heads":
        return CATEGORY_HEAD
    if name == "checkpoints":
        return CATEGORY_CHECKPOINT_ATTRS
    if len(parent) >= 1 and parent[-1] == "blobs":
        return CATEGORY_CHANNEL_BLOB
    if len(parent) >= 1 and parent[-1] == "delta_channels":
        return CATEGORY_DELTA_INDEX
    if len(parent) >= 1 and parent[-1] == "writes" and name.startswith("c~"):
        return CATEGORY_WRITE_ENTRY
    return CATEGORY_DIRECTORY


def _stats_by_operation_to_json(
    values: dict[str, dict[str, OperationStats]],
) -> dict[str, dict[str, dict[str, float | int]]]:
    return {
        group: {
            name: stats.to_json()
            for name, stats in sorted(operations.items(), key=lambda item: item[0])
        }
        for group, operations in sorted(values.items(), key=lambda item: item[0])
    }


def _stats_by_phase_category_to_json(
    values: dict[str, dict[str, dict[str, OperationStats]]],
) -> dict[str, dict[str, dict[str, dict[str, float | int]]]]:
    return {
        phase: _stats_by_operation_to_json(categories)
        for phase, categories in sorted(values.items(), key=lambda item: item[0])
    }


def _payload_stats_to_json(
    values: dict[str, PayloadOperationStats],
) -> dict[str, dict[str, float | int]]:
    return {
        name: stats.to_json()
        for name, stats in sorted(values.items(), key=lambda item: item[0])
    }


def _payload_stats_by_operation_to_json(
    values: dict[str, dict[str, PayloadOperationStats]],
) -> dict[str, dict[str, dict[str, float | int]]]:
    return {
        group: _payload_stats_to_json(operations)
        for group, operations in sorted(values.items(), key=lambda item: item[0])
    }


def _bytes_len(value: bytes | None) -> int:
    return len(value) if value is not None else 0


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _percentile(ordered: list[float], fraction: float) -> float:
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]
