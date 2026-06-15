"""PyTorch integration for NoKV.

Two pieces:

* ``NoKVRangeDataset`` / ``NoKVIterableDataset`` — datasets over the NoKV batch
  range-read fast path. Fork-safe: the native ``Client`` owns sockets and worker
  threads and must not cross a ``fork``, so each DataLoader worker lazily builds
  its own client from a picklable factory.

* ``NoKVStorageWriter`` / ``NoKVStorageReader`` — a ``torch.distributed.checkpoint``
  (DCP) backend. Each rank writes its shard as one immutable NoKV artifact; the
  DCP ``Metadata`` is published last as the atomic commit. Crucially, the read
  path groups every ``ReadItem`` by shard and issues a single NoKV
  ``read_ranges_batch`` per shard — so a resharding load (e.g. 8-way checkpoint
  into 16-way) becomes coalesced, prepared range reads instead of N point GETs.

This module imports torch lazily; importing ``nokv`` never requires torch.
"""

from __future__ import annotations

import io
import pickle
from dataclasses import dataclass
from typing import Callable, Iterator, Optional, Sequence

from .checkpoint import MANIFEST_NAME, ensure_dirs, step_dir

try:
    import torch
    from torch.utils.data import Dataset, IterableDataset, get_worker_info
except ImportError as exc:  # pragma: no cover - torch is an optional dependency.
    raise ImportError(
        "nokv.torch requires PyTorch; install torch to use the dataset and "
        "distributed-checkpoint integrations."
    ) from exc

Range = tuple[int, int]
RangeRequest = tuple[str, Sequence[Range], Optional[int], Optional[int]]
ClientFactory = Callable[[], "object"]


# --------------------------------------------------------------------- datasets
class _LazyClient:
    """Holds a picklable factory and builds the native client per process.

    The built client is never pickled, so a DataLoader worker (a forked or
    spawned process) constructs a fresh client with its own sockets/threads.
    """

    def __init__(self, factory: ClientFactory):
        self._factory = factory
        self._client = None

    def __getstate__(self):
        return {"_factory": self._factory, "_client": None}

    def get(self):
        if self._client is None:
            self._client = self._factory()
        return self._client


class NoKVRangeDataset(Dataset):
    """Map-style dataset: sample ``i`` is one ``(path, ranges, gen, gap)`` read."""

    def __init__(self, samples: Sequence[RangeRequest], client_factory: ClientFactory):
        self._samples = list(samples)
        self._client = _LazyClient(client_factory)

    def __len__(self) -> int:
        return len(self._samples)

    def __getitem__(self, index: int) -> bytes:
        path, ranges, gen, gap = _normalize_request(self._samples[index])
        packed = self._client.get().read_ranges_batch_packed([(path, ranges, gen, gap)])
        return packed[0]

    def read_batch(self, indices: Sequence[int]) -> list[bytes]:
        """Coalesced multi-sample read — one batch RPC + one prepared plan."""
        requests = [_normalize_request(self._samples[i]) for i in indices]
        return self._client.get().read_ranges_batch_packed(requests)


class NoKVIterableDataset(IterableDataset):
    """Iterable dataset that shards samples across DataLoader workers."""

    def __init__(
        self,
        samples: Sequence[RangeRequest],
        client_factory: ClientFactory,
        batch_size: int = 1,
    ):
        self._samples = list(samples)
        self._client = _LazyClient(client_factory)
        self._batch_size = max(1, int(batch_size))

    def __iter__(self) -> Iterator[bytes]:
        info = get_worker_info()
        if info is None:
            shard = self._samples
        else:
            shard = self._samples[info.id :: info.num_workers]
        client = self._client.get()
        for start in range(0, len(shard), self._batch_size):
            window = shard[start : start + self._batch_size]
            requests = [_normalize_request(sample) for sample in window]
            for packed in client.read_ranges_batch_packed(requests):
                yield packed


def _normalize_request(sample: RangeRequest) -> RangeRequest:
    path, ranges, gen, gap = sample
    return (
        path,
        [(int(offset), int(length)) for offset, length in ranges],
        gen,
        gap,
    )


# ---------------------------------------------------------- distributed checkpoint
@dataclass
class _StorageInfo:
    """Where one DCP item lives inside a shard artifact."""

    path: str
    offset: int
    length: int


# The StorageWriter/StorageReader ABCs are stable at the DCP package top level;
# the dataset half above never needs them, so a torch build without DCP still
# works. The *other* DCP types (WriteResult, WriteItemType, StorageMeta) have
# moved across minor releases, so they are resolved defensively at call time.
try:
    from torch.distributed.checkpoint import StorageReader as _StorageReader
    from torch.distributed.checkpoint import StorageWriter as _StorageWriter

    _HAS_DCP = True
except ImportError:  # pragma: no cover - DCP missing on very old torch
    _StorageReader = object  # type: ignore[assignment,misc]
    _StorageWriter = object  # type: ignore[assignment,misc]
    _HAS_DCP = False


def _require_dcp() -> None:
    if not _HAS_DCP:
        raise ImportError(
            "torch.distributed.checkpoint is unavailable; upgrade torch to use "
            "the NoKV DCP backend"
        )


def _resolve(name: str, modules):
    for module in modules:
        try:
            mod = __import__(module, fromlist=[name])
            return getattr(mod, name)
        except (ImportError, AttributeError):
            continue
    raise ImportError(f"could not locate torch DCP type {name!r}")


def _write_item_type():
    return _resolve(
        "WriteItemType",
        ("torch.distributed.checkpoint", "torch.distributed.checkpoint.metadata"),
    )


def _write_result_cls():
    return _resolve(
        "WriteResult",
        (
            "torch.distributed.checkpoint",
            "torch.distributed.checkpoint.storage",
            "torch.distributed.checkpoint.planner",
            "torch.distributed.checkpoint.metadata",
        ),
    )


class NoKVStorageWriter(_StorageWriter):  # type: ignore[misc,valid-type]
    """DCP ``StorageWriter`` that publishes each rank's shard to NoKV.

    Targets the torch >= 2.2 DCP storage ABC. Validate against your exact torch
    version before relying on it in production — the planner/Metadata contract
    has shifted across minor releases.
    """

    def __init__(self, client, run: str, step: int, *, base: str = "checkpoints"):
        _require_dcp()
        self.client = client
        self.run = run
        self.step = int(step)
        self.base = base

    @classmethod
    def validate_checkpoint_id(cls, checkpoint_id) -> bool:  # pragma: no cover
        return True

    def reset(self, checkpoint_id=None) -> None:
        if checkpoint_id is not None:
            self.run, _, raw_step = str(checkpoint_id).rpartition("/")
            self.step = int(raw_step) if raw_step.isdigit() else self.step

    def set_up_storage_writer(self, is_coordinator: bool) -> None:
        self._is_coordinator = is_coordinator
        ensure_dirs(self.client, step_dir(self.base, self.run, self.step))

    def storage_meta(self):  # pragma: no cover - optional in newer torch
        try:
            storage_meta_cls = _resolve(
                "StorageMeta",
                (
                    "torch.distributed.checkpoint",
                    "torch.distributed.checkpoint.metadata",
                ),
            )
        except ImportError:
            return None
        return storage_meta_cls(
            checkpoint_id=f"{self.run}/{self.step}", save_id=f"{self.run}/{self.step}"
        )

    def prepare_local_plan(self, plan):
        return plan

    def prepare_global_plan(self, plans):
        return plans

    def write_data(self, plan, planner):
        write_item_type = _write_item_type()
        rank = _global_rank()
        relative = f"shard_{rank}.dcp"
        path = f"{step_dir(self.base, self.run, self.step)}/{relative}"

        buffer = io.BytesIO()
        results = []
        for item in plan.items:
            offset = buffer.tell()
            data = planner.resolve_data(item)
            if item.type == write_item_type.BYTE_IO:
                buffer.write(data.getbuffer())
            else:
                torch.save(data, buffer)
            length = buffer.tell() - offset
            results.append(
                _write_result(item, length, _StorageInfo(relative, offset, length))
            )

        self.client.put_artifact(
            path,
            buffer.getvalue(),
            "nokv-dcp",
            "",
            "application/octet-stream",
            f"{self.run}/step_{self.step}/{relative}",
            0o644,
            0,
            0,
            True,
        )
        fut = torch.futures.Future()
        fut.set_result(results)
        return fut

    def finish(self, metadata, results) -> None:
        storage_data = {}
        for rank_results in results:
            for result in rank_results:
                storage_data[result.index] = result.storage_data
        metadata.storage_data = storage_data
        body = pickle.dumps(metadata, protocol=pickle.HIGHEST_PROTOCOL)
        # Metadata written last == the atomic commit point for the checkpoint.
        self.client.put_artifact(
            f"{step_dir(self.base, self.run, self.step)}/{MANIFEST_NAME}",
            body,
            "nokv-dcp",
            "",
            "application/octet-stream",
            f"{self.run}/step_{self.step}/manifest",
            0o644,
            0,
            0,
            True,
        )


class NoKVStorageReader(_StorageReader):  # type: ignore[misc,valid-type]
    """DCP ``StorageReader``. Read path coalesces every item per shard into one
    NoKV batch range read — the resharding-friendly fast path."""

    def __init__(self, client, run: str, step: int, *, base: str = "checkpoints"):
        _require_dcp()
        self.client = client
        self.run = run
        self.step = int(step)
        self.base = base
        self.storage_data = {}

    @classmethod
    def validate_checkpoint_id(cls, checkpoint_id) -> bool:  # pragma: no cover
        return True

    def reset(self, checkpoint_id=None) -> None:
        self.storage_data = {}

    def read_metadata(self):
        path = f"{step_dir(self.base, self.run, self.step)}/{MANIFEST_NAME}"
        body = self.client.cat(path)
        metadata = pickle.loads(body)
        self.storage_data = metadata.storage_data
        return metadata

    def set_up_storage_reader(self, metadata, is_coordinator: bool) -> None:
        self.storage_data = metadata.storage_data

    def prepare_local_plan(self, plan):
        return plan

    def prepare_global_plan(self, plans):
        return plans

    def read_data(self, plan, planner):
        write_item_type = _write_item_type()
        # Group read items by shard so each shard is one coalesced batch read.
        per_shard: dict[str, list] = {}
        for read_item in plan.items:
            info = self.storage_data[read_item.storage_index]
            per_shard.setdefault(info.path, []).append((read_item, info))

        for relative, reqs in per_shard.items():
            path = f"{step_dir(self.base, self.run, self.step)}/{relative}"
            ranges = [(int(info.offset), int(info.length)) for _, info in reqs]
            blobs = self.client.read_ranges_batch([(path, ranges, None, None)])[0]
            for (read_item, _info), blob in zip(reqs, blobs):
                if read_item.type == write_item_type.BYTE_IO:
                    planner.load_bytes(read_item, io.BytesIO(blob))
                else:
                    tensor = torch.load(io.BytesIO(blob), map_location="cpu")
                    tensor = _narrow_to_read_item(tensor, read_item)
                    target = planner.resolve_tensor(read_item).detach()
                    target.copy_(tensor)
                    planner.commit_tensor(read_item, target)

        fut = torch.futures.Future()
        fut.set_result(None)
        return fut


def save_checkpoint(state_dict, client, run: str, step: int, *, base: str = "checkpoints", **kwargs):
    """Convenience wrapper over ``torch.distributed.checkpoint.save``."""
    import torch.distributed.checkpoint as dcp

    return dcp.save(
        state_dict,
        storage_writer=NoKVStorageWriter(client, run, step, base=base),
        **kwargs,
    )


def load_checkpoint(state_dict, client, run: str, step: int, *, base: str = "checkpoints", **kwargs):
    """Convenience wrapper over ``torch.distributed.checkpoint.load``."""
    import torch.distributed.checkpoint as dcp

    return dcp.load(
        state_dict,
        storage_reader=NoKVStorageReader(client, run, step, base=base),
        **kwargs,
    )


def _global_rank() -> int:
    try:
        if torch.distributed.is_available() and torch.distributed.is_initialized():
            return torch.distributed.get_rank()
    except Exception:  # pragma: no cover - defensive
        pass
    return 0


def _write_result(item, length, storage_info):
    write_result_cls = _write_result_cls()
    return write_result_cls(
        index=item.index, size_in_bytes=length, storage_data=storage_info
    )


def _narrow_to_read_item(tensor, read_item):
    # For sharded/resharded tensors, slice the loaded source down to the region
    # this ReadItem wants. lengths/offsets are absent for whole-tensor items.
    lengths = getattr(read_item, "lengths", None)
    offsets = getattr(read_item, "storage_offsets", None)
    if not lengths or not offsets:
        return tensor
    for dim, (offset, length) in enumerate(zip(offsets, lengths)):
        tensor = tensor.narrow(dim, int(offset), int(length))
    return tensor
