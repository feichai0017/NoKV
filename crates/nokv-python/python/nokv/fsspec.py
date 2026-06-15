"""fsspec surface over the NoKV native SDK.

This exposes NoKV as a full fsspec filesystem — read *and* write, plus the
namespace operations (``ls``/``info``/``mkdir``/``rm``/``mv``) — so the broader
Python ecosystem (HuggingFace datasets, pyarrow, pandas, ``torch.save`` to an
fsspec file object, MosaicML Streaming, …) can target ``nokv://`` directly.

The latency-sensitive batch range-read path (``read_ranges_batch*``,
``prepare_range_batch*``, the page-locked ``ReadBuffer`` staging) is preserved as
NoKV-specific fast-path methods alongside the standard fsspec surface.
"""

from __future__ import annotations

from typing import Iterable, Optional, Sequence

from ._native import Client, RangeBatchEpochReader, RangeBatchPlan, RangeBatchReader, ReadBuffer

try:
    from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
except ImportError:  # pragma: no cover - import-time fallback for source checks.
    AbstractFileSystem = object
    AbstractBufferedFile = object

Range = tuple[int, int]
RangeRequest = tuple[str, Sequence[Range], Optional[int], Optional[int]]

# Default permission bits for namespace nodes created through the fsspec surface.
_DEFAULT_FILE_MODE = 0o644
_DEFAULT_DIR_MODE = 0o755


class NoKVFileSystem(AbstractFileSystem):
    """fsspec-compatible filesystem backed by the NoKV SDK."""

    protocol = "nokv"
    root_marker = "/"

    def __init__(self, *args, client: Optional[Client] = None, **client_options):
        # AbstractFileSystem consumes a few well-known kwargs; everything else is
        # forwarded to the native Client constructor when one is not supplied.
        fs_kwargs = {}
        for key in ("skip_instance_cache", "use_listings_cache", "listings_expiry_time", "max_paths"):
            if key in client_options:
                fs_kwargs[key] = client_options.pop(key)
        super().__init__(*args, **fs_kwargs)
        self.client = client if client is not None else Client(**client_options)

    # ------------------------------------------------------------------ paths
    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        path = super()._strip_protocol(path)
        if not path:
            return "/"
        if not path.startswith("/"):
            path = "/" + path
        return path if path == "/" else path.rstrip("/")

    # ----------------------------------------------------------------- reads
    def cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        snapshot_id = kwargs.get("snapshot_id")
        if start is None and end is None:
            return self.client.cat(path, snapshot_id)
        start = 0 if start is None else int(start)
        if end is None:
            info = self.info(path)
            end = int(info["size"])
        length = int(end) - start
        if length < 0:
            raise ValueError("end offset must be >= start offset")
        if length == 0:
            return b""
        expected_generation = kwargs.get("expected_generation")
        max_gap_bytes = kwargs.get("max_gap_bytes")
        return self.read_ranges_batch(
            [(path, [(start, length)], expected_generation, max_gap_bytes)]
        )[0][0]

    def read_ranges_batch(self, requests: Iterable[RangeRequest]) -> list[list[bytes]]:
        return self.client.read_ranges_batch(_normalize_range_requests(requests))

    def read_ranges_batch_packed(self, requests: Iterable[RangeRequest]) -> list[bytes]:
        return self.client.read_ranges_batch_packed(_normalize_range_requests(requests))

    def read_ranges_batch_into(self, requests: Iterable[RangeRequest], output=None):
        normalized = _normalize_range_requests(requests)
        if output is None:
            output = bytearray(_packed_request_bytes(normalized))
        layout = self.client.read_ranges_batch_into(normalized, output)
        return output, layout

    def new_read_buffer(self, capacity: int = 0, memory_kind: str = "system") -> ReadBuffer:
        return ReadBuffer(int(capacity), str(memory_kind))

    def prepare_range_batch(self, requests: Iterable[RangeRequest]) -> RangeBatchPlan:
        return self.client.prepare_range_batch(_normalize_range_requests(requests))

    def prepare_range_batch_reader(
        self, requests: Iterable[RangeRequest], memory_kind: str = "system"
    ) -> RangeBatchReader:
        return self.client.prepare_range_batch_reader(
            _normalize_range_requests(requests), str(memory_kind)
        )

    def prepare_range_batch_epoch(
        self,
        request_batches: Iterable[Iterable[RangeRequest]],
        memory_kind: str = "system",
    ) -> RangeBatchEpochReader:
        normalized = [_normalize_range_requests(requests) for requests in request_batches]
        return self.client.prepare_range_batch_epoch(normalized, str(memory_kind))

    def read_ranges_batch_buffer(self, requests: Iterable[RangeRequest], output=None):
        normalized = _normalize_range_requests(requests)
        if output is None:
            output = self.new_read_buffer(_packed_request_bytes(normalized))
        layout = self.client.read_ranges_batch_buffer(normalized, output)
        return output, layout

    def read_range_batch_plan_buffer(self, plan: RangeBatchPlan, output=None):
        if output is None:
            output = self.new_read_buffer(plan.output_len())
        layout = self.client.read_range_batch_plan_buffer(plan, output)
        return output, layout

    # -------------------------------------------------------------- namespace
    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        if path == "/":
            return {"name": "/", "size": 0, "type": "directory"}
        st = self.client.stat(path)
        if st is None:
            raise FileNotFoundError(path)
        return _info_from_attr(path, st)

    def exists(self, path, **kwargs):
        return self.client.exists(self._strip_protocol(path))

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        base = "" if path == "/" else path
        entries = self.client.list_dir(path)
        out = []
        for entry in entries:
            child = f"{base}/{entry['name']}"
            out.append(_info_from_attr(child, entry))
        return out if detail else [item["name"] for item in out]

    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        mode = int(kwargs.get("mode", _DEFAULT_DIR_MODE))
        uid = int(kwargs.get("uid", 0))
        gid = int(kwargs.get("gid", 0))
        if create_parents:
            self.makedirs(path, exist_ok=True, mode=mode, uid=uid, gid=gid)
            return
        self.client.mkdir(path, mode, uid, gid)

    def makedirs(self, path, exist_ok=False, **kwargs):
        path = self._strip_protocol(path)
        mode = int(kwargs.get("mode", _DEFAULT_DIR_MODE))
        uid = int(kwargs.get("uid", 0))
        gid = int(kwargs.get("gid", 0))
        parts = [p for p in path.split("/") if p]
        current = ""
        for part in parts:
            current = f"{current}/{part}"
            try:
                self.client.mkdir(current, mode, uid, gid)
            except RuntimeError:
                # Already exists (or a racing creator won): tolerate when the
                # node is present, re-raise otherwise.
                if not self.client.exists(current):
                    raise
                if not exist_ok and current == path:
                    raise FileExistsError(path)

    def rm_file(self, path):
        self.client.remove_file(self._strip_protocol(path))

    def _rm(self, path):
        self.rm_file(path)

    def rmdir(self, path):
        self.client.rmdir(self._strip_protocol(path))

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        replace = bool(kwargs.get("replace", False))
        self.client.rename(
            self._strip_protocol(path1), self._strip_protocol(path2), replace
        )

    def pipe_file(self, path, value, **kwargs):
        """Atomically publish ``value`` as an immutable artifact at ``path``."""
        path = self._strip_protocol(path)
        self.client.put_artifact(
            path,
            bytes(value),
            kwargs.get("producer", "nokv-fsspec"),
            kwargs.get("digest_uri", ""),
            kwargs.get("content_type", "application/octet-stream"),
            kwargs.get("manifest_id", ""),
            int(kwargs.get("mode", _DEFAULT_FILE_MODE)),
            int(kwargs.get("uid", 0)),
            int(kwargs.get("gid", 0)),
            True,  # replace: pipe_file overwrites by contract
        )

    # ------------------------------------------------------------------ snaps
    def snapshot(self, path) -> dict:
        return self.client.snapshot(self._strip_protocol(path))

    def stats(self) -> dict[str, int]:
        return dict(self.client.stats())

    # ------------------------------------------------------------------- open
    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        return NoKVBufferedFile(
            self,
            self._strip_protocol(path),
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )


class NoKVBufferedFile(AbstractBufferedFile):
    """An fsspec file handle over NoKV.

    Reads stream through the SDK batch range path. Writes accumulate in memory
    and publish a single immutable artifact generation on close — NoKV files are
    immutable per generation, so a partial multipart append has no meaning here;
    the whole object is committed atomically.
    """

    def __init__(self, fs, path, mode="rb", **kwargs):
        self._artifact_kwargs = {
            "producer": kwargs.pop("producer", "nokv-fsspec"),
            "digest_uri": kwargs.pop("digest_uri", ""),
            "content_type": kwargs.pop("content_type", "application/octet-stream"),
            "manifest_id": kwargs.pop("manifest_id", ""),
            "mode": int(kwargs.pop("mode", _DEFAULT_FILE_MODE)),
            "uid": int(kwargs.pop("uid", 0)),
            "gid": int(kwargs.pop("gid", 0)),
        }
        self._replace = bool(kwargs.pop("replace", True))
        super().__init__(fs, path, mode=mode, **kwargs)
        self._committed = bytearray()

    def _fetch_range(self, start, end):
        return self.fs.cat_file(self.path, start, end)

    def _initiate_upload(self):
        self._committed = bytearray()

    def _upload_chunk(self, final=False):
        self.buffer.seek(0)
        self._committed += self.buffer.read()
        if final:
            self.fs.client.put_artifact(
                self.path,
                bytes(self._committed),
                self._artifact_kwargs["producer"],
                self._artifact_kwargs["digest_uri"],
                self._artifact_kwargs["content_type"],
                self._artifact_kwargs["manifest_id"],
                self._artifact_kwargs["mode"],
                self._artifact_kwargs["uid"],
                self._artifact_kwargs["gid"],
                self._replace,
            )
        return True


def _info_from_attr(path: str, attr: dict) -> dict:
    kind = attr.get("type", "file")
    return {
        "name": path,
        "size": int(attr.get("size", 0)),
        "type": "directory" if kind == "directory" else "file",
        "nokv_type": kind,
        "generation": attr.get("generation"),
        "mode": attr.get("mode"),
        "mtime_ms": attr.get("mtime_ms"),
    }


def _normalize_range_requests(requests: Iterable[RangeRequest]):
    normalized = []
    for path, ranges, expected_generation, max_gap_bytes in requests:
        normalized.append(
            (
                path,
                [(int(offset), int(length)) for offset, length in ranges],
                expected_generation,
                max_gap_bytes,
            )
        )
    return normalized


def _packed_request_bytes(requests) -> int:
    return sum(length for _, ranges, _, _ in requests for _, length in ranges)
