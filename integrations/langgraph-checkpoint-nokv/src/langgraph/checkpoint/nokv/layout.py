from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

from langgraph.checkpoint.nokv.body_store import TypedBodyRef

LAYOUT_VERSION = 1
OPAQUE_ATTRS_MAX_BYTES = 16 * 1024

_COMPONENT_PREFIX = "b64~"
_DELTA_WRITE_ORDERED_PREFIX = "dw~2~"
_WRITE_IDX_OFFSET = 1 << 63
_WRITE_IDX_WIDTH = 20


class LayoutKind(str, Enum):
    CHECKPOINT = "checkpoint"
    CHANNEL_BLOB = "channel_blob"
    DELTA_WRITE = "delta_write"
    HEAD = "head"
    WRITE = "write"
    DIRECTORY = "directory"
    THREAD_TOMBSTONE = "thread_tombstone"


@dataclass(frozen=True)
class EntryPath:
    parent: tuple[str, ...]
    name: str

    @property
    def components(self) -> tuple[str, ...]:
        return (*self.parent, self.name)


@dataclass(frozen=True)
class FsMetaLayout:
    """Stable fsmeta namespace layout for LangGraph checkpoint records."""

    root: tuple[str, ...] = ("langgraph",)

    def thread_dir(self, thread_id: str) -> tuple[str, ...]:
        return (*self.root, "threads", encode_component(thread_id))

    def namespace_dir(self, thread_id: str, checkpoint_ns: str) -> tuple[str, ...]:
        return (
            *self.thread_dir(thread_id),
            "namespaces",
            encode_component(checkpoint_ns),
        )

    def checkpoints_dir(self, thread_id: str, checkpoint_ns: str) -> tuple[str, ...]:
        return (*self.namespace_dir(thread_id, checkpoint_ns), "checkpoints")

    def head_entry(self, thread_id: str, checkpoint_ns: str) -> EntryPath:
        return EntryPath(
            parent=(*self.namespace_dir(thread_id, checkpoint_ns), "heads"),
            name="latest",
        )

    def checkpoint_entry(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> EntryPath:
        return EntryPath(
            parent=self.checkpoints_dir(thread_id, checkpoint_ns),
            name=checkpoint_name(checkpoint_id),
        )

    def blobs_dir(
        self, thread_id: str, checkpoint_ns: str, channel: str
    ) -> tuple[str, ...]:
        return (
            *self.namespace_dir(thread_id, checkpoint_ns),
            "blobs",
            encode_component(channel),
        )

    def blob_entry(
        self, thread_id: str, checkpoint_ns: str, channel: str, version: object
    ) -> EntryPath:
        return EntryPath(
            parent=self.blobs_dir(thread_id, checkpoint_ns, channel),
            name=version_name(version),
        )

    def writes_dir(self, thread_id: str, checkpoint_ns: str) -> tuple[str, ...]:
        return (*self.namespace_dir(thread_id, checkpoint_ns), "writes")

    def checkpoint_writes_dir(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> tuple[str, ...]:
        return (*self.writes_dir(thread_id, checkpoint_ns), checkpoint_name(checkpoint_id))

    def write_entry(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
    ) -> EntryPath:
        return EntryPath(
            parent=self.checkpoint_writes_dir(thread_id, checkpoint_ns, checkpoint_id),
            name=write_name(task_id, idx),
        )

    def delta_channels_dir(
        self, thread_id: str, checkpoint_ns: str
    ) -> tuple[str, ...]:
        return (*self.namespace_dir(thread_id, checkpoint_ns), "delta_channels")

    def delta_channel_dir(
        self, thread_id: str, checkpoint_ns: str, channel: str
    ) -> tuple[str, ...]:
        return (*self.delta_channels_dir(thread_id, checkpoint_ns), encode_component(channel))

    def delta_write_entry(
        self,
        thread_id: str,
        checkpoint_ns: str,
        channel: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
    ) -> EntryPath:
        return EntryPath(
            parent=self.delta_channel_dir(thread_id, checkpoint_ns, channel),
            name=delta_write_name(checkpoint_id, task_id, idx),
        )

    def thread_tombstone_entry(self, thread_id: str) -> EntryPath:
        return EntryPath(parent=self.thread_dir(thread_id), name="thread-tombstone")


@dataclass(frozen=True)
class CheckpointEntryAttrs:
    checkpoint_id: str
    parent_checkpoint_id: str | None
    body: TypedBodyRef
    seed_body_refs_by_channel: dict[str, TypedBodyRef] | None = None

    def to_opaque_attrs(self) -> bytes:
        values: dict[str, Any] = {
            "checkpoint_id": self.checkpoint_id,
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "body": self.body.to_json_obj(),
        }
        if self.seed_body_refs_by_channel is not None:
            values["seed_body_refs_by_channel"] = {
                channel: body.to_json_obj()
                for channel, body in self.seed_body_refs_by_channel.items()
            }
        return _pack_attrs(
            LayoutKind.CHECKPOINT,
            **values,
        )

    @classmethod
    def from_opaque_attrs(cls, data: bytes) -> CheckpointEntryAttrs:
        raw = _unpack_attrs(data, LayoutKind.CHECKPOINT)
        seed_body_refs = raw.get("seed_body_refs_by_channel")
        if seed_body_refs is not None:
            if not isinstance(seed_body_refs, dict):
                raise ValueError("checkpoint seed body refs must be a JSON object")
            seed_body_refs = {
                str(channel): TypedBodyRef.from_json_obj(body)
                for channel, body in seed_body_refs.items()
            }
        return cls(
            checkpoint_id=str(raw["checkpoint_id"]),
            parent_checkpoint_id=(
                str(raw["parent_checkpoint_id"])
                if raw.get("parent_checkpoint_id") is not None
                else None
            ),
            body=TypedBodyRef.from_json_obj(raw["body"]),
            seed_body_refs_by_channel=seed_body_refs,
        )


@dataclass(frozen=True)
class ChannelBlobEntryAttrs:
    channel: str
    version: str
    body: TypedBodyRef

    def to_opaque_attrs(self) -> bytes:
        return _pack_attrs(
            LayoutKind.CHANNEL_BLOB,
            channel=self.channel,
            version=self.version,
            body=self.body.to_json_obj(),
        )

    @classmethod
    def from_opaque_attrs(cls, data: bytes) -> ChannelBlobEntryAttrs:
        raw = _unpack_attrs(data, LayoutKind.CHANNEL_BLOB)
        return cls(
            channel=str(raw["channel"]),
            version=str(raw["version"]),
            body=TypedBodyRef.from_json_obj(raw["body"]),
        )


@dataclass(frozen=True)
class HeadEntryAttrs:
    checkpoint_id: str

    def to_opaque_attrs(self) -> bytes:
        return _pack_attrs(LayoutKind.HEAD, checkpoint_id=self.checkpoint_id)

    @classmethod
    def from_opaque_attrs(cls, data: bytes) -> HeadEntryAttrs:
        raw = _unpack_attrs(data, LayoutKind.HEAD)
        return cls(checkpoint_id=str(raw["checkpoint_id"]))


@dataclass(frozen=True)
class WriteEntryAttrs:
    task_id: str
    task_path: str
    idx: int
    channel: str
    body: TypedBodyRef

    def to_opaque_attrs(self) -> bytes:
        return _pack_attrs(
            LayoutKind.WRITE,
            task_id=self.task_id,
            task_path=self.task_path,
            idx=self.idx,
            channel=self.channel,
            body=self.body.to_json_obj(),
        )

    @classmethod
    def from_opaque_attrs(cls, data: bytes) -> WriteEntryAttrs:
        raw = _unpack_attrs(data, LayoutKind.WRITE)
        return cls(
            task_id=str(raw["task_id"]),
            task_path=str(raw.get("task_path", "")),
            idx=int(raw["idx"]),
            channel=str(raw["channel"]),
            body=TypedBodyRef.from_json_obj(raw["body"]),
        )


@dataclass(frozen=True)
class DeltaWriteEntryAttrs:
    checkpoint_id: str
    task_id: str
    task_path: str
    idx: int
    channel: str
    body: TypedBodyRef

    def to_opaque_attrs(self) -> bytes:
        return _pack_attrs(
            LayoutKind.DELTA_WRITE,
            checkpoint_id=self.checkpoint_id,
            task_id=self.task_id,
            task_path=self.task_path,
            idx=self.idx,
            channel=self.channel,
            body=self.body.to_json_obj(),
        )

    @classmethod
    def from_opaque_attrs(cls, data: bytes) -> DeltaWriteEntryAttrs:
        raw = _unpack_attrs(data, LayoutKind.DELTA_WRITE)
        return cls(
            checkpoint_id=str(raw["checkpoint_id"]),
            task_id=str(raw["task_id"]),
            task_path=str(raw.get("task_path", "")),
            idx=int(raw["idx"]),
            channel=str(raw["channel"]),
            body=TypedBodyRef.from_json_obj(raw["body"]),
        )


def encode_component(value: str) -> str:
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
    return _COMPONENT_PREFIX + encoded.rstrip("=")


def decode_component(name: str) -> str:
    if not name.startswith(_COMPONENT_PREFIX):
        raise ValueError(f"encoded component must start with {_COMPONENT_PREFIX!r}")
    encoded = name[len(_COMPONENT_PREFIX) :]
    padding = "=" * (-len(encoded) % 4)
    return base64.urlsafe_b64decode((encoded + padding).encode("ascii")).decode("utf-8")


def checkpoint_name(checkpoint_id: str) -> str:
    return "c~" + encode_component(checkpoint_id)


def version_name(version: object) -> str:
    return "v~" + encode_component(str(version))


def write_name(task_id: str, idx: int) -> str:
    return "w~" + encode_component(task_id) + "~" + encode_write_idx(idx)


def delta_write_name(checkpoint_id: str, task_id: str, idx: int) -> str:
    return (
        _DELTA_WRITE_ORDERED_PREFIX
        + _encode_ordered_component(checkpoint_id)
        + "~"
        + encode_component(task_id)
        + "~"
        + encode_write_idx(idx)
    )


def legacy_delta_write_name(checkpoint_id: str, task_id: str, idx: int) -> str:
    return (
        "dw~"
        + encode_component(checkpoint_id)
        + "~"
        + encode_component(task_id)
        + "~"
        + encode_write_idx(idx)
    )


def delta_write_checkpoint_id_from_name(name: str) -> str | None:
    if not name.startswith(_DELTA_WRITE_ORDERED_PREFIX):
        return None
    remainder = name[len(_DELTA_WRITE_ORDERED_PREFIX) :]
    checkpoint_hex, _, _ = remainder.partition("~")
    if checkpoint_hex == "":
        return ""
    try:
        return _decode_ordered_component(checkpoint_hex)
    except ValueError:
        return None


def delta_write_range_start_after(checkpoint_id: str) -> str:
    return _DELTA_WRITE_ORDERED_PREFIX + _encode_ordered_component(checkpoint_id) + "~"


def delta_write_range_stop_after(checkpoint_id: str) -> str:
    return delta_write_range_start_after(checkpoint_id) + "\uffff"


def encode_write_idx(idx: int) -> str:
    shifted = idx + _WRITE_IDX_OFFSET
    if shifted < 0 or shifted >= (1 << 64):
        raise ValueError("write idx is outside signed 64-bit range")
    return f"i~{shifted:0{_WRITE_IDX_WIDTH}d}"


def decode_write_idx(name: str) -> int:
    if not name.startswith("i~"):
        raise ValueError("write idx component must start with 'i~'")
    return int(name[2:]) - _WRITE_IDX_OFFSET


def _encode_ordered_component(value: str) -> str:
    return value.encode("utf-8").hex()


def _decode_ordered_component(value: str) -> str:
    try:
        return bytes.fromhex(value).decode("utf-8")
    except ValueError as exc:
        raise ValueError("ordered component must be UTF-8 hex") from exc


def directory_attrs(kind: str = "generic") -> bytes:
    return _pack_attrs(LayoutKind.DIRECTORY, directory_kind=kind)


def thread_tombstone_attrs(deleted_at_unix_ns: int) -> bytes:
    return _pack_attrs(
        LayoutKind.THREAD_TOMBSTONE,
        deleted_at_unix_ns=deleted_at_unix_ns,
    )


def _pack_attrs(kind: LayoutKind, **values: Any) -> bytes:
    raw: dict[str, Any] = {"v": LAYOUT_VERSION, "kind": kind.value}
    raw.update(values)
    encoded = json.dumps(raw, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > OPAQUE_ATTRS_MAX_BYTES:
        raise ValueError("fsmeta opaque_attrs exceeds 16 KiB")
    return encoded


def _unpack_attrs(data: bytes, expected_kind: LayoutKind) -> dict[str, Any]:
    raw = json.loads(data.decode("utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("fsmeta attrs must be a JSON object")
    if raw.get("v") != LAYOUT_VERSION:
        raise ValueError(f"unsupported fsmeta layout version: {raw.get('v')!r}")
    if raw.get("kind") != expected_kind.value:
        raise ValueError(
            f"expected fsmeta attrs kind {expected_kind.value!r}, got {raw.get('kind')!r}"
        )
    return raw
