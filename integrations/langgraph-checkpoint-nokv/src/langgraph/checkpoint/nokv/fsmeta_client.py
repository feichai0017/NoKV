from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import grpc

from langgraph.checkpoint.nokv._proto import fsmeta_pb2, fsmeta_pb2_grpc


class InodeType(str, Enum):
    UNSPECIFIED = "unspecified"
    FILE = "file"
    DIRECTORY = "directory"


_TO_PROTO_INODE_TYPE = {
    InodeType.UNSPECIFIED: fsmeta_pb2.INODE_TYPE_UNSPECIFIED,
    InodeType.FILE: fsmeta_pb2.INODE_TYPE_FILE,
    InodeType.DIRECTORY: fsmeta_pb2.INODE_TYPE_DIRECTORY,
}

_FROM_PROTO_INODE_TYPE = {value: key for key, value in _TO_PROTO_INODE_TYPE.items()}


@dataclass(frozen=True)
class Dentry:
    parent: int
    name: str
    inode: int
    type: InodeType


@dataclass(frozen=True)
class Inode:
    inode: int
    type: InodeType
    size: int
    mode: int
    link_count: int
    created_unix_ns: int
    updated_unix_ns: int
    opaque_attrs: bytes


@dataclass(frozen=True)
class DentryAttrPair:
    dentry: Dentry
    inode: Inode


@dataclass(frozen=True)
class CreateResult:
    dentry: Dentry
    inode: Inode


@dataclass(frozen=True)
class SnapshotToken:
    mount: str
    root_inode: int
    read_version: int


class NoKVFsMetaClient:
    """Thin Python wrapper over the fsmeta gRPC service.

    This layer intentionally mirrors fsmeta RPCs. It does not encode LangGraph
    checkpoint semantics; `NoKVCheckpointSaver` is built above it.
    """

    def __init__(
        self,
        target: str | None = None,
        *,
        channel: grpc.Channel | None = None,
        stub: Any | None = None,
    ) -> None:
        if stub is not None:
            self._channel = None
            self._stub = stub
            return
        if channel is None:
            if target is None:
                raise ValueError("target or channel is required")
            channel = grpc.insecure_channel(target)
            self._channel = channel
        else:
            self._channel = None
        self._stub = fsmeta_pb2_grpc.FSMetadataStub(channel)

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()

    def wait_ready(self, timeout: float | None = None) -> None:
        if self._channel is None:
            return
        grpc.channel_ready_future(self._channel).result(timeout=timeout)

    def create(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        inode_type: InodeType,
        size: int = 0,
        mode: int = 0,
        created_unix_ns: int = 0,
        updated_unix_ns: int = 0,
        opaque_attrs: bytes = b"",
        timeout: float | None = None,
    ) -> CreateResult:
        req = fsmeta_pb2.CreateRequest(
            mount=mount,
            parent=parent,
            name=name,
            attrs=fsmeta_pb2.CreateInodeAttrs(
                type=_to_proto_inode_type(inode_type),
                size=size,
                mode=mode,
                created_unix_ns=created_unix_ns,
                updated_unix_ns=updated_unix_ns,
                opaque_attrs=opaque_attrs,
            ),
        )
        return _create_result_from_proto(self._stub.Create(req, timeout=timeout))

    def update_inode(
        self,
        *,
        mount: str,
        parent: int,
        inode: int,
        name: str,
        size: int | None = None,
        mode: int | None = None,
        updated_unix_ns: int | None = None,
        opaque_attrs: bytes | None = None,
        timeout: float | None = None,
    ) -> Inode:
        req = fsmeta_pb2.UpdateInodeRequest(
            mount=mount,
            parent=parent,
            inode=inode,
            name=name,
        )
        if size is not None:
            req.set_size = True
            req.size = size
        if mode is not None:
            req.set_mode = True
            req.mode = mode
        if updated_unix_ns is not None:
            req.set_updated_unix_ns = True
            req.updated_unix_ns = updated_unix_ns
        if opaque_attrs is not None:
            req.set_opaque_attrs = True
            req.opaque_attrs = opaque_attrs
        return _inode_from_proto(self._stub.UpdateInode(req, timeout=timeout).inode)

    def lookup(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        timeout: float | None = None,
    ) -> Dentry:
        req = fsmeta_pb2.LookupRequest(mount=mount, parent=parent, name=name)
        return _dentry_from_proto(self._stub.Lookup(req, timeout=timeout).dentry)

    def lookup_plus(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        timeout: float | None = None,
    ) -> DentryAttrPair:
        req = fsmeta_pb2.LookupRequest(mount=mount, parent=parent, name=name)
        return _pair_from_proto(self._stub.LookupPlus(req, timeout=timeout).entry)

    def read_dir_plus(
        self,
        *,
        mount: str,
        parent: int,
        start_after: str = "",
        limit: int = 0,
        snapshot_version: int = 0,
        timeout: float | None = None,
    ) -> list[DentryAttrPair]:
        req = fsmeta_pb2.ReadDirRequest(
            mount=mount,
            parent=parent,
            start_after=start_after,
            limit=limit,
            snapshot_version=snapshot_version,
        )
        resp = self._stub.ReadDirPlus(req, timeout=timeout)
        return [_pair_from_proto(pair) for pair in resp.entries]

    def rename(
        self,
        *,
        mount: str,
        from_parent: int,
        from_name: str,
        to_parent: int,
        to_name: str,
        timeout: float | None = None,
    ) -> None:
        req = fsmeta_pb2.RenameRequest(
            mount=mount,
            from_parent=from_parent,
            from_name=from_name,
            to_parent=to_parent,
            to_name=to_name,
        )
        self._stub.Rename(req, timeout=timeout)

    def unlink(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        timeout: float | None = None,
    ) -> None:
        req = fsmeta_pb2.UnlinkRequest(mount=mount, parent=parent, name=name)
        self._stub.Unlink(req, timeout=timeout)

    def snapshot_subtree(
        self,
        *,
        mount: str,
        root_inode: int,
        timeout: float | None = None,
    ) -> SnapshotToken:
        req = fsmeta_pb2.SnapshotSubtreeRequest(mount=mount, root_inode=root_inode)
        resp = self._stub.SnapshotSubtree(req, timeout=timeout)
        return SnapshotToken(
            mount=resp.mount,
            root_inode=resp.root_inode,
            read_version=resp.read_version,
        )


def _to_proto_inode_type(value: InodeType) -> int:
    try:
        return _TO_PROTO_INODE_TYPE[value]
    except KeyError as exc:
        raise ValueError(f"unsupported inode type: {value!r}") from exc


def _from_proto_inode_type(value: int) -> InodeType:
    return _FROM_PROTO_INODE_TYPE.get(value, InodeType.UNSPECIFIED)


def _dentry_from_proto(value: fsmeta_pb2.DentryRecord) -> Dentry:
    return Dentry(
        parent=value.parent,
        name=value.name,
        inode=value.inode,
        type=_from_proto_inode_type(value.type),
    )


def _inode_from_proto(value: fsmeta_pb2.InodeRecord) -> Inode:
    return Inode(
        inode=value.inode,
        type=_from_proto_inode_type(value.type),
        size=value.size,
        mode=value.mode,
        link_count=value.link_count,
        created_unix_ns=value.created_unix_ns,
        updated_unix_ns=value.updated_unix_ns,
        opaque_attrs=value.opaque_attrs,
    )


def _pair_from_proto(value: fsmeta_pb2.DentryAttrPair) -> DentryAttrPair:
    return DentryAttrPair(
        dentry=_dentry_from_proto(value.dentry),
        inode=_inode_from_proto(value.inode),
    )


def _create_result_from_proto(value: fsmeta_pb2.CreateResponse) -> CreateResult:
    return CreateResult(
        dentry=_dentry_from_proto(value.dentry),
        inode=_inode_from_proto(value.inode),
    )
