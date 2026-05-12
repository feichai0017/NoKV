from __future__ import annotations

import pytest

from langgraph.checkpoint.nokv import InodeType, NoKVFsMetaClient
from langgraph.checkpoint.nokv._proto import fsmeta_pb2


class UnaryCall:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def __call__(self, request, timeout=None):
        self.calls.append((request, timeout))
        return self.response


class FakeStub:
    def __init__(self):
        self.Create = UnaryCall(
            fsmeta_pb2.CreateResponse(
                dentry=fsmeta_pb2.DentryRecord(
                    parent=1,
                    name="entry",
                    inode=2,
                    type=fsmeta_pb2.INODE_TYPE_FILE,
                ),
                inode=fsmeta_pb2.InodeRecord(
                    inode=2,
                    type=fsmeta_pb2.INODE_TYPE_FILE,
                    size=12,
                    mode=0o644,
                    link_count=1,
                    created_unix_ns=100,
                    updated_unix_ns=200,
                    opaque_attrs=b"ref",
                ),
            )
        )
        self.UpdateInode = UnaryCall(
            fsmeta_pb2.UpdateInodeResponse(
                inode=fsmeta_pb2.InodeRecord(
                    inode=2,
                    type=fsmeta_pb2.INODE_TYPE_FILE,
                    size=13,
                    mode=0o600,
                    link_count=1,
                    created_unix_ns=100,
                    updated_unix_ns=300,
                    opaque_attrs=b"updated",
                )
            )
        )
        self.Lookup = UnaryCall(
            fsmeta_pb2.LookupResponse(
                dentry=fsmeta_pb2.DentryRecord(
                    parent=1,
                    name="entry",
                    inode=2,
                    type=fsmeta_pb2.INODE_TYPE_FILE,
                )
            )
        )
        self.LookupPlus = UnaryCall(
            fsmeta_pb2.LookupPlusResponse(
                entry=fsmeta_pb2.DentryAttrPair(
                    dentry=fsmeta_pb2.DentryRecord(
                        parent=1,
                        name="entry",
                        inode=2,
                        type=fsmeta_pb2.INODE_TYPE_FILE,
                    ),
                    inode=fsmeta_pb2.InodeRecord(
                        inode=2,
                        type=fsmeta_pb2.INODE_TYPE_FILE,
                        mode=0o644,
                        link_count=1,
                        opaque_attrs=b"file",
                    ),
                )
            )
        )
        self.ReadDirPlus = UnaryCall(
            fsmeta_pb2.ReadDirPlusResponse(
                entries=[
                    fsmeta_pb2.DentryAttrPair(
                        dentry=fsmeta_pb2.DentryRecord(
                            parent=1,
                            name="child",
                            inode=3,
                            type=fsmeta_pb2.INODE_TYPE_DIRECTORY,
                        ),
                        inode=fsmeta_pb2.InodeRecord(
                            inode=3,
                            type=fsmeta_pb2.INODE_TYPE_DIRECTORY,
                            mode=0o755,
                            link_count=1,
                            opaque_attrs=b"dir",
                        ),
                    )
                ]
            )
        )
        self.Rename = UnaryCall(fsmeta_pb2.RenameResponse())
        self.Unlink = UnaryCall(fsmeta_pb2.UnlinkResponse())
        self.SnapshotSubtree = UnaryCall(
            fsmeta_pb2.SnapshotSubtreeResponse(
                mount="vol",
                root_inode=1,
                read_version=42,
            )
        )


def test_create_maps_request_and_response():
    stub = FakeStub()
    client = NoKVFsMetaClient(stub=stub)

    result = client.create(
        mount="vol",
        parent=1,
        name="entry",
        inode_type=InodeType.FILE,
        size=12,
        mode=0o644,
        created_unix_ns=100,
        updated_unix_ns=200,
        opaque_attrs=b"ref",
        timeout=3.0,
    )

    req, timeout = stub.Create.calls[0]
    assert timeout == 3.0
    assert req.mount == "vol"
    assert req.parent == 1
    assert req.name == "entry"
    assert req.attrs.type == fsmeta_pb2.INODE_TYPE_FILE
    assert req.attrs.opaque_attrs == b"ref"
    assert result.dentry.name == "entry"
    assert result.inode.opaque_attrs == b"ref"


def test_update_inode_sets_only_provided_fields():
    stub = FakeStub()
    client = NoKVFsMetaClient(stub=stub)

    inode = client.update_inode(
        mount="vol",
        parent=1,
        inode=2,
        name="entry",
        size=13,
        opaque_attrs=b"updated",
    )

    req, _ = stub.UpdateInode.calls[0]
    assert req.set_size is True
    assert req.size == 13
    assert req.set_mode is False
    assert req.set_updated_unix_ns is False
    assert req.set_opaque_attrs is True
    assert req.opaque_attrs == b"updated"
    assert inode.size == 13
    assert inode.opaque_attrs == b"updated"


def test_lookup_and_read_dir_plus_map_responses():
    stub = FakeStub()
    client = NoKVFsMetaClient(stub=stub)

    dentry = client.lookup(mount="vol", parent=1, name="entry")
    pair = client.lookup_plus(mount="vol", parent=1, name="entry")
    pairs = client.read_dir_plus(
        mount="vol",
        parent=1,
        start_after="a",
        limit=10,
        snapshot_version=99,
    )

    lookup_req, _ = stub.Lookup.calls[0]
    lookup_plus_req, _ = stub.LookupPlus.calls[0]
    read_req, _ = stub.ReadDirPlus.calls[0]
    assert lookup_req.name == "entry"
    assert dentry.inode == 2
    assert lookup_plus_req.name == "entry"
    assert pair.inode.opaque_attrs == b"file"
    assert read_req.start_after == "a"
    assert read_req.limit == 10
    assert read_req.snapshot_version == 99
    assert pairs[0].dentry.type is InodeType.DIRECTORY
    assert pairs[0].inode.opaque_attrs == b"dir"


def test_rename_unlink_and_snapshot_subtree_map_requests():
    stub = FakeStub()
    client = NoKVFsMetaClient(stub=stub)

    client.rename(
        mount="vol",
        from_parent=1,
        from_name="old",
        to_parent=2,
        to_name="new",
    )
    client.unlink(mount="vol", parent=2, name="old")
    token = client.snapshot_subtree(mount="vol", root_inode=1)

    rename_req, _ = stub.Rename.calls[0]
    unlink_req, _ = stub.Unlink.calls[0]
    snapshot_req, _ = stub.SnapshotSubtree.calls[0]
    assert rename_req.from_name == "old"
    assert rename_req.to_name == "new"
    assert unlink_req.name == "old"
    assert snapshot_req.root_inode == 1
    assert token.read_version == 42


def test_target_or_channel_is_required_without_stub():
    with pytest.raises(ValueError, match="target or channel is required"):
        NoKVFsMetaClient()
