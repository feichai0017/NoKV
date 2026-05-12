from __future__ import annotations

import pytest

from langgraph.checkpoint.nokv import (
    BodyRef,
    ChannelBlobEntryAttrs,
    CheckpointEntryAttrs,
    DeltaWriteEntryAttrs,
    FsMetaLayout,
    HeadEntryAttrs,
    TypedBodyRef,
    WriteEntryAttrs,
    checkpoint_name,
    decode_component,
    decode_write_idx,
    delta_write_checkpoint_id_from_name,
    delta_write_name,
    delta_write_range_start_after,
    delta_write_range_stop_after,
    directory_attrs,
    encode_component,
    encode_write_idx,
    legacy_delta_write_name,
    thread_tombstone_attrs,
    version_name,
    write_name,
)


def test_encoded_components_are_valid_fsmeta_names_and_round_trip():
    values = ["", "thread/with/slash", ".", "..", "name\x00with-null", "child:abc"]

    for value in values:
        encoded = encode_component(value)
        assert encoded
        assert encoded not in {".", ".."}
        assert "/" not in encoded
        assert "\x00" not in encoded
        assert decode_component(encoded) == value


def test_layout_paths_are_stable_and_encoded():
    layout = FsMetaLayout()

    checkpoint = layout.checkpoint_entry("thread/1", "", "ckpt-1")
    blob = layout.blob_entry("thread/1", "", "messages", "0001.abc")
    write = layout.write_entry("thread/1", "", "ckpt-1", "task/1", -3)
    delta_write = layout.delta_write_entry(
        "thread/1", "", "messages", "ckpt-1", "task/1", -3
    )
    head = layout.head_entry("thread/1", "")

    assert checkpoint.parent == (
        "langgraph",
        "threads",
        encode_component("thread/1"),
        "namespaces",
        encode_component(""),
        "checkpoints",
    )
    assert checkpoint.name == checkpoint_name("ckpt-1")
    assert head.components[-2:] == ("heads", "latest")
    assert blob.name == version_name("0001.abc")
    assert write.name == write_name("task/1", -3)
    assert delta_write.parent[-2:] == ("delta_channels", encode_component("messages"))
    assert delta_write.name == delta_write_name("ckpt-1", "task/1", -3)


def test_write_idx_encoding_preserves_signed_values():
    for idx in [-4, -1, 0, 1, 42]:
        encoded = encode_write_idx(idx)
        assert encoded.startswith("i~")
        assert decode_write_idx(encoded) == idx

    with pytest.raises(ValueError, match="signed 64-bit"):
        encode_write_idx(1 << 63)


def test_delta_write_names_preserve_checkpoint_order_for_range_scans():
    older = delta_write_name("000001", "task", 0)
    middle = delta_write_name("000010", "task", 0)
    newer = delta_write_name("000100", "task", 0)

    assert older < middle < newer
    assert older.startswith("dw~")
    assert delta_write_checkpoint_id_from_name(middle) == "000010"
    assert legacy_delta_write_name("000010", "task", 0).startswith("dw~b64~")
    assert delta_write_range_start_after("000010") < middle
    assert delta_write_range_stop_after("000010") > middle


def test_checkpoint_attrs_round_trip_and_fit_opaque_limit():
    body = TypedBodyRef(
        type="msgpack",
        body_ref=BodyRef(kind="file-sha256", digest="a" * 64, size=99),
    )
    seed_body = TypedBodyRef(
        type="msgpack",
        body_ref=BodyRef(kind="file-sha256", digest="b" * 64, size=12),
    )
    attrs = CheckpointEntryAttrs(
        checkpoint_id="ckpt-2",
        parent_checkpoint_id="ckpt-1",
        body=body,
        seed_body_refs_by_channel={"messages": seed_body},
    )

    encoded = attrs.to_opaque_attrs()

    assert len(encoded) < 16 * 1024
    assert CheckpointEntryAttrs.from_opaque_attrs(encoded) == attrs


def test_legacy_checkpoint_attrs_keep_missing_delta_seed_index():
    body = TypedBodyRef(
        type="msgpack",
        body_ref=BodyRef(kind="file-sha256", digest="a" * 64, size=99),
    )
    attrs = CheckpointEntryAttrs(
        checkpoint_id="ckpt-2",
        parent_checkpoint_id="ckpt-1",
        body=body,
    )

    assert (
        CheckpointEntryAttrs.from_opaque_attrs(attrs.to_opaque_attrs())
        .seed_body_refs_by_channel
        is None
    )


def test_channel_blob_attrs_support_empty_marker():
    attrs = ChannelBlobEntryAttrs(
        channel="messages",
        version="0001.abc",
        body=TypedBodyRef(type="empty"),
    )

    assert ChannelBlobEntryAttrs.from_opaque_attrs(attrs.to_opaque_attrs()) == attrs


def test_head_attrs_round_trip_latest_checkpoint_id():
    attrs = HeadEntryAttrs(checkpoint_id="ckpt-2")

    assert HeadEntryAttrs.from_opaque_attrs(attrs.to_opaque_attrs()) == attrs


def test_write_attrs_preserve_task_path_negative_idx_and_channel():
    body = TypedBodyRef(
        type="json",
        body_ref=BodyRef(kind="file-sha256", digest="b" * 64, size=12),
    )
    attrs = WriteEntryAttrs(
        task_id="task-1",
        task_path="node:0",
        idx=-3,
        channel="__interrupt__",
        body=body,
    )

    assert WriteEntryAttrs.from_opaque_attrs(attrs.to_opaque_attrs()) == attrs


def test_delta_write_attrs_include_source_checkpoint():
    body = TypedBodyRef(
        type="json",
        body_ref=BodyRef(kind="file-sha256", digest="c" * 64, size=14),
    )
    attrs = DeltaWriteEntryAttrs(
        checkpoint_id="ckpt-1",
        task_id="task-1",
        task_path="node:0",
        idx=0,
        channel="messages",
        body=body,
    )

    assert DeltaWriteEntryAttrs.from_opaque_attrs(attrs.to_opaque_attrs()) == attrs


def test_directory_and_tombstone_attrs_validate_kind():
    assert b'"directory_kind":"checkpoints"' in directory_attrs("checkpoints")
    assert b'"deleted_at_unix_ns":123' in thread_tombstone_attrs(123)

    with pytest.raises(ValueError, match="expected fsmeta attrs kind"):
        CheckpointEntryAttrs.from_opaque_attrs(directory_attrs("checkpoints"))
