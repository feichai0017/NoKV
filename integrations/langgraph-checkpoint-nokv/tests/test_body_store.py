from __future__ import annotations

from hashlib import sha256

import pytest

from langgraph.checkpoint.nokv import (
    BodyIntegrityError,
    BodyNotFoundError,
    BodyRef,
    CheckpointBodyStore,
    FileBodyStore,
    TypedBodyRef,
)


def test_body_ref_round_trips_as_compact_json():
    ref = BodyRef(kind="file-sha256", digest="a" * 64, size=12)

    encoded = ref.to_json_bytes()
    decoded = BodyRef.from_json_bytes(encoded)

    assert decoded == ref
    assert BodyRef.from_json_obj(ref.to_json_obj()) == ref
    assert b" " not in encoded
    assert len(encoded) < 128


def test_file_body_store_put_get_and_dedupe(tmp_path):
    store = FileBodyStore(tmp_path)
    data = b"checkpoint-body"

    ref1 = store.put(data)
    ref2 = store.put(data)

    assert ref1 == ref2
    assert ref1.kind == "file-sha256"
    assert ref1.digest == sha256(data).hexdigest()
    assert ref1.size == len(data)
    assert store.get(ref1) == data


def test_file_body_store_delete_is_idempotent(tmp_path):
    store = FileBodyStore(tmp_path)
    ref = store.put(b"payload")

    store.delete(ref)
    store.delete(ref)

    with pytest.raises(BodyNotFoundError):
        store.get(ref)


def test_file_body_store_detects_size_mismatch(tmp_path):
    store = FileBodyStore(tmp_path)
    ref = store.put(b"payload")
    bad_ref = BodyRef(kind=ref.kind, digest=ref.digest, size=ref.size + 1)

    with pytest.raises(BodyIntegrityError, match="size mismatch"):
        store.get(bad_ref)


def test_file_body_store_rejects_wrong_kind(tmp_path):
    store = FileBodyStore(tmp_path)
    ref = BodyRef(kind="s3-sha256", digest="a" * 64, size=1)

    with pytest.raises(ValueError, match="unsupported body kind"):
        store.get(ref)


def test_file_body_store_rejects_invalid_digest(tmp_path):
    store = FileBodyStore(tmp_path)
    ref = BodyRef(kind="file-sha256", digest="not-hex", size=1)

    with pytest.raises(ValueError, match="64 hex"):
        store.get(ref)


def test_file_body_store_supports_unsharded_layout(tmp_path):
    store = FileBodyStore(tmp_path, shard_prefix_len=0)
    ref = store.put(b"x")

    assert (tmp_path / ref.digest).exists()
    assert store.get(ref) == b"x"


def test_typed_body_ref_round_trips_json_obj():
    ref = BodyRef(kind="file-sha256", digest="a" * 64, size=5)
    body = TypedBodyRef(type="msgpack", body_ref=ref)

    assert TypedBodyRef.from_json_obj(body.to_json_obj()) == body


def test_checkpoint_body_store_puts_and_gets_local_file_body(tmp_path):
    store = CheckpointBodyStore.from_local_path(tmp_path)

    body = store.put_typed("msgpack", b"checkpoint-body")

    assert body.body_ref is not None
    assert body.body_ref.kind == FileBodyStore.kind
    assert store.get_typed(body) == ("msgpack", b"checkpoint-body")


def test_checkpoint_body_store_empty_marker_skips_local_file(tmp_path):
    store = CheckpointBodyStore.from_local_path(tmp_path)

    body = store.put_typed("empty", None)

    assert body == TypedBodyRef(type="empty")
    assert store.get_typed(body) == ("empty", None)
    assert list(tmp_path.iterdir()) == []


def test_checkpoint_body_store_rejects_missing_non_empty_body(tmp_path):
    store = CheckpointBodyStore.from_local_path(tmp_path)

    with pytest.raises(ValueError, match="non-empty body types require bytes"):
        store.put_typed("msgpack", None)

    with pytest.raises(ValueError, match="missing a body ref"):
        store.get_typed(TypedBodyRef(type="msgpack"))
