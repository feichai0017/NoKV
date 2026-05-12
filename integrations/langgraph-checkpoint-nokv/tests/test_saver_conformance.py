from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any
from uuid import uuid4

import pytest
from fake_fsmeta import FakeFsMetaClient
from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import BaseCheckpointSaver, Checkpoint
from langgraph.checkpoint.base.id import uuid6
from langgraph.checkpoint.conformance.initializer import checkpointer_test
from langgraph.checkpoint.conformance.test_utils import generate_metadata
from langgraph.checkpoint.conformance.validate import validate
from langgraph.checkpoint.nokv import (
    CheckpointBodyStore,
    CheckpointEntryAttrs,
    NoKVCheckpointSaver,
    delta_write_checkpoint_id_from_name,
)
from langgraph.checkpoint.serde.types import _DeltaSnapshot

CONFORMANCE_CAPABILITIES = {
    "put",
    "get_tuple",
    "put_writes",
    "list",
    "delete_thread",
}


def make_saver(tmp_path):
    return NoKVCheckpointSaver(
        fsmeta_client=FakeFsMetaClient(),
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )


class CountingFsMetaClient(FakeFsMetaClient):
    def __init__(self) -> None:
        super().__init__()
        self.create_count = 0
        self.create_names: list[str] = []
        self.update_inode_count = 0
        self.update_inode_names: list[str] = []
        self.lookup_plus_count = 0
        self.lookup_plus_names: list[str] = []
        self.batch_lookup_plus_count = 0
        self.batch_lookup_plus_names: list[list[str]] = []
        self.read_dir_plus_count = 0
        self.read_dir_plus_pages: list[list[str]] = []

    def create(self, **kwargs):
        self.create_count += 1
        self.create_names.append(kwargs["name"])
        return super().create(**kwargs)

    def update_inode(self, **kwargs):
        self.update_inode_count += 1
        self.update_inode_names.append(kwargs["name"])
        return super().update_inode(**kwargs)

    def lookup_plus(self, **kwargs):
        self.lookup_plus_count += 1
        self.lookup_plus_names.append(kwargs["name"])
        return super().lookup_plus(**kwargs)

    def batch_lookup_plus(self, **kwargs):
        self.batch_lookup_plus_count += 1
        self.batch_lookup_plus_names.append(
            [lookup.name for lookup in kwargs["lookups"]]
        )
        return super().batch_lookup_plus(**kwargs)

    def read_dir_plus(self, **kwargs):
        self.read_dir_plus_count += 1
        page = super().read_dir_plus(**kwargs)
        self.read_dir_plus_pages.append([pair.dentry.name for pair in page])
        return page


class StaleLookupAfterCreateConflictFsMetaClient(FakeFsMetaClient):
    def __init__(self, hidden_name: str) -> None:
        super().__init__()
        self.hidden_name = hidden_name

    def lookup(self, **kwargs):
        if kwargs["name"] == self.hidden_name:
            raise FileNotFoundError(kwargs["name"])
        return super().lookup(**kwargs)

    def lookup_plus(self, **kwargs):
        if kwargs["name"] == self.hidden_name:
            raise FileNotFoundError(kwargs["name"])
        return super().lookup_plus(**kwargs)

    def create(self, **kwargs):
        if kwargs["name"] == self.hidden_name:
            try:
                super().create(**kwargs)
            except FileExistsError:
                pass
            raise FileExistsError(kwargs["name"])
        return super().create(**kwargs)


class UnavailableBatchLookupFsMetaClient(CountingFsMetaClient):
    def batch_lookup_plus(self, **kwargs):
        self.batch_lookup_plus_count += 1
        raise AttributeError("BatchLookupPlus unavailable")


def test_nokv_saver_registered_conformance_capabilities(tmp_path):
    counter = 0

    @checkpointer_test(name="NoKVCheckpointSaver")
    async def registered_nokv_saver():
        nonlocal counter
        counter += 1
        yield make_saver(tmp_path / f"case-{counter}")

    report = asyncio.run(
        validate(registered_nokv_saver, capabilities=CONFORMANCE_CAPABILITIES)
    )

    assert report.passed_all_base()


def test_single_child_reads_use_lookup_plus_not_readdirplus(tmp_path):
    client = CountingFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )
    entry = saver.layout.head_entry("thread-lookup-plus", "")

    saver._put_file(entry, b"payload")
    client.lookup_plus_count = 0
    client.read_dir_plus_count = 0

    found = saver._read_file(entry)

    assert found is not None
    assert found.inode.opaque_attrs == b"payload"
    assert client.lookup_plus_count == 1
    assert client.read_dir_plus_count == 0


def test_put_immutable_entries_use_create_first(tmp_path):
    client = CountingFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )
    checkpoint = Checkpoint(
        v=1,
        id="checkpoint-create-first",
        ts="",
        channel_values={"a": "A", "b": "B"},
        channel_versions={"a": 1, "b": 1},
        versions_seen={},
        updated_channels=None,
    )

    saver.put(
        {"configurable": {"thread_id": "thread-create-first", "checkpoint_ns": ""}},
        checkpoint,
        generate_metadata(step=0),
        {"a": 1, "b": 1},
    )

    assert any(name.startswith("c~") for name in client.create_names)
    assert sum(name.startswith("v~") for name in client.create_names) == 2
    assert not any(name.startswith("c~") for name in client.lookup_plus_names)
    assert not any(name.startswith("v~") for name in client.lookup_plus_names)
    assert "latest" in client.lookup_plus_names


def test_immutable_file_create_conflict_rejects_different_attrs(tmp_path):
    saver = make_saver(tmp_path)
    entry = saver.layout.checkpoint_entry("thread-immutable-conflict", "", "same-id")

    saver._create_immutable_file(entry, b"first")

    with pytest.raises(FileExistsError):
        saver._create_immutable_file(entry, b"second")

    found = saver._read_file(entry)
    assert found is not None
    assert found.inode.opaque_attrs == b"first"


def test_directory_create_conflict_can_recover_through_readdirplus(tmp_path):
    client = StaleLookupAfterCreateConflictFsMetaClient("checkpoints")
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )

    inode = saver._ensure_dir(
        saver.layout.checkpoints_dir("thread-stale-create-conflict", "")
    )

    assert inode > 1


def test_immutable_metadata_cache_hits_after_put(tmp_path):
    client = CountingFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )
    checkpoint = Checkpoint(
        v=1,
        id="checkpoint-cache-hit",
        ts="",
        channel_values={"ch": "cached-value"},
        channel_versions={"ch": 1},
        versions_seen={},
        updated_channels=None,
    )

    saver.put(
        {"configurable": {"thread_id": "thread-cache-hit", "checkpoint_ns": ""}},
        checkpoint,
        generate_metadata(step=0),
        {"ch": 1},
    )
    client.lookup_plus_names.clear()

    checkpoint_attrs = saver._load_checkpoint_attrs(
        "thread-cache-hit", "", "checkpoint-cache-hit"
    )
    found, value = saver._load_channel_value("thread-cache-hit", "", "ch", 1)

    assert checkpoint_attrs is not None
    assert found is True
    assert value == "cached-value"
    assert client.lookup_plus_names == []


def test_latest_checkpoint_read_batches_tombstone_and_head(tmp_path):
    client = CountingFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )
    asyncio.run(
        _put_delta_checkpoint(
            saver,
            thread_id="thread-batch-latest",
            parent_config=None,
            checkpoint_id="root",
        )
    )
    client.lookup_plus_names.clear()
    client.batch_lookup_plus_count = 0
    client.batch_lookup_plus_names.clear()

    tup = saver.get_tuple(
        {"configurable": {"thread_id": "thread-batch-latest", "checkpoint_ns": ""}}
    )

    assert tup is not None
    assert tup.config["configurable"]["checkpoint_id"] == "root"
    assert client.batch_lookup_plus_count == 1
    assert client.batch_lookup_plus_names == [["thread-tombstone", "latest"]]
    assert "thread-tombstone" not in client.lookup_plus_names
    assert "latest" not in client.lookup_plus_names


def test_latest_checkpoint_read_falls_back_when_batch_lookup_is_unavailable(tmp_path):
    client = UnavailableBatchLookupFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )
    asyncio.run(
        _put_delta_checkpoint(
            saver,
            thread_id="thread-batch-fallback",
            parent_config=None,
            checkpoint_id="root",
        )
    )
    client.lookup_plus_names.clear()
    client.batch_lookup_plus_count = 0

    tup = saver.get_tuple(
        {"configurable": {"thread_id": "thread-batch-fallback", "checkpoint_ns": ""}}
    )

    assert tup is not None
    assert client.batch_lookup_plus_count == 1
    assert "thread-tombstone" in client.lookup_plus_names
    assert "latest" in client.lookup_plus_names


def test_checkpoint_directory_page_warms_immutable_metadata_cache(tmp_path):
    client = CountingFsMetaClient()
    body_root = tmp_path / "bodies"
    writer = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(body_root),
    )
    awaitable_config = _put_delta_checkpoint(
        writer,
        thread_id="thread-dirpage-cache",
        parent_config=None,
        checkpoint_id="root",
    )
    asyncio.run(awaitable_config)
    reader = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(body_root),
    )

    attrs_by_id = reader._load_checkpoint_attrs_map("thread-dirpage-cache", "")
    client.lookup_plus_names.clear()

    attrs = reader._load_checkpoint_attrs("thread-dirpage-cache", "", "root")

    assert attrs_by_id is not None
    assert "root" in attrs_by_id
    assert attrs is not None
    assert attrs.checkpoint_id == "root"
    assert client.lookup_plus_names == []


def test_immutable_metadata_cache_is_bounded_lru(tmp_path):
    client = CountingFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
        metadata_cache_max_entries=1,
    )
    first = asyncio.run(
        _put_delta_checkpoint(
            saver,
            thread_id="thread-cache-bound",
            parent_config=None,
            checkpoint_id="one",
        )
    )
    asyncio.run(
        _put_delta_checkpoint(
            saver,
            thread_id="thread-cache-bound",
            parent_config=first,
            checkpoint_id="two",
        )
    )
    client.lookup_plus_names.clear()

    assert saver._load_checkpoint_attrs("thread-cache-bound", "", "two") is not None
    assert client.lookup_plus_names == []

    assert saver._load_checkpoint_attrs("thread-cache-bound", "", "one") is not None
    assert any(name.startswith("c~") for name in client.lookup_plus_names)


def test_put_writes_ordinary_entries_create_first_and_duplicate_noop(tmp_path):
    client = CountingFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )
    config = asyncio.run(
        _put_delta_checkpoint(
            saver,
            thread_id="thread-write-create-first",
            parent_config=None,
            checkpoint_id="root",
        )
    )

    client.lookup_plus_count = 0
    client.lookup_plus_names.clear()
    client.update_inode_count = 0
    client.update_inode_names.clear()
    client.create_names.clear()

    saver.put_writes(config, [("ch", "first")], "task-1")

    assert client.lookup_plus_names == []
    assert any(name.startswith("w~") for name in client.create_names)
    assert any(name.startswith("dw~") for name in client.create_names)
    assert client.update_inode_names == []

    client.lookup_plus_names.clear()
    client.update_inode_names.clear()
    client.create_names.clear()

    saver.put_writes(config, [("ch", "second")], "task-1")

    assert client.lookup_plus_names == []
    assert client.update_inode_names == []
    assert any(name.startswith("w~") for name in client.create_names)
    assert not any(name.startswith("dw~") for name in client.create_names)
    assert saver._load_pending_writes(
        "thread-write-create-first", "", "root"
    ) == [("task-1", "ch", "first")]


def test_special_writes_keep_upsert_semantics(tmp_path):
    client = CountingFsMetaClient()
    saver = NoKVCheckpointSaver(
        fsmeta_client=client,
        mount="vol",
        body_store=CheckpointBodyStore.from_local_path(tmp_path),
    )
    config = asyncio.run(
        _put_delta_checkpoint(
            saver,
            thread_id="thread-special-upsert",
            parent_config=None,
            checkpoint_id="root",
        )
    )

    saver.put_writes(config, [("__error__", "first")], "task-1")
    client.update_inode_names.clear()

    saver.put_writes(config, [("__error__", "second")], "task-1")

    assert sum(name.startswith("w~") for name in client.update_inode_names) == 1
    assert sum(name.startswith("dw~") for name in client.update_inode_names) == 1
    assert saver._load_pending_writes(
        "thread-special-upsert", "", "root"
    ) == [("task-1", "__error__", "second")]


def test_delta_history_fast_path_avoids_tuple_hydration(tmp_path):
    async def run():
        saver = make_saver(tmp_path)
        configs = await build_delta_chain(
            saver,
            thread_id="thread-fast-path",
            channel="ch",
            snapshots_at_steps=[0],
            total_steps=4,
        )

        def fail_sync(*args, **kwargs):
            raise AssertionError("delta fast path should not call tuple hydration")

        async def fail_async(*args, **kwargs):
            raise AssertionError("delta fast path should not call tuple hydration")

        saver.get_tuple = fail_sync
        saver.aget_tuple = fail_async
        saver._load_tuple = fail_sync
        saver._load_checkpoint_record = fail_sync
        saver._load_channel_value = fail_sync
        saver._load_channel_values = fail_sync

        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["ch"],
        )
        assert [write[2] for write in result["ch"]["writes"]] == [1, 2]
        assert result["ch"]["seed"].value == 0

    asyncio.run(run())


def test_delta_history_fast_path_avoids_canonical_writes_scan(tmp_path):
    async def run():
        saver = make_saver(tmp_path)
        configs = await build_delta_chain(
            saver,
            thread_id="thread-index-fast-path",
            channel="ch",
            snapshots_at_steps=[0],
            total_steps=5,
        )

        def fail_pending_scan(*args, **kwargs):
            raise AssertionError("delta index fast path should not scan writes dirs")

        saver._load_pending_writes = fail_pending_scan

        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["ch"],
        )
        assert [write[2] for write in result["ch"]["writes"]] == [1, 2, 3]
        assert result["ch"]["seed"].value == 0

    asyncio.run(run())


def test_delta_history_stage1_uses_cached_checkpoint_attrs_after_put(tmp_path):
    async def run():
        client = CountingFsMetaClient()
        saver = NoKVCheckpointSaver(
            fsmeta_client=client,
            mount="vol",
            body_store=CheckpointBodyStore.from_local_path(tmp_path),
        )
        configs = await build_delta_chain(
            saver,
            thread_id="thread-checkpoint-dir-stage1",
            channel="ch",
            snapshots_at_steps=[0],
            total_steps=5,
        )

        def fail_checkpoint_point_read(*args, **kwargs):
            raise AssertionError("delta stage 1 should not point-read checkpoint attrs")

        saver._load_checkpoint_attrs = fail_checkpoint_point_read
        client.lookup_plus_count = 0
        client.read_dir_plus_count = 0

        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["ch"],
        )

        assert [write[2] for write in result["ch"]["writes"]] == [1, 2, 3]
        assert result["ch"]["seed"].value == 0
        assert client.lookup_plus_count == 1  # logical thread tombstone check
        assert client.read_dir_plus_count == 1  # delta index page only

    asyncio.run(run())


def test_delta_history_latest_target_batches_tombstone_and_head(tmp_path):
    async def run():
        client = CountingFsMetaClient()
        saver = NoKVCheckpointSaver(
            fsmeta_client=client,
            mount="vol",
            body_store=CheckpointBodyStore.from_local_path(tmp_path),
        )
        await build_delta_chain(
            saver,
            thread_id="thread-delta-batch-latest",
            channel="ch",
            snapshots_at_steps=[0],
            total_steps=5,
        )
        client.batch_lookup_plus_count = 0
        client.batch_lookup_plus_names.clear()
        client.lookup_plus_names.clear()

        result = await saver.aget_delta_channel_history(
            config={
                "configurable": {
                    "thread_id": "thread-delta-batch-latest",
                    "checkpoint_ns": "",
                }
            },
            channels=["ch"],
        )

        assert [write[2] for write in result["ch"]["writes"]] == [1, 2, 3]
        assert result["ch"]["seed"].value == 0
        assert client.batch_lookup_plus_count == 1
        assert client.batch_lookup_plus_names == [["thread-tombstone", "latest"]]
        assert "thread-tombstone" not in client.lookup_plus_names
        assert "latest" not in client.lookup_plus_names

    asyncio.run(run())


def test_delta_history_directory_stage1_falls_back_for_legacy_seed_attrs(tmp_path):
    async def run():
        saver = make_saver(tmp_path)
        configs = await build_delta_chain(
            saver,
            thread_id="thread-legacy-stage1",
            channel="ch",
            snapshots_at_steps=[0],
            total_steps=4,
        )
        root_id = configs[0]["configurable"]["checkpoint_id"]
        root_attrs = saver._load_checkpoint_attrs("thread-legacy-stage1", "", root_id)
        assert root_attrs is not None
        saver._put_file(
            saver.layout.checkpoint_entry("thread-legacy-stage1", "", root_id),
            CheckpointEntryAttrs(
                checkpoint_id=root_attrs.checkpoint_id,
                parent_checkpoint_id=root_attrs.parent_checkpoint_id,
                body=root_attrs.body,
            ).to_opaque_attrs(),
        )

        fallback_called = False
        original_fallback = saver._get_delta_channel_history_by_parent_chain

        def fallback_spy(*args, **kwargs):
            nonlocal fallback_called
            fallback_called = True
            return original_fallback(*args, **kwargs)

        saver._get_delta_channel_history_by_parent_chain = fallback_spy

        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["ch"],
        )

        assert fallback_called is True
        assert [write[2] for write in result["ch"]["writes"]] == [1, 2]
        assert result["ch"]["seed"].value == 0

    asyncio.run(run())


def test_delta_history_directory_stage1_missing_target_returns_empty(tmp_path):
    saver = make_saver(tmp_path)

    result = saver.get_delta_channel_history(
        config={
            "configurable": {
                "thread_id": "thread-missing-target",
                "checkpoint_ns": "",
                "checkpoint_id": "missing",
            }
        },
        channels=["ch"],
    )

    assert result == {"ch": {"writes": []}}


def test_delta_history_falls_back_when_index_is_absent(tmp_path):
    async def run():
        saver = NoKVCheckpointSaver(
            fsmeta_client=FakeFsMetaClient(),
            mount="vol",
            body_store=CheckpointBodyStore.from_local_path(tmp_path),
            enable_delta_index=False,
        )
        configs = await build_delta_chain(
            saver,
            thread_id="thread-old-layout",
            channel="ch",
            snapshots_at_steps=[0],
            total_steps=4,
        )
        saver.enable_delta_index = True

        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["ch"],
        )
        assert [write[2] for write in result["ch"]["writes"]] == [1, 2]
        assert result["ch"]["seed"].value == 0

    asyncio.run(run())


def test_delta_history_index_filters_off_branch_writes(tmp_path):
    async def run():
        saver = make_saver(tmp_path)
        thread_id = "thread-fork-filter"
        root = await _put_delta_checkpoint(
            saver,
            thread_id=thread_id,
            parent_config=None,
            checkpoint_id="root",
            channel_values={"ch": _DeltaSnapshot(0)},
            channel_versions={"ch": 1},
        )
        main = await _put_delta_checkpoint(
            saver,
            thread_id=thread_id,
            parent_config=root,
            checkpoint_id="main",
        )
        await saver.aput_writes(main, [("ch", "main-write")], str(uuid4()))

        fork = await _put_delta_checkpoint(
            saver,
            thread_id=thread_id,
            parent_config=root,
            checkpoint_id="fork",
        )
        await saver.aput_writes(fork, [("ch", "fork-write")], str(uuid4()))

        head = await _put_delta_checkpoint(
            saver,
            thread_id=thread_id,
            parent_config=main,
            checkpoint_id="head",
        )

        result = await saver.aget_delta_channel_history(config=head, channels=["ch"])
        assert [write[2] for write in result["ch"]["writes"]] == ["main-write"]
        assert result["ch"]["seed"].value == 0

    asyncio.run(run())


def test_delta_history_multi_channel_windows_are_independent(tmp_path):
    async def run():
        saver = make_saver(tmp_path)
        thread_id = "thread-multi-channel"
        configs: list[RunnableConfig] = []
        parent_config: RunnableConfig | None = None

        for step in range(5):
            channel_values: dict[str, Any] = {}
            channel_versions: dict[str, int] = {}
            if step == 1:
                channel_values["a"] = _DeltaSnapshot("snap-a")
                channel_versions["a"] = step + 1
            if step == 3:
                channel_values["b"] = _DeltaSnapshot("snap-b")
                channel_versions["b"] = step + 1
            parent_config = await _put_delta_checkpoint(
                saver,
                thread_id=thread_id,
                parent_config=parent_config,
                channel_values=channel_values,
                channel_versions=channel_versions,
            )
            configs.append(parent_config)
            await saver.aput_writes(
                parent_config,
                [("a", step), ("b", step)],
                str(uuid4()),
            )

        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["a", "b"],
        )

        assert [write[2] for write in result["a"]["writes"]] == [1, 2, 3]
        assert [write[2] for write in result["b"]["writes"]] == [3]
        assert result["a"]["seed"].value == "snap-a"
        assert result["b"]["seed"].value == "snap-b"

    asyncio.run(run())


def test_delta_history_ordered_delta_index_scans_only_replay_window(tmp_path):
    async def run():
        client = CountingFsMetaClient()
        saver = NoKVCheckpointSaver(
            fsmeta_client=client,
            mount="vol",
            body_store=CheckpointBodyStore.from_local_path(tmp_path),
        )
        configs = await build_delta_chain(
            saver,
            thread_id="thread-ordered-delta-range",
            channel="ch",
            snapshots_at_steps=[0, 4],
            total_steps=7,
            write_value_fn=lambda step: f"write-{step}",
            checkpoint_id_fn=lambda step: f"{step:06d}",
        )

        client.read_dir_plus_count = 0
        client.read_dir_plus_pages.clear()
        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["ch"],
        )

        assert [write[2] for write in result["ch"]["writes"]] == ["write-5"]
        assert result["ch"]["seed"].value == "write-4"
        delta_pages = [
            names
            for names in client.read_dir_plus_pages
            if any(name.startswith("dw~") for name in names)
        ]
        assert len(delta_pages) == 1
        scanned_checkpoint_ids = {
            checkpoint_id
            for name in delta_pages[0]
            if (checkpoint_id := delta_write_checkpoint_id_from_name(name)) is not None
        }
        assert scanned_checkpoint_ids == {"000005", "000006"}

    asyncio.run(run())


def test_delta_history_plain_value_seed_keeps_seed_checkpoint_writes(tmp_path):
    async def run():
        saver = make_saver(tmp_path)
        configs: list[RunnableConfig] = []
        parent_config: RunnableConfig | None = None

        for step in range(4):
            channel_values: dict[str, Any] = {}
            channel_versions: dict[str, int] = {}
            if step == 1:
                channel_values["ch"] = [10, 20, 30]
                channel_versions["ch"] = step + 1
            parent_config = await _put_delta_checkpoint(
                saver,
                thread_id="thread-migration-seed",
                parent_config=parent_config,
                channel_values=channel_values,
                channel_versions=channel_versions,
            )
            configs.append(parent_config)
            if step in {1, 2}:
                await saver.aput_writes(parent_config, [("ch", step)], str(uuid4()))

        result = await saver.aget_delta_channel_history(
            config=configs[-1],
            channels=["ch"],
        )

        assert result["ch"]["seed"] == [10, 20, 30]
        assert [write[2] for write in result["ch"]["writes"]] == [1, 2]

    asyncio.run(run())


async def build_delta_chain(
    saver: BaseCheckpointSaver,
    *,
    thread_id: str | None = None,
    checkpoint_ns: str = "",
    channel: str = "messages",
    snapshots_at_steps: Sequence[int] = (0,),
    total_steps: int = 6,
    write_value_fn: Any | None = None,
    checkpoint_id_fn: Any | None = None,
) -> list[RunnableConfig]:
    """Local DeltaChannel fixture matching LangGraph's public saver contract.

    The released `langgraph-checkpoint-conformance==0.0.2` package does not ship
    delta-channel fixtures or a `delta_channel_history` capability. Keep this
    test helper local so the NoKV integration test suite remains reproducible
    against published packages while still covering the beta DeltaChannel
    contract we implement.
    """
    if write_value_fn is None:

        def write_value_fn(step: int) -> Any:
            return step

    if checkpoint_id_fn is None:

        def checkpoint_id_fn(step: int) -> str:
            return str(uuid6(clock_seq=-1))

    thread_id = thread_id or str(uuid4())
    snapshot_set = set(snapshots_at_steps)
    stored: list[RunnableConfig] = []
    parent_config: RunnableConfig | None = None

    for step in range(total_steps):
        channel_values: dict[str, Any] = {}
        channel_versions: dict[str, int] = {}
        if step in snapshot_set:
            channel_values[channel] = _DeltaSnapshot(write_value_fn(step))
            channel_versions[channel] = step + 1

        parent_config = await _put_delta_checkpoint(
            saver,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            parent_config=parent_config,
            checkpoint_id=checkpoint_id_fn(step),
            channel_values=channel_values,
            channel_versions=channel_versions,
        )
        stored.append(parent_config)

        if step not in snapshot_set:
            await saver.aput_writes(
                parent_config,
                [(channel, write_value_fn(step))],
                str(uuid4()),
            )

    return stored


async def _put_delta_checkpoint(
    saver,
    *,
    thread_id: str,
    parent_config,
    checkpoint_ns: str = "",
    checkpoint_id: str | None = None,
    channel_values: dict | None = None,
    channel_versions: dict | None = None,
):
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}}
    if parent_config is not None:
        config["configurable"]["checkpoint_id"] = parent_config["configurable"][
            "checkpoint_id"
        ]
    channel_values = channel_values or {}
    channel_versions = channel_versions or {}
    checkpoint = Checkpoint(
        v=1,
        id=checkpoint_id or str(uuid6(clock_seq=-1)),
        ts="",
        channel_values=channel_values,
        channel_versions=channel_versions,
        versions_seen={},
        updated_channels=None,
    )
    return await saver.aput(
        config,
        checkpoint,
        generate_metadata(step=0),
        channel_versions,
    )
