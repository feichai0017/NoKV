from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any
from uuid import uuid4

from fake_fsmeta import FakeFsMetaClient
from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import BaseCheckpointSaver, Checkpoint
from langgraph.checkpoint.base.id import uuid6
from langgraph.checkpoint.conformance.initializer import checkpointer_test
from langgraph.checkpoint.conformance.test_utils import generate_metadata
from langgraph.checkpoint.conformance.validate import validate
from langgraph.checkpoint.nokv import CheckpointBodyStore, NoKVCheckpointSaver
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
        self.lookup_plus_count = 0
        self.read_dir_plus_count = 0

    def lookup_plus(self, **kwargs):
        self.lookup_plus_count += 1
        return super().lookup_plus(**kwargs)

    def read_dir_plus(self, **kwargs):
        self.read_dir_plus_count += 1
        return super().read_dir_plus(**kwargs)


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
