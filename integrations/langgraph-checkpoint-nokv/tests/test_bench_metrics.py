from __future__ import annotations

from fake_fsmeta import FakeFsMetaClient

from langgraph.checkpoint.nokv import CheckpointBodyStore, InodeType, LookupKey
from langgraph.checkpoint.nokv._bench_metrics import (
    InstrumentedCheckpointBodyStore,
    InstrumentedFsMetaClient,
    InstrumentedSaverMetrics,
    InstrumentedSerde,
    PhaseTracker,
)


class DummySerde:
    def dumps_typed(self, value):
        return "bytes", bytes(value)

    def loads_typed(self, value):
        return value[1]


def test_instrumented_fsmeta_client_splits_phase_and_path_category():
    client = InstrumentedFsMetaClient(FakeFsMetaClient())
    root = 1

    langgraph = client.create(
        mount="vol",
        parent=root,
        name="langgraph",
        inode_type=InodeType.DIRECTORY,
    )
    threads = client.create(
        mount="vol",
        parent=langgraph.inode.inode,
        name="threads",
        inode_type=InodeType.DIRECTORY,
    )
    thread = client.create(
        mount="vol",
        parent=threads.inode.inode,
        name="b64~dA",
        inode_type=InodeType.DIRECTORY,
    )
    namespaces = client.create(
        mount="vol",
        parent=thread.inode.inode,
        name="namespaces",
        inode_type=InodeType.DIRECTORY,
    )
    namespace = client.create(
        mount="vol",
        parent=namespaces.inode.inode,
        name="b64~",
        inode_type=InodeType.DIRECTORY,
    )
    checkpoints = client.create(
        mount="vol",
        parent=namespace.inode.inode,
        name="checkpoints",
        inode_type=InodeType.DIRECTORY,
    )

    with client.phase("write_phase"):
        client.create(
            mount="vol",
            parent=checkpoints.inode.inode,
            name="c~b64~Y2s",
            inode_type=InodeType.FILE,
            opaque_attrs=b"checkpoint",
        )

    with client.phase("delta_history_phase"):
        client.lookup_plus(
            mount="vol",
            parent=checkpoints.inode.inode,
            name="c~b64~Y2s",
        )

    metrics = client.to_json()

    assert metrics["create"]["count"] == 7
    assert metrics["lookup_plus"]["count"] == 1
    assert metrics["by_phase"]["write_phase"]["create"]["count"] == 1
    assert metrics["by_phase"]["delta_history_phase"]["lookup_plus"]["count"] == 1
    assert metrics["by_category"]["checkpoint_attrs"]["create"]["count"] == 1
    assert metrics["by_category"]["checkpoint_attrs"]["lookup_plus"]["count"] == 1
    assert (
        metrics["by_phase_category"]["delta_history_phase"]["checkpoint_attrs"][
            "lookup_plus"
        ]["count"]
        == 1
    )


def test_instrumented_fsmeta_client_categorizes_readdirplus_by_directory():
    client = InstrumentedFsMetaClient(FakeFsMetaClient())
    root = 1

    delta_channels = client.create(
        mount="vol",
        parent=root,
        name="delta_channels",
        inode_type=InodeType.DIRECTORY,
    )
    channel = client.create(
        mount="vol",
        parent=delta_channels.inode.inode,
        name="b64~Y2g",
        inode_type=InodeType.DIRECTORY,
    )
    client.create(
        mount="vol",
        parent=channel.inode.inode,
        name="dw~b64~Y2s~b64~dGFzaw~i~09223372036854775808",
        inode_type=InodeType.FILE,
        opaque_attrs=b"delta",
    )

    with client.phase("storage_count_phase"):
        client.read_dir_plus(mount="vol", parent=channel.inode.inode, limit=1024)

    metrics = client.to_json()

    assert metrics["by_phase"]["storage_count_phase"]["read_dir_plus"]["count"] == 1
    assert metrics["by_category"]["delta_index"]["read_dir_plus"]["count"] == 1
    assert (
        metrics["by_phase_category"]["storage_count_phase"]["delta_index"][
            "read_dir_plus"
        ]["count"]
        == 1
    )


def test_instrumented_fsmeta_client_records_batch_lookup_plus():
    client = InstrumentedFsMetaClient(FakeFsMetaClient())
    root = 1

    thread = client.create(
        mount="vol",
        parent=root,
        name="thread",
        inode_type=InodeType.DIRECTORY,
    )
    tombstone = client.create(
        mount="vol",
        parent=thread.inode.inode,
        name="thread-tombstone",
        inode_type=InodeType.FILE,
    )
    heads = client.create(
        mount="vol",
        parent=thread.inode.inode,
        name="heads",
        inode_type=InodeType.DIRECTORY,
    )
    latest = client.create(
        mount="vol",
        parent=heads.inode.inode,
        name="latest",
        inode_type=InodeType.FILE,
    )

    with client.phase("get_state_phase"):
        result = client.batch_lookup_plus(
            mount="vol",
            lookups=[
                LookupKey(parent=thread.inode.inode, name="thread-tombstone"),
                LookupKey(parent=heads.inode.inode, name="latest"),
            ],
        )

    metrics = client.to_json()

    assert [item.found for item in result] == [True, True]
    assert tombstone.dentry.inode != latest.dentry.inode
    assert metrics["batch_lookup_plus"]["count"] == 1
    assert metrics["by_phase"]["get_state_phase"]["batch_lookup_plus"]["count"] == 1
    assert metrics["by_category"]["mixed_lookup"]["batch_lookup_plus"]["count"] == 1


def test_instrumented_payload_metrics_record_phase_bytes_and_items(tmp_path):
    phase_tracker = PhaseTracker()
    body_store = InstrumentedCheckpointBodyStore(
        CheckpointBodyStore.from_local_path(tmp_path / "body"),
        phase_tracker=phase_tracker,
    )
    serde = InstrumentedSerde(DummySerde(), phase_tracker=phase_tracker)
    saver_metrics = InstrumentedSaverMetrics(phase_tracker=phase_tracker)

    with phase_tracker.phase("get_state_phase"):
        body = serde.dumps_typed(b"payload")
        ref = body_store.put_typed(*body)
        loaded = body_store.get_typed(ref)
        assert serde.loads_typed(loaded) == b"payload"
        saver_metrics.record_items("load_pending_writes", 3)

    with phase_tracker.phase("delta_history_phase"):
        saver_metrics.record_delta_history_result(
            {
                "ch0": {"writes": [("task", "ch0", 1), ("task", "ch0", 2)]},
                "ch1": {"writes": [], "seed": "snapshot"},
            }
        )

    assert body_store.to_json()["by_phase"]["get_state_phase"]["get_typed"][
        "bytes_total"
    ] == len(b"payload")
    assert serde.to_json()["by_phase"]["get_state_phase"]["loads_typed"][
        "bytes_total"
    ] == len(b"payload")
    saver_json = saver_metrics.to_json()
    assert saver_json["by_phase"]["get_state_phase"]["load_pending_writes"][
        "items_total"
    ] == 3
    assert saver_json["by_phase"]["delta_history_phase"]["delta_history_writes"][
        "items_total"
    ] == 2
    assert saver_json["by_phase"]["delta_history_phase"]["delta_history_seeds"][
        "items_total"
    ] == 1
