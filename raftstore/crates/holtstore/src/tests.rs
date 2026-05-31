use super::*;
use metastore::{MetadataEngine, MetadataSnapshotEngine};
use nokv_metastore as metastore;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

#[test]
fn opens_required_multi_tree_layout() {
    let store = HoltStore::open_memory().unwrap();
    store.data().unwrap();
    store.write().unwrap();
    store.region_meta().unwrap();
    store.apply_state().unwrap();
    store.watch_apply().unwrap();
}

#[test]
fn stores_data_tree_values() {
    let store = HoltStore::open_memory().unwrap();
    store.put_data(b"/workspace/a", b"meta").unwrap();
    assert_eq!(store.get_data(b"/workspace/a").unwrap().unwrap(), b"meta");
}

#[test]
fn watch_apply_events_survive_reopen_and_replay_after_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let event1 = metadatapb::MetadataApplyWatchEvent {
        region_id: 7,
        term: 2,
        index: 10,
        commit_version: 100,
        keys: vec![b"artifact/a".to_vec()],
        ..Default::default()
    };
    let event2 = metadatapb::MetadataApplyWatchEvent {
        region_id: 7,
        term: 2,
        index: 11,
        commit_version: 101,
        keys: vec![b"artifact/b".to_vec()],
        ..Default::default()
    };
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        store.put_watch_apply_event(&event1).unwrap();
        store.put_watch_apply_event(&event2).unwrap();
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    assert_eq!(
        reopened.first_watch_apply_event(7).unwrap().unwrap().index,
        10
    );
    let replay = reopened
        .watch_apply_events_after(7, 2, 10, b"artifact/", 16)
        .unwrap();
    assert_eq!(replay, vec![event2]);
}

#[test]
fn applies_cross_tree_atomic_batch() {
    let store = HoltStore::open_memory().unwrap();
    let applied = store
        .atomic(|batch| {
            batch.put(DATA_TREE, b"k", b"v");
            batch.put(REGION_META_TREE, b"k", b"meta");
        })
        .unwrap();
    assert!(applied);
    assert_eq!(store.data().unwrap().get(b"k").unwrap().unwrap(), b"v");
    assert_eq!(
        store.region_meta().unwrap().get(b"k").unwrap().unwrap(),
        b"meta"
    );
}

#[test]
fn region_descriptor_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let descriptor = metapb::RegionDescriptor {
        region_id: 42,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        epoch: Some(metapb::RegionEpoch {
            version: 7,
            conf_version: 3,
        }),
        peers: vec![metapb::RegionPeer {
            store_id: 5,
            peer_id: 55,
        }],
        ..Default::default()
    };
    {
        let store = HoltStore::open_file(dir.path()).unwrap();
        assert!(store.get_region_descriptor(42).unwrap().is_none());
        let bootstrapped = store
            .load_or_bootstrap_region_descriptor(&descriptor)
            .unwrap();
        assert_eq!(bootstrapped, descriptor);
        store.checkpoint().unwrap();
    }
    let reopened = HoltStore::open_file(dir.path()).unwrap();
    assert_eq!(
        reopened.get_region_descriptor(42).unwrap().unwrap(),
        descriptor
    );
    reopened.delete_region_descriptor(42).unwrap();
    assert!(reopened.get_region_descriptor(42).unwrap().is_none());
}

#[test]
fn region_descriptors_list_persisted_descriptors_in_region_order() {
    let store = HoltStore::open_memory().unwrap();
    let first = metapb::RegionDescriptor {
        region_id: 2,
        start_key: b"m".to_vec(),
        peers: vec![metapb::RegionPeer {
            store_id: 7,
            peer_id: 20,
        }],
        ..Default::default()
    };
    let second = metapb::RegionDescriptor {
        region_id: 1,
        end_key: b"m".to_vec(),
        peers: vec![metapb::RegionPeer {
            store_id: 7,
            peer_id: 10,
        }],
        ..Default::default()
    };
    store.put_region_descriptor(&first).unwrap();
    store.put_region_descriptor(&second).unwrap();

    let descriptors = store.region_descriptors().unwrap();
    assert_eq!(
        descriptors
            .iter()
            .map(|descriptor| descriptor.region_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(descriptors[0], second);
    assert_eq!(descriptors[1], first);
}

#[test]
fn region_apply_state_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let state = RegionApplyState {
        region_id: 42,
        term: 9,
        applied_index: 123,
        truncated_term: 8,
        truncated_index: 99,
    };
    {
        let store = HoltStore::open_file(dir.path()).unwrap();
        store.put_region_apply_state(&state).unwrap();
        store.checkpoint().unwrap();
    }
    let reopened = HoltStore::open_file(dir.path()).unwrap();
    assert_eq!(reopened.get_region_apply_state(42).unwrap().unwrap(), state);
}

#[test]
fn pending_root_events_survive_reopen_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let event = metapb::RootEvent {
        kind: metapb::RootEventKind::PeerAdded as i32,
        payload: Some(metapb::root_event::Payload::PeerChange(
            metapb::RootPeerChange {
                region_id: 42,
                store_id: 7,
                peer_id: 9,
                target: Some(metapb::RegionDescriptor {
                    region_id: 42,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        assert_eq!(store.enqueue_pending_root_event(&event).unwrap(), 1);
        assert_eq!(store.enqueue_pending_root_event(&event).unwrap(), 2);
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    let pending = reopened.pending_root_events().unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].sequence, 1);
    assert_eq!(pending[0].event, event);

    reopened.delete_pending_root_event(1).unwrap();
    let pending = reopened.pending_root_events().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sequence, 2);
}

#[test]
fn blocked_root_events_survive_reopen_and_advance_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let event = metapb::RootEvent {
        kind: metapb::RootEventKind::PeerAdded as i32,
        payload: Some(metapb::root_event::Payload::PeerChange(
            metapb::RootPeerChange {
                region_id: 42,
                store_id: 7,
                peer_id: 9,
                target: Some(metapb::RegionDescriptor {
                    region_id: 42,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        let sequence = store.enqueue_pending_root_event(&event).unwrap();
        store
            .block_pending_root_event(sequence, &event, "peer:42:add:7:9", "catalog precondition")
            .unwrap();
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    assert!(reopened.pending_root_events().unwrap().is_empty());
    let blocked = reopened.blocked_root_events().unwrap();
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].sequence, 1);
    assert_eq!(blocked[0].event, event);
    assert_eq!(blocked[0].transition_id, "peer:42:add:7:9");
    assert_eq!(blocked[0].last_error, "catalog precondition");
    assert_eq!(reopened.enqueue_pending_root_event(&event).unwrap(), 2);
}

#[test]
fn pending_scheduler_operations_survive_reopen_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let split = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
        region_id: 42,
        split_key: b"m".to_vec(),
        split_child: Some(metapb::RegionDescriptor {
            region_id: 43,
            ..Default::default()
        }),
        ..Default::default()
    };
    let merge = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::MergeRegion as i32,
        region_id: 42,
        source_region_id: 43,
        ..Default::default()
    };
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        store.record_pending_scheduler_operation(&split).unwrap();
        store.record_pending_scheduler_operation(&merge).unwrap();
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    let pending = reopened.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].operation, split);
    assert_eq!(pending[0].attempts, 0);
    assert_eq!(pending[1].operation, merge);
    assert_eq!(pending[1].attempts, 0);

    reopened.delete_pending_scheduler_operation(&split).unwrap();
    let pending = reopened.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].operation, merge);
    assert_eq!(pending[0].attempts, 0);
}

#[test]
fn pending_scheduler_operations_dedupe_by_full_operation_identity() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let split_m = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
        region_id: 42,
        split_key: b"m".to_vec(),
        split_child: Some(metapb::RegionDescriptor {
            region_id: 43,
            ..Default::default()
        }),
        ..Default::default()
    };
    let split_n = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
        region_id: 42,
        split_key: b"n".to_vec(),
        split_child: Some(metapb::RegionDescriptor {
            region_id: 44,
            ..Default::default()
        }),
        ..Default::default()
    };
    let merge = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::MergeRegion as i32,
        region_id: 42,
        source_region_id: 43,
        ..Default::default()
    };

    store.record_pending_scheduler_operation(&split_m).unwrap();
    store.record_pending_scheduler_operation(&split_m).unwrap();
    store.record_pending_scheduler_operation(&split_n).unwrap();
    store.record_pending_scheduler_operation(&merge).unwrap();

    let pending = store.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].operation, split_m);
    assert_eq!(pending[0].attempts, 0);
    assert_eq!(pending[1].operation, split_n);
    assert_eq!(pending[1].attempts, 0);
    assert_eq!(pending[2].operation, merge);
    assert_eq!(pending[2].attempts, 0);
}

#[test]
fn pending_scheduler_operation_attempts_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let operation = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
        region_id: 42,
        source_peer_id: 1,
        target_peer_id: 2,
        ..Default::default()
    };
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        store
            .record_pending_scheduler_operation(&operation)
            .unwrap();
        assert_eq!(
            store
                .increment_pending_scheduler_operation_attempts(&operation)
                .unwrap(),
            1
        );
        store
            .record_pending_scheduler_operation(&operation)
            .unwrap();
        assert_eq!(
            store
                .increment_pending_scheduler_operation_attempts(&operation)
                .unwrap(),
            2
        );
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    let pending = reopened.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].operation, operation);
    assert_eq!(pending[0].attempts, 2);
}

#[test]
fn blocked_scheduler_operations_survive_reopen_and_clear_pending() {
    let dir = tempfile::tempdir().unwrap();
    let operation = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
        region_id: 42,
        split_key: b"m".to_vec(),
        split_child: Some(metapb::RegionDescriptor {
            region_id: 43,
            ..Default::default()
        }),
        ..Default::default()
    };
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        store
            .record_pending_scheduler_operation(&operation)
            .unwrap();
        store
            .block_pending_scheduler_operation(&operation, 8, "attempt limit reached")
            .unwrap();
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    assert!(reopened.pending_scheduler_operations().unwrap().is_empty());
    let blocked = reopened.blocked_scheduler_operations().unwrap();
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].operation, operation);
    assert_eq!(blocked[0].attempts, 8);
    assert_eq!(blocked[0].last_error, "attempt limit reached");
}

fn metadata_put_command(
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
) -> metadatapb::MetadataCommand {
    let key = key.into();
    metadatapb::MetadataCommand {
        request_id: read_version.to_be_bytes().to_vec(),
        read_version,
        predicates: vec![metadatapb::MetadataPredicate {
            key: key.clone(),
            kind: metadatapb::MetadataPredicateKind::NotExists as i32,
            read_version,
            ..Default::default()
        }],
        mutations: vec![metadatapb::MetadataMutation {
            op: metadatapb::metadata_mutation::Op::Put as i32,
            key,
            value: value.into(),
            assertion_not_exist: true,
            ..Default::default()
        }],
        watch_keys: vec![b"artifact/".to_vec()],
        ..Default::default()
    }
}

fn metadata_overwrite_command(
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
) -> metadatapb::MetadataCommand {
    let key = key.into();
    metadatapb::MetadataCommand {
        request_id: read_version.to_be_bytes().to_vec(),
        read_version,
        mutations: vec![metadatapb::MetadataMutation {
            op: metadatapb::metadata_mutation::Op::Put as i32,
            key,
            value: value.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn holt_metadata_command_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        let result = store
            .commit_metadata(&metadata_put_command(b"artifact/a", b"v1", 10), 11)
            .unwrap();
        assert!(result.error.is_none());
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    let got = reopened
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 11,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(got.kv.unwrap().value, b"v1");
}

#[test]
fn holt_metadata_predicate_failure_does_not_partially_apply() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(&metadata_put_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();

    let failed = store
        .commit_metadata(&metadata_put_command(b"artifact/a", b"v2", 11), 12)
        .unwrap();

    assert!(failed.error.unwrap().already_exists.is_some());
    let got = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 12,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(got.kv.unwrap().value, b"v1");
}

#[test]
fn holt_metadata_snapshot_replaces_write_tree() {
    let source = HoltMetadataStore::open_memory().unwrap();
    source
        .commit_metadata(&metadata_put_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();
    let snapshot = source.export_metadata_snapshot().unwrap();

    let target = HoltMetadataStore::open_memory().unwrap();
    target
        .commit_metadata(&metadata_put_command(b"artifact/old", b"old", 20), 21)
        .unwrap();
    target.install_metadata_snapshot(snapshot).unwrap();

    assert!(
        target
            .get_metadata(&metadatapb::MetadataGetRequest {
                key: b"artifact/old".to_vec(),
                version: 21,
                ..Default::default()
            })
            .unwrap()
            .not_found
    );
    let got = target
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 11,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(got.kv.unwrap().value, b"v1");
}

#[test]
fn holt_metadata_reverse_scan_uses_start_key_as_upper_bound() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(&metadata_put_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();
    store
        .commit_metadata(&metadata_put_command(b"artifact/b", b"v2", 11), 12)
        .unwrap();
    store
        .commit_metadata(&metadata_put_command(b"artifact/c", b"v3", 12), 13)
        .unwrap();

    let scan = store
        .scan_metadata(&metadatapb::MetadataScanRequest {
            start_key: b"artifact/c".to_vec(),
            include_start: false,
            limit: 10,
            version: 13,
            reverse: true,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(
        scan.kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<Vec<_>>(),
        vec![
            (b"artifact/b".to_vec(), b"v2".to_vec()),
            (b"artifact/a".to_vec(), b"v1".to_vec())
        ]
    );
}

#[test]
fn holt_metadata_retention_prunes_only_versions_hidden_by_floor_anchor() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v2", 20), 21)
        .unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v3", 30), 31)
        .unwrap();

    let pruned = store.prune_metadata_versions(25).unwrap();
    assert_eq!(pruned.retention_floor, 25);
    assert_eq!(pruned.retained_anchor_versions, 1);
    assert_eq!(pruned.pruned_versions, 1);

    let at_floor = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 25,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(at_floor.kv.unwrap().value, b"v2");
    let latest = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 31,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(latest.kv.unwrap().value, b"v3");

    let snapshot = store.export_metadata_snapshot().unwrap();
    assert_eq!(
        snapshot
            .writes
            .into_iter()
            .map(|write| write.commit_version)
            .collect::<Vec<_>>(),
        vec![21, 31]
    );
}

#[test]
fn holt_metadata_retention_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        store
            .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v1", 10), 11)
            .unwrap();
        store
            .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v2", 20), 21)
            .unwrap();
        store
            .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v3", 30), 31)
            .unwrap();
        store.prune_metadata_versions(25).unwrap();
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    let at_floor = reopened
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 25,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(at_floor.kv.unwrap().value, b"v2");
    let snapshot = reopened.export_metadata_snapshot().unwrap();
    assert_eq!(
        snapshot
            .writes
            .into_iter()
            .map(|write| write.commit_version)
            .collect::<Vec<_>>(),
        vec![21, 31]
    );
}
