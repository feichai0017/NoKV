use super::*;
use metadata_state::{MetadataEngine, MetadataSnapshotEngine};
use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

#[test]
fn opens_required_multi_tree_layout() {
    let store = HoltStore::open_memory().unwrap();
    store.default_current().unwrap();
    store.history().unwrap();
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
fn metadata_retention_prunes_watch_apply_history_with_region_anchor() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let event1 = metadatapb::MetadataApplyWatchEvent {
        region_id: 7,
        term: 2,
        index: 10,
        commit_version: 11,
        keys: vec![b"artifact/a".to_vec()],
        ..Default::default()
    };
    let event2 = metadatapb::MetadataApplyWatchEvent {
        region_id: 7,
        term: 2,
        index: 11,
        commit_version: 21,
        keys: vec![b"artifact/b".to_vec()],
        ..Default::default()
    };
    let event3 = metadatapb::MetadataApplyWatchEvent {
        region_id: 7,
        term: 2,
        index: 12,
        commit_version: 31,
        keys: vec![b"artifact/c".to_vec()],
        ..Default::default()
    };
    store.put_watch_apply_event(&event1).unwrap();
    store.put_watch_apply_event(&event2).unwrap();
    store.put_watch_apply_event(&event3).unwrap();

    let pruned = store.prune_metadata_versions(25).unwrap();
    assert_eq!(pruned.pruned_watch_events, 1);
    assert_eq!(
        store.first_watch_apply_event(7).unwrap().unwrap().index,
        event2.index
    );
    assert_eq!(
        store.watch_apply_retention_cursor(7).unwrap(),
        Some((event2.term, event2.index, event2.commit_version))
    );
    let replay = store
        .watch_apply_events_after(7, 2, event2.index, b"artifact/", 16)
        .unwrap();
    assert_eq!(replay, vec![event3]);
}

#[test]
fn applies_cross_tree_atomic_batch() {
    let store = HoltStore::open_memory().unwrap();
    let applied = store
        .atomic(|batch| {
            batch.put(DEFAULT_CURRENT_TREE, b"k", b"v");
            batch.put(REGION_META_TREE, b"k", b"meta");
        })
        .unwrap();
    assert!(applied);
    assert_eq!(
        store.default_current().unwrap().get(b"k").unwrap().unwrap(),
        b"v"
    );
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
    let transfer = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
        region_id: 42,
        source_peer_id: 1,
        target_peer_id: 2,
        ..Default::default()
    };
    let prune = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::PruneMetadataVersions as i32,
        region_id: 42,
        retention_floor: 100,
        ..Default::default()
    };
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        store.record_pending_scheduler_operation(&transfer).unwrap();
        store.record_pending_scheduler_operation(&prune).unwrap();
        store.checkpoint().unwrap();
    }

    let reopened = HoltMetadataStore::open_file(dir.path()).unwrap();
    let pending = reopened.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].operation, transfer);
    assert_eq!(pending[0].attempts, 0);
    assert_eq!(pending[1].operation, prune);
    assert_eq!(pending[1].attempts, 0);

    reopened
        .delete_pending_scheduler_operation(&transfer)
        .unwrap();
    let pending = reopened.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].operation, prune);
    assert_eq!(pending[0].attempts, 0);
}

#[test]
fn pending_scheduler_operations_dedupe_by_full_operation_identity() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let transfer_to_2 = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
        region_id: 42,
        source_peer_id: 1,
        target_peer_id: 2,
        ..Default::default()
    };
    let transfer_to_3 = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
        region_id: 42,
        source_peer_id: 1,
        target_peer_id: 3,
        ..Default::default()
    };
    let prune = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::PruneMetadataVersions as i32,
        region_id: 42,
        retention_floor: 100,
        ..Default::default()
    };

    store
        .record_pending_scheduler_operation(&transfer_to_2)
        .unwrap();
    store
        .record_pending_scheduler_operation(&transfer_to_2)
        .unwrap();
    store
        .record_pending_scheduler_operation(&transfer_to_3)
        .unwrap();
    store.record_pending_scheduler_operation(&prune).unwrap();

    let pending = store.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].operation, transfer_to_2);
    assert_eq!(pending[0].attempts, 0);
    assert_eq!(pending[1].operation, transfer_to_3);
    assert_eq!(pending[1].attempts, 0);
    assert_eq!(pending[2].operation, prune);
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

fn metadata_retention_pin_command(
    key: impl Into<Vec<u8>>,
    read_version: u64,
    pin_version: u64,
) -> metadatapb::MetadataCommand {
    metadatapb::MetadataCommand {
        request_id: read_version.to_be_bytes().to_vec(),
        read_version,
        mutations: vec![metadatapb::MetadataMutation {
            op: metadatapb::metadata_mutation::Op::Put as i32,
            key: key.into(),
            value: b"snapshot-pin".to_vec(),
            key_family: metadatapb::MetadataFamily::Snapshot as i32,
            retention_pin_version: pin_version,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn family_put_command(
    family: metadatapb::MetadataFamily,
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
) -> metadatapb::MetadataCommand {
    let key = key.into();
    let mut request_id = Vec::with_capacity(16);
    request_id.extend_from_slice(&(family as i32 as u64).to_be_bytes());
    request_id.extend_from_slice(&read_version.to_be_bytes());
    metadatapb::MetadataCommand {
        request_id,
        read_version,
        predicates: vec![metadatapb::MetadataPredicate {
            key: key.clone(),
            key_family: family as i32,
            kind: metadatapb::MetadataPredicateKind::NotExists as i32,
            read_version,
            ..Default::default()
        }],
        mutations: vec![metadatapb::MetadataMutation {
            op: metadatapb::metadata_mutation::Op::Put as i32,
            key,
            key_family: family as i32,
            value: value.into(),
            assertion_not_exist: true,
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
fn holt_metadata_command_dedupe_returns_original_commit_result() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let command = metadata_put_command(b"artifact/a", b"v1", 10);
    let first = store.commit_metadata(&command, 11).unwrap();
    assert!(first.error.is_none());

    let replayed = store.commit_metadata(&command, 12).unwrap();
    assert!(replayed.error.is_none());
    assert_eq!(replayed.commit_version, 11);
    assert_eq!(replayed.applied_mutations, 1);

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
fn holt_metadata_command_dedupe_rejects_request_id_reuse_for_different_command() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let command = metadata_put_command(b"artifact/a", b"v1", 10);
    store.commit_metadata(&command, 11).unwrap();

    let mut changed = command;
    changed.mutations[0].value = b"v2".to_vec();
    let err = store.commit_metadata(&changed, 12).unwrap_err();
    assert!(err
        .to_string()
        .contains("metadata command request id reused with different command"));
}

#[test]
fn holt_metadata_family_current_trees_keep_identical_keys_separate() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Inode, b"same", b"inode", 10),
            11,
        )
        .unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Dentry, b"same", b"dentry", 20),
            21,
        )
        .unwrap();

    let inode = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"same".to_vec(),
            key_family: metadatapb::MetadataFamily::Inode as i32,
            version: 21,
            ..Default::default()
        })
        .unwrap();
    let dentry = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"same".to_vec(),
            key_family: metadatapb::MetadataFamily::Dentry as i32,
            version: 21,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(inode.kv.unwrap().value, b"inode");
    assert_eq!(dentry.kv.unwrap().value, b"dentry");
}

#[test]
fn holt_metadata_scan_uses_only_requested_family_current_tree() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Inode, b"a", b"inode-a", 10),
            11,
        )
        .unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Dentry, b"a", b"dentry-a", 20),
            21,
        )
        .unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Dentry, b"b", b"dentry-b", 30),
            31,
        )
        .unwrap();

    let scan = store
        .scan_metadata(&metadatapb::MetadataScanRequest {
            key_family: metadatapb::MetadataFamily::Dentry as i32,
            version: 31,
            limit: 10,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(
        scan.kvs
            .into_iter()
            .map(|kv| (kv.key_family, kv.key, kv.value))
            .collect::<Vec<_>>(),
        vec![
            (
                metadatapb::MetadataFamily::Dentry as i32,
                b"a".to_vec(),
                b"dentry-a".to_vec()
            ),
            (
                metadatapb::MetadataFamily::Dentry as i32,
                b"b".to_vec(),
                b"dentry-b".to_vec()
            )
        ]
    );
}

#[test]
fn holt_metadata_scan_uses_current_tree_prefix() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Dentry, b"a/1", b"v1", 10),
            11,
        )
        .unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Dentry, b"a/2", b"v2", 20),
            21,
        )
        .unwrap();
    store
        .commit_metadata(
            &family_put_command(metadatapb::MetadataFamily::Dentry, b"b/1", b"other", 30),
            31,
        )
        .unwrap();
    let scan = store
        .scan_metadata(&metadatapb::MetadataScanRequest {
            key_family: metadatapb::MetadataFamily::Dentry as i32,
            start_key: b"a/".to_vec(),
            prefix_key: b"a/".to_vec(),
            version: 31,
            limit: 10,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(
        scan.kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<Vec<_>>(),
        vec![
            (b"a/1".to_vec(), b"v1".to_vec()),
            (b"a/2".to_vec(), b"v2".to_vec())
        ]
    );
}

#[test]
fn holt_metadata_commit_skips_history_without_retention() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(&metadata_put_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();

    let history_count = store.store.history().unwrap().range().into_iter().count();
    assert_eq!(history_count, 0);
    let got = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 11,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(got.kv.unwrap().value, b"v1");
    let snapshot = store.export_metadata_snapshot().unwrap();
    assert!(snapshot
        .writes
        .iter()
        .any(|write| write.key == b"artifact/a" && write.commit_version == 11));
}

#[test]
fn holt_metadata_retention_preserves_prior_current_version() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();
    store
        .commit_metadata(&metadata_retention_pin_command(b"snapshot/15", 40, 15), 41)
        .unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v2", 50), 51)
        .unwrap();

    let snapshot_read = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 15,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(snapshot_read.kv.unwrap().value, b"v1");
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
fn holt_metadata_prefix_empty_failure_does_not_partially_apply() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(&metadata_put_command(b"dir/child", b"v1", 10), 11)
        .unwrap();

    let failed = store
        .commit_metadata(
            &metadatapb::MetadataCommand {
                request_id: b"remove-dir".to_vec(),
                read_version: 11,
                predicates: vec![metadatapb::MetadataPredicate {
                    key: b"dir/".to_vec(),
                    kind: metadatapb::MetadataPredicateKind::PrefixEmpty as i32,
                    read_version: 11,
                    ..Default::default()
                }],
                mutations: vec![metadatapb::MetadataMutation {
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    key: b"side-effect".to_vec(),
                    value: b"bad".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            12,
        )
        .unwrap();

    assert!(failed.error.is_some());
    assert!(
        store
            .get_metadata(&metadatapb::MetadataGetRequest {
                key: b"side-effect".to_vec(),
                version: 12,
                ..Default::default()
            })
            .unwrap()
            .not_found
    );
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
        .store
        .region_meta()
        .unwrap()
        .put(crate::trees::METADATA_HISTORY_ACTIVE_KEY, b"1")
        .unwrap();
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
            .filter(|write| write.key == b"artifact/a")
            .map(|write| write.commit_version)
            .collect::<Vec<_>>(),
        vec![21, 31]
    );
}

#[test]
fn holt_metadata_retention_pin_clamps_requested_floor() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();
    store
        .commit_metadata(&metadata_retention_pin_command(b"snapshot/15", 40, 15), 41)
        .unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v2", 50), 51)
        .unwrap();
    store
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v3", 60), 61)
        .unwrap();

    let pruned = store.prune_metadata_versions(60).unwrap();
    assert_eq!(pruned.retention_floor, 15);
    assert_eq!(pruned.pruned_versions, 0);

    let snapshot_read = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 15,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(snapshot_read.kv.unwrap().value, b"v1");
}

#[test]
fn holt_metadata_retention_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = HoltMetadataStore::open_file(dir.path()).unwrap();
        store
            .store
            .region_meta()
            .unwrap()
            .put(crate::trees::METADATA_HISTORY_ACTIVE_KEY, b"1")
            .unwrap();
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
            .filter(|write| write.key == b"artifact/a")
            .map(|write| write.commit_version)
            .collect::<Vec<_>>(),
        vec![21, 31]
    );
}

#[test]
fn holt_metadata_snapshot_round_trips_retention_pin_versions() {
    let source = HoltMetadataStore::open_memory().unwrap();
    source
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v1", 10), 11)
        .unwrap();
    source
        .commit_metadata(&metadata_retention_pin_command(b"snapshot/15", 40, 15), 41)
        .unwrap();
    source
        .commit_metadata(&metadata_overwrite_command(b"artifact/a", b"v2", 50), 51)
        .unwrap();

    let target = HoltMetadataStore::open_memory().unwrap();
    target
        .install_metadata_snapshot(source.export_metadata_snapshot().unwrap())
        .unwrap();

    let pruned = target.prune_metadata_versions(20).unwrap();
    assert_eq!(pruned.retention_floor, 15);
    let snapshot_read = target
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"artifact/a".to_vec(),
            version: 15,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(snapshot_read.kv.unwrap().value, b"v1");
}
