use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nokv_metadata_state::MemoryMetadataStore;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use openraft::{storage::RaftLogStorageExt, CommittedLeaderId, EntryPayload, LogId};

use super::*;
use crate::{AppliedMetadataEngine, MetadataReadExecutor, Proposal, RegionLogFlushOptions};

fn log_id(term: u64, index: u64) -> LogId<NodeId> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

fn normal_entry(region_id: u64, index: u64, key: &[u8], value: &[u8]) -> OpenRaftEntry {
    let command = metadatapb::MetadataCommitRequest {
        context: Some(metadatapb::MetadataContext {
            region_id,
            ..Default::default()
        }),
        command: Some(metadatapb::MetadataCommand {
            request_id: index.to_be_bytes().to_vec(),
            read_version: index,
            commit_version: index.saturating_add(1),
            mutations: vec![metadatapb::MetadataMutation {
                op: metadatapb::metadata_mutation::Op::Put as i32,
                key: key.to_vec(),
                value: value.to_vec(),
                ..Default::default()
            }],
            watch_keys: vec![key.to_vec()],
            ..Default::default()
        }),
    };
    OpenRaftEntry {
        log_id: log_id(3, index),
        payload: EntryPayload::Normal(Proposal::from_metadata_command(&command).unwrap()),
    }
}

fn membership_entry(_region_id: u64, index: u64, voter: NodeId) -> OpenRaftEntry {
    let nodes = BTreeMap::from([(voter, BasicNode::new(format!("local-{voter}")))]);
    OpenRaftEntry {
        log_id: log_id(1, index),
        payload: EntryPayload::Membership(openraft::Membership::new(
            vec![BTreeSet::from([voter])],
            nodes,
        )),
    }
}

#[tokio::test]
async fn region_log_storage_appends_and_reads_entries() {
    let dir = tempfile::tempdir().unwrap();
    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let mut storage = RegionLogStorage::new(log);
    let entries = vec![
        normal_entry(7, 1, b"a", b"1"),
        normal_entry(7, 2, b"b", b"2"),
    ];

    storage.blocking_append(entries).await.unwrap();
    assert_eq!(
        storage
            .get_log_state()
            .await
            .unwrap()
            .last_log_id
            .unwrap()
            .index,
        2
    );

    let mut reader = storage.get_log_reader().await;
    let read = reader.try_get_log_entries(2..3).await.unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].log_id.index, 2);
}

#[tokio::test]
async fn region_log_storage_defaults_to_buffered_log_flush() {
    let before = crate::metrics::raftnode_metrics_snapshot();
    let dir = tempfile::tempdir().unwrap();
    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let mut storage = RegionLogStorage::new(log);

    storage
        .blocking_append(vec![normal_entry(7, 1, b"a", b"1")])
        .await
        .unwrap();

    let after = crate::metrics::raftnode_metrics_snapshot();
    assert!(after.log_flush_skipped_total > before.log_flush_skipped_total);
}

#[tokio::test]
async fn region_log_storage_group_commit_flushes_before_callback() {
    let before = crate::metrics::raftnode_metrics_snapshot();
    let dir = tempfile::tempdir().unwrap();
    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let mut storage = RegionLogStorage::new_with_options(
        log,
        RegionLogFlushOptions::group_commit(Duration::from_millis(1)),
    );

    storage
        .blocking_append(vec![normal_entry(7, 1, b"a", b"1")])
        .await
        .unwrap();

    let after = crate::metrics::raftnode_metrics_snapshot();
    assert!(after.log_group_flush_calls_total > before.log_group_flush_calls_total);
    assert!(after.log_group_flush_callbacks_total > before.log_group_flush_callbacks_total);
}

#[tokio::test]
async fn region_log_storage_truncates_conflicting_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let mut storage = RegionLogStorage::new(log);
    storage
        .blocking_append(vec![
            normal_entry(7, 1, b"a", b"1"),
            normal_entry(7, 2, b"b", b"2"),
            normal_entry(7, 3, b"c", b"3"),
        ])
        .await
        .unwrap();

    storage.truncate(log_id(3, 3)).await.unwrap();
    let mut reader = storage.get_log_reader().await;
    let read = reader.try_get_log_entries(0..10).await.unwrap();
    assert_eq!(
        read.iter()
            .map(|entry| entry.log_id.index)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        storage
            .get_log_state()
            .await
            .unwrap()
            .last_log_id
            .unwrap()
            .index,
        2
    );
}

#[tokio::test]
async fn region_log_storage_purges_prefix_and_recovers_marker() {
    let dir = tempfile::tempdir().unwrap();
    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let mut storage = RegionLogStorage::new(log);
    storage
        .blocking_append(vec![
            normal_entry(7, 1, b"a", b"1"),
            normal_entry(7, 2, b"b", b"2"),
            normal_entry(7, 3, b"c", b"3"),
        ])
        .await
        .unwrap();
    storage.purge(log_id(3, 2)).await.unwrap();
    drop(storage);

    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let mut storage = RegionLogStorage::new(log);
    let state = storage.get_log_state().await.unwrap();
    let purged = state.last_purged_log_id.unwrap();
    assert_eq!(purged.index, 2);
    assert_eq!(purged.leader_id.node_id, 1);
    assert_eq!(state.last_log_id.unwrap().index, 3);

    let mut reader = storage.get_log_reader().await;
    let read = reader.try_get_log_entries(0..10).await.unwrap();
    assert_eq!(
        read.iter()
            .map(|entry| entry.log_id.index)
            .collect::<Vec<_>>(),
        vec![3]
    );
}

#[tokio::test]
async fn region_log_storage_recovers_latest_membership() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    log.append_entries(&[
        membership_entry(7, 1, 1),
        normal_entry(7, 2, b"a", b"1"),
        membership_entry(7, 3, 2),
    ])
    .unwrap();
    drop(log);

    let storage = RegionLogStorage::new(SegmentedEntryLog::open(7, dir.path()).unwrap());
    let membership = storage.latest_membership().unwrap().unwrap();
    assert_eq!(membership.log_id().as_ref().unwrap().index, 3);
    assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![2]);
}

#[tokio::test]
async fn region_log_storage_recovers_exact_log_id_for_applied_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = RegionLogStorage::new(SegmentedEntryLog::open(7, dir.path()).unwrap());
    storage
        .blocking_append(vec![
            normal_entry(7, 1, b"a", b"1"),
            normal_entry(7, 2, b"b", b"2"),
        ])
        .await
        .unwrap();
    assert_eq!(storage.log_id_at_index(2).unwrap(), Some(log_id(3, 2)));
    storage.purge(log_id(3, 2)).await.unwrap();
    assert_eq!(storage.log_id_at_index(2).unwrap(), Some(log_id(3, 2)));
    assert_eq!(storage.log_id_at_index(3).unwrap(), None);
}

#[tokio::test]
async fn region_log_storage_seeds_restart_vote_above_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    log.append_entries(&[membership_entry(7, 1, 1), normal_entry(7, 2, b"a", b"1")])
        .unwrap();
    drop(log);

    let mut storage = RegionLogStorage::new(SegmentedEntryLog::open(7, dir.path()).unwrap());
    storage.seed_single_node_vote_above_log(1).unwrap();
    assert_eq!(
        storage.read_vote().await.unwrap().unwrap().leader_id.term,
        4
    );
    drop(storage);

    let mut storage = RegionLogStorage::new(SegmentedEntryLog::open(7, dir.path()).unwrap());
    let vote = storage.read_vote().await.unwrap().unwrap();
    assert_eq!(vote.leader_id.term, 4);
    assert_eq!(vote.leader_id.voted_for(), Some(1));
    assert!(vote.committed);
}

#[tokio::test]
async fn region_log_storage_persists_committed_log_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = RegionLogStorage::new(SegmentedEntryLog::open(7, dir.path()).unwrap());
    storage.save_committed(Some(log_id(5, 8))).await.unwrap();
    drop(storage);

    let mut storage = RegionLogStorage::new(SegmentedEntryLog::open(7, dir.path()).unwrap());
    let committed = storage.read_committed().await.unwrap().unwrap();
    assert_eq!(committed.leader_id.term, 5);
    assert_eq!(committed.index, 8);

    storage.save_committed(None).await.unwrap();
    drop(storage);
    let mut storage = RegionLogStorage::new(SegmentedEntryLog::open(7, dir.path()).unwrap());
    assert!(storage.read_committed().await.unwrap().is_none());
}

#[tokio::test]
async fn region_state_machine_applies_entries() {
    let engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::default());
    let mut state_machine = RegionStateMachine::new(engine);
    let entries = vec![
        normal_entry(7, 1, b"a", b"1"),
        normal_entry(7, 2, b"b", b"2"),
    ];

    let applied = state_machine.apply(entries).await.unwrap();
    assert_eq!(applied.len(), 2);
    assert_eq!(state_machine.apply_engine().status().applied_index, 2);
}

#[tokio::test]
async fn region_state_machine_builds_and_installs_snapshot() {
    let engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::default());
    let mut state_machine = RegionStateMachine::new(engine.clone());
    state_machine
        .apply(vec![
            normal_entry(7, 1, b"a", b"1"),
            normal_entry(7, 2, b"b", b"2"),
        ])
        .await
        .unwrap();

    let mut builder = state_machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();
    assert_eq!(snapshot.meta.last_log_id.unwrap(), log_id(3, 2));
    assert!(!snapshot.snapshot.get_ref().is_empty());

    let restored = AppliedMetadataEngine::new(7, MemoryMetadataStore::default());
    let mut restored_state_machine = RegionStateMachine::new(restored.clone());
    restored_state_machine
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();

    assert_eq!(restored.status().applied_index, 2);
    let current = restored
        .execute_metadata_get(&metadatapb::MetadataGetRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            key: b"b".to_vec(),
            version: 3,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(current.kv.unwrap().value, b"2");

    let current_snapshot = restored_state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert!(!current_snapshot.snapshot.get_ref().is_empty());
}

#[tokio::test]
async fn region_state_machine_rejects_stale_snapshot_install() {
    let old_engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::default());
    let mut old_state_machine = RegionStateMachine::new(old_engine);
    old_state_machine
        .apply(vec![normal_entry(7, 1, b"a", b"1")])
        .await
        .unwrap();
    let mut old_builder = old_state_machine.get_snapshot_builder().await;
    let old_snapshot = old_builder.build_snapshot().await.unwrap();

    let current_engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::default());
    let mut current_state_machine = RegionStateMachine::new(current_engine);
    current_state_machine
        .apply(vec![
            normal_entry(7, 1, b"a", b"1"),
            normal_entry(7, 2, b"b", b"2"),
        ])
        .await
        .unwrap();

    let err = current_state_machine
        .install_snapshot(&old_snapshot.meta, old_snapshot.snapshot)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("stale region snapshot"));
}

#[tokio::test]
async fn region_state_machine_rejects_corrupt_snapshot_without_mutation() {
    let source_engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::default());
    let mut source_state_machine = RegionStateMachine::new(source_engine);
    source_state_machine
        .apply(vec![normal_entry(7, 1, b"a", b"1")])
        .await
        .unwrap();
    let mut builder = source_state_machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    let restored = AppliedMetadataEngine::new(7, MemoryMetadataStore::default());
    let mut restored_state_machine = RegionStateMachine::new(restored.clone());
    let err = restored_state_machine
        .install_snapshot(
            &snapshot.meta,
            Box::new(std::io::Cursor::new(vec![0xde, 0xad, 0xbe, 0xef])),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed to decode"));
    assert_eq!(restored.status().applied_index, 0);

    let missing = restored
        .execute_metadata_get(&metadatapb::MetadataGetRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            key: b"a".to_vec(),
            version: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(missing.not_found);
}
