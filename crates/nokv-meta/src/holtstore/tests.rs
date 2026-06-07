use super::*;
use crate::command::{
    CommandKind, DelimitedScanItem, DelimitedScanRequest, HistoryPruneRequest, MetadataCommand,
    Mutation, PredicateRef, ScanRequest, Value,
};
use std::sync::{Arc, Barrier};
use std::thread;

fn version(raw: u64) -> Version {
    Version::new(raw).unwrap()
}

fn put_command(key: &[u8], request_id: &[u8], value: &[u8], commit: u64) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::CreateFile,
        read_version: version(commit - 1),
        commit_version: version(commit),
        primary_family: RecordFamily::Dentry,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            predicate: Predicate::NotExists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(value.to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn replace_command(
    key: &[u8],
    request_id: &[u8],
    value: &[u8],
    read: u64,
    commit: u64,
) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::RenameReplace,
        read_version: version(read),
        commit_version: version(commit),
        primary_family: RecordFamily::Dentry,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            predicate: Predicate::Exists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(value.to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn snapshot_pin_command(request_id: &[u8], commit: u64) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::SnapshotSubtree,
        read_version: version(commit - 1),
        commit_version: version(commit),
        primary_family: RecordFamily::Snapshot,
        primary_key: b"snapshot/1".to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::Snapshot,
            key: b"snapshot/1".to_vec(),
            predicate: Predicate::NotExists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::Snapshot,
            key: b"snapshot/1".to_vec(),
            op: MutationOp::Put,
            value: Some(Value(b"pin".to_vec())),
        }],
        watch: Vec::new(),
    }
}

#[test]
fn commit_put_then_get_and_scan() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    let scan = store
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            version: version(2),
            limit: 10,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].key, b"dir/a");
}

#[test]
fn scan_start_after_skips_prior_prefix_keys() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/c", b"req-3", b"value-c", 4))
        .unwrap();

    let before = store.metadata_store_stats();
    let scan = store
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: Some(b"dir/a".to_vec()),
            version: version(4),
            limit: 1,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();

    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].key, b"dir/b");
    let after = store.metadata_store_stats();
    assert_eq!(
        after.scan_key_visited_total - before.scan_key_visited_total,
        1
    );
    assert_eq!(
        after.scan_key_returned_total - before.scan_key_returned_total,
        1
    );
}

#[test]
fn scan_delimited_uses_engine_common_prefix_rollup() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/sub/b", b"req-2", b"value-b", 3))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/sub/c", b"req-3", b"value-c", 4))
        .unwrap();

    let before = store.metadata_store_stats();
    let scan = store
        .scan_delimited(DelimitedScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            delimiter: b'/',
            version: version(4),
            limit: 10,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();

    assert_eq!(
        scan,
        vec![
            DelimitedScanItem::Key(ScanItem {
                key: b"dir/a".to_vec(),
                value: Value(b"value-a".to_vec()),
                version: version(2),
            }),
            DelimitedScanItem::CommonPrefix(b"dir/sub/".to_vec()),
        ]
    );
    let after = store.metadata_store_stats();
    assert_eq!(
        after.scan_key_returned_total - before.scan_key_returned_total,
        2
    );
}

#[test]
fn scan_keys_uses_key_only_range_with_start_after() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/c", b"req-3", b"value-c", 4))
        .unwrap();

    let before = store.metadata_store_stats();
    let keys = store
        .scan_keys(KeyScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: Some(b"dir/a".to_vec()),
            limit: 1,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();

    assert_eq!(keys, vec![b"dir/b".to_vec()]);
    let after = store.metadata_store_stats();
    let visited = after.scan_key_visited_total - before.scan_key_visited_total;
    assert!(
            visited <= 2,
            "bounded key scan should stop at the requested entry or one internal cursor step past it, visited {visited}"
        );
    assert_eq!(
        after.scan_key_returned_total - before.scan_key_returned_total,
        1
    );
}

#[test]
fn repeated_key_scan_records_holt_prefix_cache_hit() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
        .unwrap();

    let request = KeyScanRequest {
        family: RecordFamily::Dentry,
        prefix: b"dir/".to_vec(),
        start_after: None,
        limit: 10,
        purpose: ReadPurpose::UserStrong,
    };
    assert_eq!(
        store.scan_keys(request.clone()).unwrap(),
        vec![b"dir/a".to_vec(), b"dir/b".to_vec()]
    );
    let before = store.metadata_store_stats();
    assert_eq!(
        store.scan_keys(request).unwrap(),
        vec![b"dir/a".to_vec(), b"dir/b".to_vec()]
    );
    let after = store.metadata_store_stats();
    assert_eq!(after.scan_cache_hit_total - before.scan_cache_hit_total, 1);
}

#[test]
fn predicate_failure_does_not_apply_any_mutation() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    let failed = store.commit_metadata(put_command(b"dir/a", b"req-2", b"value-b", 3));
    assert_eq!(failed, Err(MetadataError::PredicateFailed));
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
}

#[test]
fn independent_batch_commits_disjoint_commands() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let results = store.commit_independent_batch(&[
        put_command(b"dir/a", b"req-1", b"value-a", 2),
        put_command(b"dir/b", b"req-2", b"value-b", 3),
    ]);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().commit_version, version(2));
    assert_eq!(results[1].as_ref().unwrap().commit_version, version(3));
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/b",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    let stats = store.metadata_store_stats();
    assert_eq!(stats.commit_total, 2);
    assert_eq!(stats.atomic_apply_total, 1);
    assert_eq!(stats.atomic_apply_command_total, 2);
    assert_eq!(stats.atomic_apply_max_batch, 2);
}

#[test]
fn independent_batch_preserves_conflict_result_boundary() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let results = store.commit_independent_batch(&[
        put_command(b"dir/a", b"req-1", b"value-a", 2),
        put_command(b"dir/a", b"req-2", b"value-b", 3),
        put_command(b"dir/b", b"req-3", b"value-c", 4),
    ]);

    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert_eq!(results[1], Err(MetadataError::PredicateFailed));
    assert!(results[2].is_ok());
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/b",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-c".to_vec()))
    );
    let stats = store.metadata_store_stats();
    assert_eq!(stats.commit_total, 2);
    assert_eq!(stats.atomic_apply_total, 2);
    assert_eq!(stats.atomic_apply_command_total, 2);
    assert_eq!(stats.atomic_apply_max_batch, 1);
}

#[test]
fn independent_batch_isolates_snapshot_retention_changes() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    let results = store.commit_independent_batch(&[
        snapshot_pin_command(b"snapshot-1", 3),
        replace_command(b"dir/a", b"req-2", b"value-b", 2, 4),
    ]);

    assert!(results.iter().all(Result::is_ok));
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    assert!(store.metadata_store_stats().history_write_total > 0);
}

#[test]
fn checkpoint_image_round_trips_current_history_and_dedupe() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    let replace = store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
        .unwrap();

    let image = store.export_checkpoint_image().unwrap();
    let restored = HoltMetadataStore::open_memory().unwrap();
    restored.install_checkpoint_image(&image).unwrap();

    assert_eq!(
        restored
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    assert_eq!(
        restored
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        restored.committed_request_result(b"req-2").unwrap(),
        Some(replace)
    );
    assert_eq!(restored.metadata_store_stats().active_snapshot_pin_total, 1);
}

#[test]
fn storage_reclaim_is_idempotent_after_checkpoint() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    store.checkpoint().unwrap();
    store.reclaim_unreachable_storage().unwrap();
    store.reclaim_unreachable_storage().unwrap();

    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
}

#[test]
fn checkpoint_image_rejects_malformed_bytes() {
    let store = HoltMetadataStore::open_memory().unwrap();
    assert!(store.install_checkpoint_image(b"not-a-checkpoint").is_err());

    let mut image = store.export_checkpoint_image().unwrap();
    image.push(1);
    assert!(store.install_checkpoint_image(&image).is_err());
}

#[test]
fn deleted_key_is_hidden_latest_but_visible_to_old_version() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(MetadataCommand {
            request_id: b"req-delete".to_vec(),
            kind: CommandKind::RemoveFile,
            read_version: version(3),
            commit_version: version(4),
            primary_family: RecordFamily::Dentry,
            primary_key: b"dir/a".to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                op: MutationOp::Delete,
                value: None,
            }],
            watch: Vec::new(),
        })
        .unwrap();

    let before_latest = store.metadata_store_stats();
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        None
    );
    let after_latest = store.metadata_store_stats();
    assert_eq!(
        after_latest.history_lookup_total - before_latest.history_lookup_total,
        0,
        "live current-missing reads should not scan history"
    );
    let before_snapshot = store.metadata_store_stats();
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    let after_snapshot = store.metadata_store_stats();
    assert_eq!(
        after_snapshot.history_lookup_total - before_snapshot.history_lookup_total,
        1,
        "snapshot reads must retain historical visibility"
    );
}

#[test]
fn not_exists_allows_recreate_after_tombstone() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(MetadataCommand {
            request_id: b"req-delete".to_vec(),
            kind: CommandKind::RemoveFile,
            read_version: version(2),
            commit_version: version(3),
            primary_family: RecordFamily::Dentry,
            primary_key: b"dir/a".to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                op: MutationOp::Delete,
                value: None,
            }],
            watch: Vec::new(),
        })
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-2", b"value-b", 4))
        .unwrap();

    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
}

#[test]
fn prefix_empty_predicate_uses_family_prefix() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    let mut command = put_command(b"dir", b"req-2", b"directory", 3);
    command.predicates = vec![PredicateRef {
        family: RecordFamily::Dentry,
        key: b"dir/".to_vec(),
        predicate: Predicate::PrefixEmpty,
    }];
    assert_eq!(
        store.commit_metadata(command),
        Err(MetadataError::PredicateFailed)
    );
}

#[test]
fn duplicate_request_id_returns_original_result() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let first = store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    let duplicate = store
        .commit_metadata(put_command(b"dir/b", b"req-1", b"value-b", 3))
        .unwrap();
    assert_eq!(duplicate, first);
    assert!(store
        .get(
            RecordFamily::Dentry,
            b"dir/b",
            version(3),
            ReadPurpose::UserStrong
        )
        .unwrap()
        .is_none());
}

#[test]
fn concurrent_duplicate_request_id_commits_once() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let left_store = store.clone();
    let left_barrier = Arc::clone(&barrier);
    let left = thread::spawn(move || {
        left_barrier.wait();
        left_store.commit_metadata(put_command(b"dir/a", b"req-shared", b"value-a", 2))
    });
    let right_store = store.clone();
    let right = thread::spawn(move || {
        barrier.wait();
        right_store.commit_metadata(put_command(b"dir/b", b"req-shared", b"value-b", 3))
    });

    let left = left.join().unwrap().unwrap();
    let right = right.join().unwrap().unwrap();
    assert_eq!(left, right);

    let a = store
        .get(
            RecordFamily::Dentry,
            b"dir/a",
            version(3),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    let b = store
        .get(
            RecordFamily::Dentry,
            b"dir/b",
            version(3),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    assert_ne!(a.is_some(), b.is_some());
}

#[test]
fn concurrent_not_exists_commits_one_writer() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let left_store = store.clone();
    let left_barrier = Arc::clone(&barrier);
    let left = thread::spawn(move || {
        left_barrier.wait();
        left_store.commit_metadata(put_command(b"dir/a", b"req-left", b"value-a", 2))
    });
    let right_store = store.clone();
    let right = thread::spawn(move || {
        barrier.wait();
        right_store.commit_metadata(put_command(b"dir/a", b"req-right", b"value-b", 3))
    });

    let outcomes = [left.join().unwrap(), right.join().unwrap()];
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(MetadataError::PredicateFailed)))
            .count(),
        1
    );
    assert!(store
        .get(
            RecordFamily::Dentry,
            b"dir/a",
            version(3),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());
}

#[test]
fn hot_path_skips_history_without_snapshot_retention() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 3))
        .unwrap();

    assert_eq!(store.metadata_store_stats().history_write_total, 0);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        None
    );
    let outcome = store
        .prune_history(HistoryPruneRequest {
            retain_from: None,
            limit: 100,
        })
        .unwrap();
    assert_eq!(outcome.removed, 0);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
}

#[test]
fn prune_history_keeps_snapshot_floor_anchor_per_key() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-3", b"value-c", 4, 5))
        .unwrap();

    assert_eq!(store.metadata_store_stats().history_write_total, 2);

    let outcome = store
        .prune_history(HistoryPruneRequest {
            retain_from: Some(version(5)),
            limit: 100,
        })
        .unwrap();
    assert_eq!(outcome.scanned, 2);
    assert_eq!(outcome.removed, 1);
    assert_eq!(outcome.retained_by_snapshots, 1);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        None
    );
}
