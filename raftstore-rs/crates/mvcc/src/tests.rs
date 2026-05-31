use super::*;
use nokv_proto::nokv::kv::v1 as kvpb;

fn assert_abort_contains(error: Option<kvpb::KeyError>, needle: &str) {
    let err = error.expect("expected key error");
    assert!(
        err.abort.contains(needle),
        "expected abort containing {needle:?}, got {err:?}"
    );
}

#[test]
fn prewrite_commit_makes_value_visible_at_commit_ts() {
    let store = MvccStore::new();
    store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"k".to_vec(),
            start_version: 10,
            lock_ttl: 30_000,
            ..Default::default()
        })
        .unwrap();
    store
        .commit(&kvpb::CommitRequest {
            keys: vec![b"k".to_vec()],
            start_version: 10,
            commit_version: 20,
        })
        .unwrap();

    let old = store
        .get(&kvpb::GetRequest {
            key: b"k".to_vec(),
            version: 19,
        })
        .unwrap();
    assert!(old.not_found);

    let current = store
        .get(&kvpb::GetRequest {
            key: b"k".to_vec(),
            version: 20,
        })
        .unwrap();
    assert_eq!(current.value, b"v1");
}

#[test]
fn empty_key_txn_requests_abort_without_partial_apply() {
    let store = MvccStore::new();

    let prewrite = store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![
                kvpb::Mutation {
                    key: b"prewrite-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                },
                kvpb::Mutation {
                    key: Vec::new(),
                    value: b"bad".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                },
            ],
            primary_lock: b"prewrite-valid".to_vec(),
            start_version: 10,
            lock_ttl: 30_000,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(prewrite.errors.len(), 1);
    assert!(prewrite.errors[0].abort.contains("empty key in mutation"));
    let missing_lock = store
        .commit(&kvpb::CommitRequest {
            keys: vec![b"prewrite-valid".to_vec()],
            start_version: 10,
            commit_version: 20,
        })
        .unwrap();
    assert!(missing_lock
        .error
        .unwrap()
        .retryable
        .contains("lock not found"));

    store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: b"commit-valid".to_vec(),
                value: b"v".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"commit-valid".to_vec(),
            start_version: 30,
            lock_ttl: 30_000,
            ..Default::default()
        })
        .unwrap();
    let commit = store
        .commit(&kvpb::CommitRequest {
            keys: vec![b"commit-valid".to_vec(), Vec::new()],
            start_version: 30,
            commit_version: 40,
        })
        .unwrap();
    assert_abort_contains(commit.error, "empty key in commit");
    let not_committed = store
        .get(&kvpb::GetRequest {
            key: b"commit-valid".to_vec(),
            version: 40,
        })
        .unwrap();
    assert!(not_committed.error.unwrap().locked.is_some());

    store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: b"rollback-valid".to_vec(),
                value: b"v".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"rollback-valid".to_vec(),
            start_version: 50,
            lock_ttl: 30_000,
            ..Default::default()
        })
        .unwrap();
    let rollback = store
        .batch_rollback(&kvpb::BatchRollbackRequest {
            keys: vec![b"rollback-valid".to_vec(), Vec::new()],
            start_version: 50,
        })
        .unwrap();
    assert_abort_contains(rollback.error, "empty key in rollback");
    let still_committable = store
        .commit(&kvpb::CommitRequest {
            keys: vec![b"rollback-valid".to_vec()],
            start_version: 50,
            commit_version: 60,
        })
        .unwrap();
    assert!(still_committable.error.is_none());
}

#[test]
fn empty_key_atomic_mutate_aborts_without_partial_apply() {
    let store = MvccStore::new();

    let empty_predicate = store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            predicates: vec![kvpb::AtomicPredicate {
                key: Vec::new(),
                kind: kvpb::AtomicPredicateKind::NotExists as i32,
                read_version: 1,
                ..Default::default()
            }],
            mutations: vec![kvpb::Mutation {
                key: b"predicate-valid".to_vec(),
                value: b"v".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 2,
        })
        .unwrap();
    assert_abort_contains(empty_predicate.error, "empty key in mutation");
    assert!(
        store
            .get(&kvpb::GetRequest {
                key: b"predicate-valid".to_vec(),
                version: 2,
            })
            .unwrap()
            .not_found
    );

    let empty_mutation = store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![
                kvpb::Mutation {
                    key: b"mutation-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                },
                kvpb::Mutation {
                    key: Vec::new(),
                    value: b"bad".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                },
            ],
            predicates: Vec::new(),
            start_version: 3,
            commit_version: 4,
        })
        .unwrap();
    assert_abort_contains(empty_mutation.error, "empty key in mutation");
    assert!(
        store
            .get(&kvpb::GetRequest {
                key: b"mutation-valid".to_vec(),
                version: 4,
            })
            .unwrap()
            .not_found
    );
}

#[test]
fn atomic_mutate_rejects_invalid_commit_version_before_apply() {
    let store = MvccStore::new();

    let result = store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"invalid-commit-version".to_vec(),
                value: b"bad".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 10,
            commit_version: 10,
            ..Default::default()
        })
        .unwrap();
    assert_abort_contains(result.error, "greater than start version");
    assert!(
        store
            .get(&kvpb::GetRequest {
                key: b"invalid-commit-version".to_vec(),
                version: 11,
            })
            .unwrap()
            .not_found
    );
}

#[test]
fn scan_reports_read_version_and_skips_marker_writes() {
    let store = MvccStore::new();
    store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 10,
            ..Default::default()
        })
        .unwrap();
    store
        .batch_rollback(&kvpb::BatchRollbackRequest {
            keys: vec![b"k".to_vec()],
            start_version: 20,
        })
        .unwrap();

    let scan = store
        .scan(&kvpb::ScanRequest {
            start_key: b"k".to_vec(),
            limit: 1,
            version: 30,
            include_start: true,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(scan.kvs.len(), 1);
    assert_eq!(scan.kvs[0].key, b"k");
    assert_eq!(scan.kvs[0].value, b"v1");
    assert_eq!(scan.kvs[0].version, 30);

    let latest = store
        .scan(&kvpb::ScanRequest {
            start_key: b"k".to_vec(),
            limit: 1,
            include_start: true,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(latest.kvs.len(), 1);
    assert_eq!(latest.kvs[0].version, u64::MAX);
}

#[test]
fn scan_zero_limit_defaults_to_one_key() {
    let store = MvccStore::new();
    store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![
                kvpb::Mutation {
                    key: b"a".to_vec(),
                    value: b"va".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                },
                kvpb::Mutation {
                    key: b"b".to_vec(),
                    value: b"vb".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                },
            ],
            start_version: 1,
            commit_version: 10,
            ..Default::default()
        })
        .unwrap();

    let scan = store
        .scan(&kvpb::ScanRequest {
            version: 20,
            include_start: true,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(scan.kvs.len(), 1);
    assert_eq!(scan.kvs[0].key, b"a");
}

#[test]
fn install_prepared_rejects_malformed_batch_without_partial_apply() {
    let store = MvccStore::new();
    let version_mismatch = store
        .install_prepared(&kvpb::InstallPreparedMvccEntriesRequest {
            routing_key: b"prepared-a".to_vec(),
            commit_version: 44,
            entries: vec![
                kvpb::PreparedMvccEntry {
                    column_family: kvpb::prepared_mvcc_entry::ColumnFamily::Default as i32,
                    key: b"prepared-a".to_vec(),
                    version: 44,
                    value: b"value".to_vec(),
                    has_value: true,
                    ..Default::default()
                },
                kvpb::PreparedMvccEntry {
                    column_family: kvpb::prepared_mvcc_entry::ColumnFamily::Default as i32,
                    key: b"prepared-b".to_vec(),
                    version: 45,
                    value: b"must-not-apply".to_vec(),
                    has_value: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .unwrap();
    assert_abort_contains(version_mismatch.error, "version");
    assert!(
        store
            .get(&kvpb::GetRequest {
                key: b"prepared-a".to_vec(),
                version: 44,
            })
            .unwrap()
            .not_found
    );

    let invalid_cf = store
        .install_prepared(&kvpb::InstallPreparedMvccEntriesRequest {
            routing_key: b"prepared-c".to_vec(),
            commit_version: 50,
            entries: vec![
                kvpb::PreparedMvccEntry {
                    column_family: kvpb::prepared_mvcc_entry::ColumnFamily::Default as i32,
                    key: b"prepared-c".to_vec(),
                    version: 50,
                    value: b"value".to_vec(),
                    has_value: true,
                    ..Default::default()
                },
                kvpb::PreparedMvccEntry {
                    column_family: 99,
                    key: b"prepared-d".to_vec(),
                    version: 50,
                    value: b"must-not-apply".to_vec(),
                    has_value: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .unwrap();
    assert_abort_contains(invalid_cf.error, "column family");
    assert!(
        store
            .get(&kvpb::GetRequest {
                key: b"prepared-c".to_vec(),
                version: 50,
            })
            .unwrap()
            .not_found
    );
}

#[test]
fn expired_values_are_not_visible_to_get_or_scan() {
    let store = MvccStore::new();
    store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"expired".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                expires_at: 1,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 10,
            ..Default::default()
        })
        .unwrap();

    let got = store
        .get(&kvpb::GetRequest {
            key: b"k".to_vec(),
            version: 20,
        })
        .unwrap();
    assert!(got.not_found);

    let scan = store
        .scan(&kvpb::ScanRequest {
            start_key: b"k".to_vec(),
            limit: 1,
            version: 20,
            include_start: true,
            ..Default::default()
        })
        .unwrap();
    assert!(scan.kvs.is_empty());
}

#[test]
fn atomic_not_exists_rejects_existing_key() {
    let store = MvccStore::new();
    let first = store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            predicates: vec![kvpb::AtomicPredicate {
                key: b"k".to_vec(),
                kind: kvpb::AtomicPredicateKind::NotExists as i32,
                read_version: 1,
                ..Default::default()
            }],
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 2,
        })
        .unwrap();
    assert_eq!(first.applied_keys, 1);

    let second = store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            predicates: vec![kvpb::AtomicPredicate {
                key: b"k".to_vec(),
                kind: kvpb::AtomicPredicateKind::NotExists as i32,
                read_version: 2,
                ..Default::default()
            }],
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 3,
            commit_version: 4,
        })
        .unwrap();
    assert!(second.error.unwrap().already_exists.is_some());
}

#[test]
fn atomic_mutate_matches_go_validation_and_idempotency() {
    let store = MvccStore::new();
    let request = kvpb::TryAtomicMutateRequest {
        predicates: vec![kvpb::AtomicPredicate {
            key: b"atomic-idempotent".to_vec(),
            kind: kvpb::AtomicPredicateKind::NotExists as i32,
            ..Default::default()
        }],
        mutations: vec![kvpb::Mutation {
            key: b"atomic-idempotent".to_vec(),
            value: b"v1".to_vec(),
            op: kvpb::mutation::Op::Put as i32,
            ..Default::default()
        }],
        start_version: 10,
        commit_version: 11,
    };
    let first = store.try_atomic_mutate(&request).unwrap();
    assert_eq!(first.applied_keys, 1);
    let retry = store.try_atomic_mutate(&request).unwrap();
    assert_eq!(retry.applied_keys, 1);
    assert!(retry.error.is_none());

    let mismatch = store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            predicates: vec![kvpb::AtomicPredicate {
                key: b"atomic-idempotent".to_vec(),
                kind: kvpb::AtomicPredicateKind::ValueEquals as i32,
                expected_value: b"old".to_vec(),
                read_version: 11,
            }],
            mutations: vec![kvpb::Mutation {
                key: b"atomic-idempotent".to_vec(),
                value: b"bad".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 12,
            commit_version: 13,
        })
        .unwrap();
    assert!(mismatch
        .error
        .unwrap()
        .retryable
        .contains("atomic predicate mismatch"));

    let unsupported = store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"atomic-lock".to_vec(),
                value: b"bad".to_vec(),
                op: kvpb::mutation::Op::Lock as i32,
                ..Default::default()
            }],
            start_version: 14,
            commit_version: 15,
            ..Default::default()
        })
        .unwrap();
    assert_abort_contains(unsupported.error, "unsupported mutation op");
}

#[test]
fn resolve_lock_matches_go_key_set_boundary() {
    let store = MvccStore::new();
    let key = b"resolve-key-boundary".to_vec();
    let prewrite = store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: key.clone(),
                value: b"resolve-value".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: key.clone(),
            start_version: 40,
            lock_ttl: 10_000,
            ..Default::default()
        })
        .unwrap();
    assert!(prewrite.errors.is_empty());

    let empty = store
        .resolve_lock(&kvpb::ResolveLockRequest {
            start_version: 40,
            commit_version: 50,
            ..Default::default()
        })
        .unwrap();
    assert!(empty.error.is_none());
    assert_eq!(empty.resolved_locks, 0);

    let duplicate = store
        .resolve_lock(&kvpb::ResolveLockRequest {
            keys: vec![Vec::new(), key.clone(), key.clone()],
            start_version: 40,
            commit_version: 50,
        })
        .unwrap();
    assert!(duplicate.error.is_none());
    assert_eq!(duplicate.resolved_locks, 1);

    let retry = store
        .resolve_lock(&kvpb::ResolveLockRequest {
            keys: vec![key.clone()],
            start_version: 40,
            commit_version: 50,
        })
        .unwrap();
    assert!(retry.error.is_none());
    assert_eq!(retry.resolved_locks, 0);

    let got = store
        .get(&kvpb::GetRequest {
            key: key.clone(),
            version: 60,
        })
        .unwrap();
    assert_eq!(got.value, b"resolve-value");
}

#[test]
fn resolve_lock_commit_matches_go_lingering_lock_boundary() {
    let store = MvccStore::new();
    let key = b"resolve-lingering-lock".to_vec();
    store
        .install_snapshot(MvccSnapshot {
            writes: vec![MvccSnapshotWrite {
                key: key.clone(),
                commit_version: 50,
                value: VersionedValue {
                    kind: kvpb::mutation::Op::Put,
                    start_version: 45,
                    value: Some(b"committed".to_vec()),
                    expires_at: 0,
                },
            }],
            locks: vec![MvccSnapshotLock {
                key: key.clone(),
                lock: LockRecord {
                    primary: key.clone(),
                    start_version: 45,
                    start_time: 1,
                    ttl: 10_000,
                    min_commit_ts: 0,
                    op: kvpb::mutation::Op::Put,
                    value: b"stale-lock".to_vec(),
                    expires_at: 0,
                },
            }],
            rollbacks: Vec::new(),
        })
        .unwrap();

    let resolved = store
        .resolve_lock(&kvpb::ResolveLockRequest {
            keys: vec![key.clone()],
            start_version: 45,
            commit_version: 60,
        })
        .unwrap();
    assert!(resolved.error.is_none());
    assert_eq!(resolved.resolved_locks, 1);
    let snapshot = store.export_snapshot().unwrap();
    assert!(snapshot.locks.is_empty());
    assert!(snapshot
        .writes
        .iter()
        .all(|write| write.commit_version != 60));
    let got = store.get(&kvpb::GetRequest { key, version: 70 }).unwrap();
    assert_eq!(got.value, b"committed");
}

#[test]
fn prewrite_rejects_unsupported_ops_without_partial_apply() {
    let store = MvccStore::new();
    let valid_key = b"prewrite-valid-before-unsupported".to_vec();
    let invalid_key = b"prewrite-unsupported".to_vec();
    let response = store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![
                kvpb::Mutation {
                    key: valid_key.clone(),
                    value: b"valid".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                },
                kvpb::Mutation {
                    key: invalid_key,
                    op: kvpb::mutation::Op::Rollback as i32,
                    ..Default::default()
                },
            ],
            primary_lock: valid_key.clone(),
            start_version: 70,
            lock_ttl: 10_000,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(response.errors.len(), 1);
    assert_abort_contains(
        response.errors.into_iter().next(),
        "unsupported mutation op",
    );

    let got = store
        .get(&kvpb::GetRequest {
            key: valid_key,
            version: 80,
        })
        .unwrap();
    assert!(got.not_found);
}

#[test]
fn prewrite_write_conflict_matches_go_fields_and_rollback_fence() {
    let store = MvccStore::new();
    let key = b"prewrite-conflict-fields".to_vec();
    assert!(store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: key.clone(),
                value: b"old".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: key.clone(),
            start_version: 10,
            lock_ttl: 10_000,
            ..Default::default()
        })
        .unwrap()
        .errors
        .is_empty());
    assert!(store
        .commit(&kvpb::CommitRequest {
            keys: vec![key.clone()],
            start_version: 10,
            commit_version: 20,
        })
        .unwrap()
        .error
        .is_none());

    let conflict = store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: key.clone(),
                value: b"new".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: key,
            start_version: 15,
            lock_ttl: 10_000,
            ..Default::default()
        })
        .unwrap();
    let conflict = conflict.errors[0].write_conflict.as_ref().unwrap();
    assert_eq!(conflict.conflict_ts, 20);
    assert_eq!(conflict.start_ts, 10);
    assert_eq!(conflict.commit_ts, 15);

    let rollback_key = b"prewrite-rollback-fence".to_vec();
    assert!(store
        .batch_rollback(&kvpb::BatchRollbackRequest {
            keys: vec![rollback_key.clone()],
            start_version: 30,
        })
        .unwrap()
        .error
        .is_none());
    let fenced = store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: rollback_key,
                value: b"new".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"prewrite-rollback-fence".to_vec(),
            start_version: 30,
            lock_ttl: 10_000,
            ..Default::default()
        })
        .unwrap();
    let fenced = fenced.errors[0].write_conflict.as_ref().unwrap();
    assert_eq!(fenced.conflict_ts, 30);
    assert_eq!(fenced.start_ts, 30);
    assert_eq!(fenced.commit_ts, 30);
}

#[test]
fn commit_matches_go_missing_lock_and_lingering_lock_boundaries() {
    let store = MvccStore::new();
    let missing = store
        .commit(&kvpb::CommitRequest {
            keys: vec![b"commit-missing-lock".to_vec()],
            start_version: 10,
            commit_version: 20,
        })
        .unwrap();
    assert!(missing.error.unwrap().retryable.contains("lock not found"));

    let rollback_key = b"commit-rolled-back".to_vec();
    assert!(store
        .batch_rollback(&kvpb::BatchRollbackRequest {
            keys: vec![rollback_key.clone()],
            start_version: 30,
        })
        .unwrap()
        .error
        .is_none());
    let rolled_back = store
        .commit(&kvpb::CommitRequest {
            keys: vec![rollback_key],
            start_version: 30,
            commit_version: 40,
        })
        .unwrap();
    assert!(rolled_back
        .error
        .unwrap()
        .retryable
        .contains("transaction already rolled back"));

    let lingering = b"commit-lingering-lock".to_vec();
    store
        .install_snapshot(MvccSnapshot {
            writes: vec![MvccSnapshotWrite {
                key: lingering.clone(),
                commit_version: 50,
                value: VersionedValue {
                    kind: kvpb::mutation::Op::Put,
                    start_version: 45,
                    value: Some(b"committed".to_vec()),
                    expires_at: 0,
                },
            }],
            locks: vec![MvccSnapshotLock {
                key: lingering.clone(),
                lock: LockRecord {
                    primary: lingering.clone(),
                    start_version: 45,
                    start_time: 1,
                    ttl: 10_000,
                    min_commit_ts: 0,
                    op: kvpb::mutation::Op::Put,
                    value: b"stale-lock".to_vec(),
                    expires_at: 0,
                },
            }],
            rollbacks: Vec::new(),
        })
        .unwrap();
    assert!(store
        .commit(&kvpb::CommitRequest {
            keys: vec![lingering.clone()],
            start_version: 45,
            commit_version: 60,
        })
        .unwrap()
        .error
        .is_none());
    let snapshot = store.export_snapshot().unwrap();
    assert!(snapshot.locks.is_empty());
    assert!(snapshot
        .writes
        .iter()
        .all(|write| write.commit_version != 60));
    let got = store
        .get(&kvpb::GetRequest {
            key: lingering,
            version: 70,
        })
        .unwrap();
    assert_eq!(got.value, b"committed");
}

#[test]
fn mvcc_maintenance_counts_requested_tombstones() {
    let store = MvccStore::new();
    store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"maint/k".to_vec(),
                value: b"value".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 2,
            ..Default::default()
        })
        .unwrap();

    let response = store
        .mvcc_maintenance(&kvpb::MvccMaintenanceRequest {
            tombstones: vec![
                kvpb::InternalEntryTombstone {
                    column_family: kvpb::internal_entry_tombstone::ColumnFamily::Write as i32,
                    key: b"maint/k".to_vec(),
                    version: 2,
                },
                kvpb::InternalEntryTombstone {
                    column_family: kvpb::internal_entry_tombstone::ColumnFamily::Default as i32,
                    key: b"maint/missing".to_vec(),
                    version: 9,
                },
            ],
        })
        .unwrap();
    assert_eq!(response.applied_entries, 2);
    assert!(response.error.is_none());

    let got = store
        .get(&kvpb::GetRequest {
            key: b"maint/k".to_vec(),
            version: 2,
        })
        .unwrap();
    assert!(got.not_found);
}

#[test]
fn mvcc_maintenance_rejects_malformed_batch_without_partial_apply() {
    let store = MvccStore::new();
    store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"maint/keep".to_vec(),
                value: b"value".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 2,
            ..Default::default()
        })
        .unwrap();

    let response = store
        .mvcc_maintenance(&kvpb::MvccMaintenanceRequest {
            tombstones: vec![
                kvpb::InternalEntryTombstone {
                    column_family: 99,
                    key: b"maint/bad".to_vec(),
                    version: 1,
                },
                kvpb::InternalEntryTombstone {
                    column_family: kvpb::internal_entry_tombstone::ColumnFamily::Write as i32,
                    key: b"maint/keep".to_vec(),
                    version: 2,
                },
            ],
        })
        .unwrap();
    assert!(response.error.unwrap().abort.contains("column family"));

    let got = store
        .get(&kvpb::GetRequest {
            key: b"maint/keep".to_vec(),
            version: 2,
        })
        .unwrap();
    assert_eq!(got.value, b"value");

    let empty_key = store
        .mvcc_maintenance(&kvpb::MvccMaintenanceRequest {
            tombstones: vec![kvpb::InternalEntryTombstone {
                column_family: kvpb::internal_entry_tombstone::ColumnFamily::Write as i32,
                key: Vec::new(),
                version: 2,
            }],
        })
        .unwrap();
    assert!(empty_key.error.unwrap().abort.contains("empty key"));
}

#[test]
fn rollback_marker_does_not_hide_older_visible_put() {
    let store = MvccStore::new();
    store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 10,
            ..Default::default()
        })
        .unwrap();
    store
        .batch_rollback(&kvpb::BatchRollbackRequest {
            keys: vec![b"k".to_vec()],
            start_version: 20,
        })
        .unwrap();

    let current = store
        .get(&kvpb::GetRequest {
            key: b"k".to_vec(),
            version: 20,
        })
        .unwrap();
    assert!(!current.not_found);
    assert_eq!(current.value, b"v1");
}

#[test]
fn snapshot_round_trips_writes_locks_and_rollbacks() {
    let store = MvccStore::new();
    store
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 1,
            commit_version: 10,
            ..Default::default()
        })
        .unwrap();
    store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: b"locked".to_vec(),
                value: b"pending".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"locked".to_vec(),
            start_version: 20,
            lock_ttl: 30_000,
            ..Default::default()
        })
        .unwrap();
    store
        .batch_rollback(&kvpb::BatchRollbackRequest {
            keys: vec![b"rolled-back".to_vec()],
            start_version: 30,
        })
        .unwrap();

    let encoded = encode_mvcc_snapshot(&store.export_snapshot().unwrap());
    let restored = MvccStore::new();
    restored
        .install_snapshot(decode_mvcc_snapshot(&encoded).unwrap())
        .unwrap();

    let committed = restored
        .get(&kvpb::GetRequest {
            key: b"k".to_vec(),
            version: 10,
        })
        .unwrap();
    assert_eq!(committed.value, b"v1");

    let locked = restored
        .get(&kvpb::GetRequest {
            key: b"locked".to_vec(),
            version: 20,
        })
        .unwrap();
    assert!(locked.error.unwrap().locked.is_some());

    let rolled_back = restored
        .commit(&kvpb::CommitRequest {
            keys: vec![b"rolled-back".to_vec()],
            start_version: 30,
            commit_version: 40,
        })
        .unwrap();
    assert!(rolled_back.error.unwrap().retryable.contains("rolled back"));
}

#[test]
fn commit_after_rollback_is_rejected() {
    let store = MvccStore::new();
    store
        .batch_rollback(&kvpb::BatchRollbackRequest {
            keys: vec![b"k".to_vec()],
            start_version: 10,
        })
        .unwrap();

    let committed = store
        .commit(&kvpb::CommitRequest {
            keys: vec![b"k".to_vec()],
            start_version: 10,
            commit_version: 20,
        })
        .unwrap();
    assert!(committed.error.unwrap().retryable.contains("rolled back"));
}

#[test]
fn check_txn_status_ttl_expire_rolls_back_primary() {
    let store = MvccStore::new();
    store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"k".to_vec(),
            start_version: 10,
            lock_ttl: 1,
            ..Default::default()
        })
        .unwrap();

    let status = store
        .check_txn_status(&kvpb::CheckTxnStatusRequest {
            primary_key: b"k".to_vec(),
            lock_ts: 10,
            current_time: u64::MAX,
            rollback_if_not_exist: true,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        status.action,
        kvpb::CheckTxnStatusAction::CheckTxnStatusTtlExpireRollback as i32
    );

    let committed = store
        .commit(&kvpb::CommitRequest {
            keys: vec![b"k".to_vec()],
            start_version: 10,
            commit_version: 20,
        })
        .unwrap();
    assert!(committed.error.unwrap().retryable.contains("rolled back"));
}

#[test]
fn check_txn_status_empty_primary_rollback_aborts_without_marker() {
    let store = MvccStore::new();
    let status = store
        .check_txn_status(&kvpb::CheckTxnStatusRequest {
            primary_key: Vec::new(),
            lock_ts: 10,
            current_time: 1,
            rollback_if_not_exist: true,
            ..Default::default()
        })
        .unwrap();
    assert_abort_contains(status.error, "empty key in rollback");

    let snapshot = store.export_mvcc_snapshot().unwrap();
    assert!(snapshot
        .rollbacks
        .iter()
        .all(|rollback| !rollback.key.is_empty()));
}

#[test]
fn txn_heartbeat_extends_ttl_and_reports_expiry() {
    let store = MvccStore::new();
    store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"k".to_vec(),
            start_version: 10,
            lock_ttl: 10,
            ..Default::default()
        })
        .unwrap();

    let heartbeat = store
        .txn_heartbeat(&kvpb::TxnHeartBeatRequest {
            primary_key: b"k".to_vec(),
            start_version: 10,
            ttl_extension: 100,
            current_time: current_physical_time_millis(),
        })
        .unwrap();
    assert!(heartbeat.lock_ttl >= 100);
    assert!(heartbeat.lock_expire_time > 0);
}

#[test]
fn txn_heartbeat_validates_request_like_go_percolator() {
    let store = MvccStore::new();
    let cases = [
        (
            kvpb::TxnHeartBeatRequest {
                primary_key: Vec::new(),
                start_version: 10,
                ttl_extension: 1,
                current_time: 1,
            },
            "heartbeat primary key is required",
        ),
        (
            kvpb::TxnHeartBeatRequest {
                primary_key: b"k".to_vec(),
                start_version: 0,
                ttl_extension: 1,
                current_time: 1,
            },
            "heartbeat start version is required",
        ),
        (
            kvpb::TxnHeartBeatRequest {
                primary_key: b"k".to_vec(),
                start_version: 10,
                ttl_extension: 0,
                current_time: 1,
            },
            "heartbeat ttl extension is required",
        ),
        (
            kvpb::TxnHeartBeatRequest {
                primary_key: b"k".to_vec(),
                start_version: 10,
                ttl_extension: 1,
                current_time: 0,
            },
            "heartbeat current time is required",
        ),
    ];
    for (request, needle) in cases {
        let heartbeat = store.txn_heartbeat(&request).unwrap();
        assert_abort_contains(heartbeat.error, needle);
    }
}

#[test]
fn txn_heartbeat_rejects_secondary_lock_primary_mismatch() {
    let store = MvccStore::new();
    store
        .prewrite(&kvpb::PrewriteRequest {
            mutations: vec![kvpb::Mutation {
                key: b"secondary".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            primary_lock: b"primary".to_vec(),
            start_version: 10,
            lock_ttl: 10,
            ..Default::default()
        })
        .unwrap();

    let heartbeat = store
        .txn_heartbeat(&kvpb::TxnHeartBeatRequest {
            primary_key: b"secondary".to_vec(),
            start_version: 10,
            ttl_extension: 100,
            current_time: current_physical_time_millis(),
        })
        .unwrap();
    assert_abort_contains(heartbeat.error, "primary key does not match lock primary");
}
