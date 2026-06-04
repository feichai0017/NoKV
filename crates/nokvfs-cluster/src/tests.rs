use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;

use super::*;
use nokvfs_meta::command::{
    CommandKind, MetadataCommand, MetadataStore, MetadataStoreStatsProvider, Mutation, MutationOp,
    Predicate, PredicateRef, ReadPurpose, Value, Version,
};
use nokvfs_meta::HoltMetadataStore;
use nokvfs_types::{MountId, RecordFamily};
use tempfile::tempdir;

fn version(raw: u64) -> Version {
    Version::new(raw).unwrap()
}

fn node(raw: u64) -> NodeId {
    NodeId::new(raw).unwrap()
}

fn command(id: &[u8], commit_version: u64) -> MetadataCommand {
    MetadataCommand {
        request_id: id.to_vec(),
        kind: CommandKind::CreateFile,
        read_version: version(commit_version - 1),
        commit_version: version(commit_version),
        primary_family: RecordFamily::Dentry,
        primary_key: id.to_vec(),
        predicates: Vec::new(),
        mutations: vec![Mutation {
            family: RecordFamily::Dentry,
            key: id.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(b"value".to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn not_exists_command(id: &[u8], commit_version: u64) -> MetadataCommand {
    let mut command = command(id, commit_version);
    command.predicates = vec![PredicateRef {
        family: RecordFamily::Dentry,
        key: id.to_vec(),
        predicate: Predicate::NotExists,
    }];
    command
}

#[derive(Default)]
struct RecordingSink {
    applied: Mutex<Vec<DurableReceipt>>,
}

struct MetadataStoreSink<M> {
    store: M,
}

impl MetadataLogSink for RecordingSink {
    fn apply_command(
        &self,
        receipt: DurableReceipt,
        command: MetadataCommand,
    ) -> Result<AppliedMetadataCommand, ReplayError> {
        assert_eq!(receipt.request_id, command.request_id);
        self.applied.lock().unwrap().push(receipt.clone());
        Ok(AppliedMetadataCommand {
            receipt,
            applied_mutations: command.mutations.len(),
            watch_events: command.watch.len(),
        })
    }
}

impl<M> MetadataLogSink for MetadataStoreSink<M>
where
    M: MetadataStore,
{
    fn apply_command(
        &self,
        receipt: DurableReceipt,
        command: MetadataCommand,
    ) -> Result<AppliedMetadataCommand, ReplayError> {
        let result = self
            .store
            .commit_metadata(command)
            .map_err(|err| ReplayError::Apply {
                position: receipt.position,
                batch_position: receipt.batch_position,
                message: err.to_string(),
            })?;
        Ok(AppliedMetadataCommand {
            receipt,
            applied_mutations: result.applied_mutations,
            watch_events: result.watch_events,
        })
    }
}

#[test]
fn append_batch_returns_per_command_receipts() {
    let log = InMemorySharedLog::new();
    let term = LogTerm::new(1).unwrap();
    let mount = MountId::new(7).unwrap();
    let receipts = log
        .append_batch(term, mount, &[command(b"a", 2), command(b"b", 3)])
        .unwrap();

    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].position.index.get(), 1);
    assert_eq!(receipts[0].position.term, term);
    assert_eq!(receipts[0].batch_position, 0);
    assert_eq!(receipts[0].request_id, b"a");
    assert_eq!(receipts[0].commit_version, version(2));
    assert_eq!(receipts[1].batch_position, 1);
    assert_eq!(receipts[1].request_id, b"b");
    assert_eq!(log.committed_index().get(), 1);
}

#[test]
fn read_from_replays_log_entries_in_index_order() {
    let log = InMemorySharedLog::new();
    let mount = MountId::new(1).unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
        .unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"b", 3)])
        .unwrap();

    let entries = log.read_from(LogIndex::new(2).unwrap(), 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].position.index.get(), 2);
    assert_eq!(entries[0].commands[0].request_id, b"b");
}

#[test]
fn compact_through_removes_old_entries_and_rejects_stale_reads() {
    let log = InMemorySharedLog::new();
    let mount = MountId::new(1).unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
        .unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"b", 3)])
        .unwrap();

    log.compact_through(LogIndex::new(1).unwrap()).unwrap();
    assert!(matches!(
        log.read_from(LogIndex::new(1).unwrap(), 10),
        Err(SharedLogError::Compacted { .. })
    ));
    let entries = log.read_from(LogIndex::new(2).unwrap(), 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].commands[0].request_id, b"b");
}

#[test]
fn rejects_empty_batches_and_zero_positions() {
    let log = InMemorySharedLog::new();
    assert_eq!(LogTerm::new(0), Err(SharedLogError::ZeroTerm));
    assert_eq!(LogIndex::new(0), Err(SharedLogError::ZeroIndex));
    assert_eq!(
        log.append_batch(LogTerm::new(1).unwrap(), MountId::new(1).unwrap(), &[]),
        Err(SharedLogError::EmptyBatch)
    );
}

#[test]
fn replay_driver_applies_commands_and_reports_frontier() {
    let log = InMemorySharedLog::new();
    let sink = RecordingSink::default();
    let mount = MountId::new(1).unwrap();
    log.append_batch(
        LogTerm::new(1).unwrap(),
        mount,
        &[command(b"a", 2), command(b"b", 3)],
    )
    .unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"c", 4)])
        .unwrap();

    let outcome = ReplayDriver::new(&log, &sink)
        .replay_from(LogIndex::new(1).unwrap(), 0)
        .unwrap();

    assert_eq!(outcome.entries, 2);
    assert_eq!(outcome.commands, 3);
    assert_eq!(
        outcome.frontier,
        Some(ApplyFrontier {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(2).unwrap()
            },
            commit_version: version(4)
        })
    );
    let applied = sink.applied.lock().unwrap();
    assert_eq!(applied[0].request_id, b"a");
    assert_eq!(applied[1].batch_position, 1);
    assert_eq!(applied[2].position.index.get(), 2);
}

#[test]
fn metadata_group_appends_and_applies_one_batch() {
    let log = InMemorySharedLog::new();
    let sink = RecordingSink::default();
    let mount = MountId::new(1).unwrap();
    let commands = vec![command(b"a", 2), command(b"b", 3)];

    let commit = MetadataGroup::new(&log, &sink, LogTerm::new(1).unwrap(), mount)
        .commit_batch(&commands)
        .unwrap();

    assert_eq!(commit.durable_receipts.len(), 2);
    assert_eq!(commit.applied.len(), 2);
    assert_eq!(commit.applied[1].receipt.batch_position, 1);
    assert_eq!(
        commit.frontier,
        Some(ApplyFrontier {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(1).unwrap(),
            },
            commit_version: version(3),
        })
    );
    assert_eq!(log.committed_index().get(), 1);
}

#[test]
fn replay_into_metadata_store_rebuilds_learner_state() {
    let log = InMemorySharedLog::new();
    let leader = MetadataStoreSink {
        store: HoltMetadataStore::open_memory().unwrap(),
    };
    let learner = MetadataStoreSink {
        store: HoltMetadataStore::open_memory().unwrap(),
    };
    let mount = MountId::new(1).unwrap();
    let commands = vec![command(b"a", 2), command(b"b", 3)];

    MetadataGroup::new(&log, &leader, LogTerm::new(1).unwrap(), mount)
        .commit_batch(&commands)
        .unwrap();

    let outcome = ReplayDriver::new(&log, &learner)
        .replay_from(LogIndex::new(1).unwrap(), 0)
        .unwrap();

    assert_eq!(outcome.commands, 2);
    assert_eq!(
        learner
            .store
            .get(
                RecordFamily::Dentry,
                b"a",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );
    assert_eq!(
        learner
            .store
            .get(
                RecordFamily::Dentry,
                b"b",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );
}

#[test]
fn replay_into_metadata_store_is_idempotent_by_request_id() {
    let log = InMemorySharedLog::new();
    let learner = MetadataStoreSink {
        store: HoltMetadataStore::open_memory().unwrap(),
    };
    let mount = MountId::new(1).unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
        .unwrap();

    ReplayDriver::new(&log, &learner)
        .replay_from(LogIndex::new(1).unwrap(), 0)
        .unwrap();
    ReplayDriver::new(&log, &learner)
        .replay_from(LogIndex::new(1).unwrap(), 0)
        .unwrap();

    assert_eq!(learner.store.metadata_store_stats().dedupe_hit_total, 1);
    assert_eq!(
        learner
            .store
            .get(
                RecordFamily::Dentry,
                b"a",
                version(2),
                ReadPurpose::UserStrong
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );
}

#[test]
fn file_shared_log_reopens_entries_and_replays_into_metadata_store() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata.log");
    let mount = MountId::new(1).unwrap();
    {
        let log = FileSharedLog::open(&path).unwrap();
        log.append_batch(
            LogTerm::new(1).unwrap(),
            mount,
            &[command(b"a", 2), command(b"b", 3)],
        )
        .unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"c", 4)])
            .unwrap();
        assert_eq!(log.committed_index().get(), 2);
    }

    let reopened = FileSharedLog::open(&path).unwrap();
    assert_eq!(reopened.committed_index().get(), 2);
    let entries = reopened.read_from(LogIndex::new(1).unwrap(), 0).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].commands[0].request_id, b"a");
    assert_eq!(entries[0].commands[1].request_id, b"b");
    assert_eq!(entries[1].commands[0].request_id, b"c");

    let learner = MetadataStoreSink {
        store: HoltMetadataStore::open_memory().unwrap(),
    };
    let outcome = ReplayDriver::new(&reopened, &learner)
        .replay_from(LogIndex::new(1).unwrap(), 0)
        .unwrap();
    assert_eq!(outcome.entries, 2);
    assert_eq!(outcome.commands, 3);
    assert_eq!(
        learner
            .store
            .get(
                RecordFamily::Dentry,
                b"c",
                version(4),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );
}

#[test]
fn file_shared_log_persists_compaction_marker_and_continues_indexes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata.log");
    let mount = MountId::new(1).unwrap();
    {
        let log = FileSharedLog::open(&path).unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
            .unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"b", 3)])
            .unwrap();
        log.compact_through(LogIndex::new(1).unwrap()).unwrap();
    }

    let reopened = FileSharedLog::open(&path).unwrap();
    assert_eq!(reopened.committed_index().get(), 2);
    assert!(matches!(
        reopened.read_from(LogIndex::new(1).unwrap(), 10),
        Err(SharedLogError::Compacted { .. })
    ));
    let entries = reopened.read_from(LogIndex::new(2).unwrap(), 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].commands[0].request_id, b"b");

    let receipt = reopened
        .append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"c", 4)])
        .unwrap();
    assert_eq!(receipt[0].position.index.get(), 3);
    assert_eq!(reopened.committed_index().get(), 3);
}

#[test]
fn file_shared_log_truncates_partial_tail_on_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata.log");
    let mount = MountId::new(1).unwrap();
    {
        let log = FileSharedLog::open(&path).unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
            .unwrap();
    }
    {
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"NKFSLG01").unwrap();
        file.write_all(&128_u32.to_be_bytes()).unwrap();
        file.write_all(b"partial").unwrap();
        file.flush().unwrap();
    }

    let reopened = FileSharedLog::open(&path).unwrap();
    assert_eq!(reopened.committed_index().get(), 1);
    let receipt = reopened
        .append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"b", 3)])
        .unwrap();
    assert_eq!(receipt[0].position.index.get(), 2);

    let reopened_again = FileSharedLog::open(&path).unwrap();
    let entries = reopened_again
        .read_from(LogIndex::new(1).unwrap(), 0)
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].commands[0].request_id, b"a");
    assert_eq!(entries[1].commands[0].request_id, b"b");
}

#[test]
fn shared_log_metadata_store_logs_before_applying_command() {
    let log = InMemorySharedLog::new();
    let store = HoltMetadataStore::open_memory().unwrap();
    let mount = MountId::new(1).unwrap();
    let shared = SharedLogMetadataStore::new(store, log, LogTerm::new(1).unwrap(), mount);

    let result = shared.commit_metadata(command(b"a", 2)).unwrap();
    assert_eq!(result.commit_version, version(2));
    assert_eq!(shared.log().committed_index().get(), 1);
    assert_eq!(
        shared.applied_frontier(),
        Some(ApplyFrontier {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(1).unwrap(),
            },
            commit_version: version(2),
        })
    );
    assert_eq!(
        shared
            .inner()
            .get(
                RecordFamily::Dentry,
                b"a",
                version(2),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );
    let entries = shared
        .log()
        .read_from(LogIndex::new(1).unwrap(), 0)
        .unwrap();
    assert_eq!(entries[0].commands[0].request_id, b"a");
}

#[test]
fn shared_log_metadata_store_allows_deduped_retry_after_predicate_changes() {
    let log = InMemorySharedLog::new();
    let store = HoltMetadataStore::open_memory().unwrap();
    let mount = MountId::new(1).unwrap();
    let shared = SharedLogMetadataStore::new(store, log, LogTerm::new(1).unwrap(), mount);

    let first = shared.commit_metadata(not_exists_command(b"a", 2)).unwrap();
    let retry = shared.commit_metadata(not_exists_command(b"a", 2)).unwrap();

    assert_eq!(retry, first);
    assert_eq!(
        shared.applied_frontier(),
        Some(ApplyFrontier {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(2).unwrap(),
            },
            commit_version: version(2),
        })
    );
}

#[test]
fn shared_log_metadata_store_persists_file_applied_frontier() {
    let dir = tempdir().unwrap();
    let frontier_path = dir.path().join("applied.frontier");
    let log = InMemorySharedLog::new();
    let store = HoltMetadataStore::open_memory().unwrap();
    let frontier = FileAppliedFrontierStore::open(&frontier_path).unwrap();
    let mount = MountId::new(1).unwrap();
    let shared = SharedLogMetadataStore::with_frontier_store(
        store,
        log,
        LogTerm::new(1).unwrap(),
        mount,
        frontier,
    )
    .unwrap();

    shared.commit_metadata(command(b"a", 2)).unwrap();
    assert_eq!(
        FileAppliedFrontierStore::open(&frontier_path)
            .unwrap()
            .load()
            .unwrap(),
        Some(ApplyFrontier {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(1).unwrap(),
            },
            commit_version: version(2),
        })
    );
}

#[test]
fn shared_log_metadata_store_replays_tail_after_file_frontier() {
    let dir = tempdir().unwrap();
    let meta_path = dir.path().join("meta");
    let log_path = dir.path().join("metadata.log");
    let frontier_path = dir.path().join("metadata.log.apply");
    let mount = MountId::new(1).unwrap();
    {
        let log = FileSharedLog::open(&log_path).unwrap();
        let store = HoltMetadataStore::open_file(&meta_path).unwrap();
        let frontier = FileAppliedFrontierStore::open(&frontier_path).unwrap();
        let shared = SharedLogMetadataStore::with_frontier_store(
            store,
            log,
            LogTerm::new(1).unwrap(),
            mount,
            frontier,
        )
        .unwrap();
        shared.commit_metadata(command(b"applied", 2)).unwrap();
        shared
            .log()
            .append_batch(
                LogTerm::new(1).unwrap(),
                mount,
                &[command(b"durable-tail", 3)],
            )
            .unwrap();
    }

    let log = FileSharedLog::open(&log_path).unwrap();
    let store = HoltMetadataStore::open_file(&meta_path).unwrap();
    let frontier = FileAppliedFrontierStore::open(&frontier_path).unwrap();
    let (recovered, outcome) = SharedLogMetadataStore::recover_with_frontier_store(
        store,
        log,
        LogTerm::new(1).unwrap(),
        mount,
        frontier,
    )
    .unwrap();

    assert_eq!(outcome.entries, 1);
    assert_eq!(outcome.commands, 1);
    assert_eq!(
        recovered.applied_frontier(),
        Some(ApplyFrontier {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(2).unwrap(),
            },
            commit_version: version(3),
        })
    );
    assert_eq!(
        recovered
            .inner()
            .get(
                RecordFamily::Dentry,
                b"applied",
                version(3),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );
    assert_eq!(
        recovered
            .inner()
            .get(
                RecordFamily::Dentry,
                b"durable-tail",
                version(3),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );
}

#[test]
fn shared_log_metadata_store_compacts_only_applied_prefix() {
    let log = InMemorySharedLog::new();
    let store = HoltMetadataStore::open_memory().unwrap();
    let mount = MountId::new(1).unwrap();
    let shared = SharedLogMetadataStore::new(store, log, LogTerm::new(1).unwrap(), mount);

    shared.commit_metadata(command(b"applied", 2)).unwrap();
    shared
        .log()
        .append_batch(
            LogTerm::new(1).unwrap(),
            mount,
            &[command(b"durable-but-unapplied", 3)],
        )
        .unwrap();

    let frontier = shared
        .compact_applied_log(LogIndex::new(3).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(frontier.applied_position.index, LogIndex::new(1).unwrap());
    assert_eq!(frontier.durable_position.index, LogIndex::new(2).unwrap());
    assert_eq!(frontier.min_retained_index, LogIndex::new(2).unwrap());
    assert_eq!(frontier.compact_through(), Some(LogIndex::new(1).unwrap()));
    assert!(matches!(
        shared.log().read_from(LogIndex::new(1).unwrap(), 0),
        Err(SharedLogError::Compacted { .. })
    ));
    let tail = shared
        .log()
        .read_from(LogIndex::new(2).unwrap(), 0)
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].commands[0].request_id, b"durable-but-unapplied");
}

#[test]
fn shared_log_metadata_store_rejects_failed_predicate_before_log_append() {
    let log = InMemorySharedLog::new();
    let store = HoltMetadataStore::open_memory().unwrap();
    let mount = MountId::new(1).unwrap();
    let shared = SharedLogMetadataStore::new(store, log, LogTerm::new(1).unwrap(), mount);
    let mut command = command(b"a", 2);
    command.predicates = vec![PredicateRef {
        family: RecordFamily::Dentry,
        key: b"a".to_vec(),
        predicate: Predicate::Exists,
    }];

    assert_eq!(
        shared.commit_metadata(command),
        Err(nokvfs_meta::MetadataError::PredicateFailed)
    );
    assert_eq!(shared.log().committed_index(), LogIndex::ZERO);
}

#[test]
fn shared_log_metadata_store_recovers_file_log_into_fresh_store() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata.log");
    let mount = MountId::new(1).unwrap();
    {
        let log = FileSharedLog::open(&path).unwrap();
        let store = HoltMetadataStore::open_memory().unwrap();
        let shared = SharedLogMetadataStore::new(store, log, LogTerm::new(1).unwrap(), mount);
        shared
            .commit_batch(&[command(b"a", 2), command(b"b", 3)])
            .unwrap();
    }

    let log = FileSharedLog::open(&path).unwrap();
    let store = HoltMetadataStore::open_memory().unwrap();
    let (recovered, outcome) =
        SharedLogMetadataStore::recover(store, log, LogTerm::new(1).unwrap(), mount).unwrap();
    assert_eq!(outcome.entries, 1);
    assert_eq!(outcome.commands, 2);
    assert_eq!(
        recovered
            .inner()
            .get(
                RecordFamily::Dentry,
                b"b",
                version(3),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        b"value"
    );

    let result = recovered.commit_metadata(command(b"c", 4)).unwrap();
    assert_eq!(result.commit_version, version(4));
    assert_eq!(recovered.log().committed_index().get(), 2);
    assert_eq!(
        recovered.applied_frontier(),
        Some(ApplyFrontier {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(2).unwrap(),
            },
            commit_version: version(4),
        })
    );
}

#[test]
fn checkpoint_frontier_compacts_before_first_retained_index() {
    let frontier = CheckpointFrontier {
        durable_position: LogPosition {
            term: LogTerm::new(2).unwrap(),
            index: LogIndex::new(12).unwrap(),
        },
        applied_position: LogPosition {
            term: LogTerm::new(2).unwrap(),
            index: LogIndex::new(10).unwrap(),
        },
        min_retained_index: LogIndex::new(7).unwrap(),
        max_commit_version: version(30),
    };

    assert_eq!(frontier.compact_through(), Some(LogIndex::new(6).unwrap()));
    assert_eq!(
        CheckpointFrontier {
            min_retained_index: LogIndex::new(1).unwrap(),
            ..frontier
        }
        .compact_through(),
        None
    );
}

#[test]
fn replay_rejects_non_contiguous_entries() {
    let mount = MountId::new(1).unwrap();
    let entries = vec![
        MetadataLogEntry {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(1).unwrap(),
            },
            mount,
            commands: vec![command(b"a", 2)],
        },
        MetadataLogEntry {
            position: LogPosition {
                term: LogTerm::new(1).unwrap(),
                index: LogIndex::new(3).unwrap(),
            },
            mount,
            commands: vec![command(b"b", 3)],
        },
    ];

    assert!(matches!(
        replay_entries(
            &RecordingSink::default(),
            LogIndex::new(1).unwrap(),
            &entries
        ),
        Err(ReplayError::NonContiguousLog { .. })
    ));
}

#[test]
fn quorum_log_commits_with_majority_and_rejects_minority() {
    let log = InMemoryQuorumLog::new([node(1), node(2), node(3)]).unwrap();
    let mount = MountId::new(1).unwrap();

    log.set_node_available(node(3), false).unwrap();
    let receipts = log
        .append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
        .unwrap();
    assert_eq!(receipts[0].position.index.get(), 1);
    assert_eq!(log.committed_index().get(), 1);
    assert_eq!(log.replica_committed_index(node(1)).unwrap().get(), 1);
    assert_eq!(
        log.replica_committed_index(node(3)).unwrap(),
        LogIndex::ZERO
    );

    log.set_node_available(node(2), false).unwrap();
    assert!(matches!(
        log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"b", 3)]),
        Err(SharedLogError::NoQuorum {
            required: 2,
            available: 1,
        })
    ));
    assert_eq!(log.committed_index().get(), 1);
}

#[test]
fn quorum_log_syncs_restarted_voter_from_committed_tail() {
    let log = InMemoryQuorumLog::new([node(1), node(2), node(3)]).unwrap();
    let mount = MountId::new(1).unwrap();

    log.set_node_available(node(3), false).unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
        .unwrap();
    assert_eq!(
        log.replica_committed_index(node(3)).unwrap(),
        LogIndex::ZERO
    );

    log.set_node_available(node(3), true).unwrap();
    assert_eq!(log.sync_node(node(3)).unwrap().get(), 1);
    let entries = log
        .read_from_node(node(3), LogIndex::new(1).unwrap(), 0)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].commands[0].request_id, b"a");
}

#[test]
fn quorum_log_learner_catches_up_without_voting() {
    let log = InMemoryQuorumLog::with_learners([node(1), node(2), node(3)], [node(4)]).unwrap();
    let mount = MountId::new(1).unwrap();

    log.set_node_available(node(4), false).unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
        .unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"b", 3)])
        .unwrap();
    assert_eq!(
        log.replica_committed_index(node(4)).unwrap(),
        LogIndex::ZERO
    );

    log.set_node_available(node(4), true).unwrap();
    assert_eq!(log.sync_learner(node(4)).unwrap().get(), 2);
    let entries = log
        .read_from_node(node(4), LogIndex::new(1).unwrap(), 0)
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1].commands[0].request_id, b"b");
}

#[test]
fn quorum_log_requires_checkpoint_for_learner_past_compaction() {
    let log = InMemoryQuorumLog::with_learners([node(1), node(2), node(3)], [node(4)]).unwrap();
    let mount = MountId::new(1).unwrap();

    log.set_node_available(node(4), false).unwrap();
    log.append_batch(LogTerm::new(1).unwrap(), mount, &[command(b"a", 2)])
        .unwrap();
    log.compact_through(LogIndex::new(1).unwrap()).unwrap();
    log.set_node_available(node(4), true).unwrap();

    assert!(matches!(
        log.sync_learner(node(4)),
        Err(SharedLogError::Compacted { .. })
    ));
}

#[test]
fn quorum_membership_rejects_empty_or_duplicate_voters() {
    assert!(matches!(
        InMemoryQuorumLog::new([]),
        Err(SharedLogError::NoVoters)
    ));
    assert!(matches!(NodeId::new(0), Err(SharedLogError::ZeroNodeId)));
    assert!(matches!(
        InMemoryQuorumLog::with_learners([node(1), node(2)], [node(2)]),
        Err(SharedLogError::DuplicateNode(_))
    ));
}
