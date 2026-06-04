use std::sync::Mutex;

use super::*;
use nokvfs_meta::command::{
    CommandKind, MetadataCommand, MetadataStore, MetadataStoreStatsProvider, Mutation, MutationOp,
    ReadPurpose, Value, Version,
};
use nokvfs_meta::HoltMetadataStore;
use nokvfs_types::{MountId, RecordFamily};

fn version(raw: u64) -> Version {
    Version::new(raw).unwrap()
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
