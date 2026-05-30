use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;

use openraft::{
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
    AnyError, BasicNode, CommittedLeaderId, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership,
    Vote,
};

use crate::{
    AppliedProposal, NodeId, OpenRaftEntry, RaftEntryLog, RaftStoreConfig, RegionApplyEngine,
    SegmentedEntryLog,
};

pub struct RegionLogStorage {
    log: SegmentedEntryLog,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

impl RegionLogStorage {
    pub fn new(log: SegmentedEntryLog) -> Self {
        Self {
            log,
            vote: None,
            committed: None,
        }
    }
}

pub struct RegionStateMachine<E> {
    engine: E,
    membership: StoredMembership<NodeId, BasicNode>,
}

impl<E> RegionStateMachine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            membership: StoredMembership::default(),
        }
    }

    pub fn apply_engine(&self) -> &E {
        &self.engine
    }
}

#[derive(Clone)]
pub struct RegionLogReader {
    entries: Vec<OpenRaftEntry>,
}

impl RaftLogReader<RaftStoreConfig> for RegionLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<OpenRaftEntry>, StorageError<NodeId>> {
        Ok(self
            .entries
            .iter()
            .filter(|entry| range_contains(&range, entry.log_id.index))
            .cloned()
            .collect())
    }
}

impl RaftLogReader<RaftStoreConfig> for RegionLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<OpenRaftEntry>, StorageError<NodeId>> {
        self.log
            .read_entries(range)
            .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))
    }
}

impl RaftLogStorage<RaftStoreConfig> for RegionLogStorage {
    type LogReader = RegionLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<RaftStoreConfig>, StorageError<NodeId>> {
        let last_purged_log_id = self
            .log
            .last_purged_log_id()
            .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))?;
        let last_log_id = self
            .log
            .last_log_id()
            .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))?
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        RegionLogReader {
            entries: self.log.recover_entries().unwrap_or_default(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RaftStoreConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = OpenRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let result = self
            .log
            .append_entries(&entries)
            .and_then(|_| self.log.sync());
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(err) => {
                let message = err.to_string();
                callback.log_io_completed(Err(io::Error::other(message.clone())));
                Err(storage_error(ErrorVerb::Write, message))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log
            .truncate_since(log_id)
            .and_then(|_| self.log.sync())
            .map_err(|err| storage_error(ErrorVerb::Delete, err.to_string()))
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log
            .purge_upto(log_id)
            .and_then(|_| self.log.sync())
            .map_err(|err| storage_error(ErrorVerb::Delete, err.to_string()))
    }
}

pub struct NoopSnapshotBuilder {
    last_log_id: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
}

impl RaftSnapshotBuilder<RaftStoreConfig> for NoopSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RaftStoreConfig>, StorageError<NodeId>> {
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_log_id,
                last_membership: self.membership.clone(),
                snapshot_id: "noop".to_owned(),
            },
            snapshot: Box::new(std::io::Cursor::new(Vec::new())),
        })
    }
}

impl<E> RaftStateMachine<RaftStoreConfig> for RegionStateMachine<E>
where
    E: RegionApplyEngine,
{
    type SnapshotBuilder = NoopSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let status = self.engine.apply_status();
        let last_applied = (status.applied_index != 0).then(|| {
            LogId::new(
                CommittedLeaderId::new(status.term, NodeId::default()),
                status.applied_index,
            )
        });
        Ok((last_applied, self.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppliedProposal>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = OpenRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        for entry in &entries {
            if let openraft::EntryPayload::Membership(membership) = &entry.payload {
                self.membership = StoredMembership::new(Some(entry.log_id), membership.clone());
            }
        }
        self.engine
            .apply_openraft_entries(entries)
            .map_err(|err| storage_error(ErrorVerb::Write, err.to_string()))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let (last_log_id, membership) = self.applied_state().await.unwrap_or_default();
        NoopSnapshotBuilder {
            last_log_id,
            membership,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<NodeId, BasicNode>,
        _snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        Err(storage_error(
            ErrorVerb::Write,
            "raft snapshot install is not wired yet",
        ))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftStoreConfig>>, StorageError<NodeId>> {
        Ok(None)
    }
}

fn range_contains<R>(range: &R, index: u64) -> bool
where
    R: RangeBounds<u64>,
{
    let after_start = match range.start_bound() {
        std::ops::Bound::Included(start) => index >= *start,
        std::ops::Bound::Excluded(start) => index > *start,
        std::ops::Bound::Unbounded => true,
    };
    let before_end = match range.end_bound() {
        std::ops::Bound::Included(end) => index <= *end,
        std::ops::Bound::Excluded(end) => index < *end,
        std::ops::Bound::Unbounded => true,
    };
    after_start && before_end
}

fn storage_error(verb: ErrorVerb, message: impl Into<String>) -> StorageError<NodeId> {
    StorageError::IO {
        source: openraft::StorageIOError::new(
            ErrorSubject::Store,
            verb,
            AnyError::error(message.into()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AppliedKvEngine, Proposal};
    use nokv_mvcc::MvccStore;
    use nokv_proto::nokv::kv::v1 as kvpb;
    use nokv_proto::nokv::raft::v1 as raftpb;
    use openraft::{storage::RaftLogStorageExt, CommittedLeaderId, EntryPayload, LogId};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn normal_entry(region_id: u64, index: u64, key: &[u8], value: &[u8]) -> OpenRaftEntry {
        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id,
                request_id: index,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                    kvpb::TryAtomicMutateRequest {
                        mutations: vec![kvpb::Mutation {
                            op: kvpb::mutation::Op::Put as i32,
                            key: key.to_vec(),
                            value: value.to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }],
        };
        OpenRaftEntry {
            log_id: log_id(3, index),
            payload: EntryPayload::Normal(Proposal::from_raft_command(&command).unwrap()),
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
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
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
    async fn region_state_machine_applies_entries() {
        let engine = AppliedKvEngine::new(7, MvccStore::default());
        let mut state_machine = RegionStateMachine::new(engine);
        let entries = vec![
            normal_entry(7, 1, b"a", b"1"),
            normal_entry(7, 2, b"b", b"2"),
        ];

        let applied = state_machine.apply(entries).await.unwrap();
        assert_eq!(applied.len(), 2);
        assert_eq!(state_machine.apply_engine().status().applied_index, 2);
    }
}
