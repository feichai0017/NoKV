use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::{
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
    AnyError, BasicNode, CommittedLeaderId, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership,
    Vote,
};

use crate::{
    decode_region_snapshot_status, AppliedProposal, Error, NodeId, OpenRaftEntry, RaftEntryLog,
    RaftStoreConfig, RegionSnapshotEngine, SegmentedEntryLog,
};

pub struct RegionLogStorage {
    log: Arc<Mutex<SegmentedEntryLog>>,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

impl RegionLogStorage {
    pub fn new(log: SegmentedEntryLog) -> Self {
        Self {
            log: Arc::new(Mutex::new(log)),
            vote: None,
            committed: None,
        }
    }

    pub fn latest_membership(&self) -> Result<Option<StoredMembership<NodeId, BasicNode>>, Error> {
        let mut latest = None;
        for entry in self.lock_log()?.recover_entries()? {
            if let openraft::EntryPayload::Membership(membership) = entry.payload {
                latest = Some(StoredMembership::new(Some(entry.log_id), membership));
            }
        }
        Ok(latest)
    }

    pub fn seed_single_node_vote_above_log(&mut self, node_id: NodeId) -> Result<(), Error> {
        if self.vote.is_some() {
            return Ok(());
        }
        let last_log_id = {
            let log = self.lock_log()?;
            log.last_log_id()?.or(log.last_purged_log_id()?)
        };
        if let Some(log_id) = last_log_id {
            let vote = Vote::new_committed(log_id.leader_id.term + 1, node_id);
            self.lock_log()?.save_vote(vote)?;
            self.vote = Some(vote);
        }
        Ok(())
    }

    fn lock_log(&self) -> Result<MutexGuard<'_, SegmentedEntryLog>, Error> {
        self.log
            .lock()
            .map_err(|_| Error::OpenRaft("region log mutex poisoned".to_owned()))
    }

    fn lock_storage_log(
        &self,
        verb: ErrorVerb,
    ) -> Result<MutexGuard<'_, SegmentedEntryLog>, StorageError<NodeId>> {
        self.log
            .lock()
            .map_err(|_| storage_error(verb, "region log mutex poisoned"))
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

    pub fn with_membership(engine: E, membership: StoredMembership<NodeId, BasicNode>) -> Self {
        Self { engine, membership }
    }

    pub fn apply_engine(&self) -> &E {
        &self.engine
    }

    pub fn restore_membership(&mut self, membership: StoredMembership<NodeId, BasicNode>) {
        self.membership = membership;
    }

    pub fn membership(&self) -> &StoredMembership<NodeId, BasicNode> {
        &self.membership
    }
}

#[derive(Clone)]
pub struct RegionLogReader {
    log: Arc<Mutex<SegmentedEntryLog>>,
}

impl RaftLogReader<RaftStoreConfig> for RegionLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<OpenRaftEntry>, StorageError<NodeId>> {
        self.log
            .lock()
            .map_err(|_| storage_error(ErrorVerb::Read, "region log mutex poisoned"))?
            .read_entries(range)
            .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))
    }
}

impl RaftLogReader<RaftStoreConfig> for RegionLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<OpenRaftEntry>, StorageError<NodeId>> {
        self.lock_storage_log(ErrorVerb::Read)?
            .read_entries(range)
            .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))
    }
}

impl RaftLogStorage<RaftStoreConfig> for RegionLogStorage {
    type LogReader = RegionLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<RaftStoreConfig>, StorageError<NodeId>> {
        let log = self.lock_storage_log(ErrorVerb::Read)?;
        let last_purged_log_id = log
            .last_purged_log_id()
            .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))?;
        let last_log_id = log
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
            log: self.log.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.lock_storage_log(ErrorVerb::Write)?
            .save_vote(*vote)
            .map_err(|err| storage_error(ErrorVerb::Write, err.to_string()))?;
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        if self.vote.is_none() {
            let vote = {
                self.lock_storage_log(ErrorVerb::Read)?
                    .read_vote()
                    .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))?
            };
            self.vote = vote;
        }
        Ok(self.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.lock_storage_log(ErrorVerb::Write)?
            .save_committed(committed)
            .map_err(|err| storage_error(ErrorVerb::Write, err.to_string()))?;
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        if self.committed.is_none() {
            let committed = {
                self.lock_storage_log(ErrorVerb::Read)?
                    .read_committed()
                    .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))?
            };
            self.committed = committed;
        }
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
        let result = self.lock_storage_log(ErrorVerb::Write).and_then(|mut log| {
            log.append_entries(&entries)
                .and_then(|_| log.sync())
                .map_err(|err| storage_error(ErrorVerb::Write, err.to_string()))
        });
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
        self.lock_storage_log(ErrorVerb::Delete)
            .and_then(|mut log| {
                log.truncate_since(log_id)
                    .and_then(|_| log.sync())
                    .map_err(|err| storage_error(ErrorVerb::Delete, err.to_string()))
            })
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.lock_storage_log(ErrorVerb::Delete)
            .and_then(|mut log| {
                log.purge_upto(log_id)
                    .and_then(|_| log.sync())
                    .map_err(|err| storage_error(ErrorVerb::Delete, err.to_string()))
            })
    }
}

pub struct RegionSnapshotBuilder {
    last_log_id: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    payload: Result<Vec<u8>, String>,
}

impl RaftSnapshotBuilder<RaftStoreConfig> for RegionSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RaftStoreConfig>, StorageError<NodeId>> {
        let payload = self
            .payload
            .clone()
            .map_err(|err| storage_error(ErrorVerb::Read, err))?;
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_log_id,
                last_membership: self.membership.clone(),
                snapshot_id: snapshot_id(self.last_log_id, &payload),
            },
            snapshot: Box::new(std::io::Cursor::new(payload)),
        })
    }
}

impl<E> RaftStateMachine<RaftStoreConfig> for RegionStateMachine<E>
where
    E: RegionSnapshotEngine,
{
    type SnapshotBuilder = RegionSnapshotBuilder;

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
        RegionSnapshotBuilder {
            last_log_id,
            membership,
            payload: self
                .engine
                .export_region_snapshot()
                .map_err(|err| err.to_string()),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let payload = snapshot.into_inner();
        let status = decode_region_snapshot_status(&payload)
            .map_err(|err| storage_error(ErrorVerb::Read, err.to_string()))?;
        if let Some(log_id) = meta.last_log_id {
            if log_id.index != status.applied_index || log_id.leader_id.term != status.term {
                return Err(storage_error(
                    ErrorVerb::Write,
                    format!(
                        "snapshot metadata log id {log_id:?} does not match payload term {} index {}",
                        status.term, status.applied_index
                    ),
                ));
            }
        } else if status.applied_index != 0 {
            return Err(storage_error(
                ErrorVerb::Write,
                "non-empty snapshot is missing last log id",
            ));
        }
        self.engine
            .install_region_snapshot(&payload)
            .map_err(|err| storage_error(ErrorVerb::Write, err.to_string()))?;
        self.membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftStoreConfig>>, StorageError<NodeId>> {
        let mut builder = self.get_snapshot_builder().await;
        builder.build_snapshot().await.map(Some)
    }
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

fn snapshot_id(last_log_id: Option<LogId<NodeId>>, payload: &[u8]) -> String {
    let (term, index) = last_log_id
        .map(|log_id| (log_id.leader_id.term, log_id.index))
        .unwrap_or_default();
    format!(
        "region-snapshot-{term}-{index}-{:08x}",
        crc32fast::hash(payload)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AppliedKvEngine, Proposal};
    use nokv_mvcc::{KvEngine, MvccStore};
    use nokv_proto::nokv::kv::v1 as kvpb;
    use nokv_proto::nokv::raft::v1 as raftpb;
    use openraft::{storage::RaftLogStorageExt, CommittedLeaderId, EntryPayload, LogId};
    use std::collections::{BTreeMap, BTreeSet};

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
                        commit_version: index,
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

    #[tokio::test]
    async fn region_state_machine_builds_and_installs_snapshot() {
        let engine = AppliedKvEngine::new(7, MvccStore::default());
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
        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 2);
        assert!(!snapshot.snapshot.get_ref().is_empty());

        let restored = AppliedKvEngine::new(7, MvccStore::default());
        let mut restored_state_machine = RegionStateMachine::new(restored.clone());
        restored_state_machine
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        assert_eq!(restored.status().applied_index, 2);
        let current = restored
            .get(&kvpb::GetRequest {
                key: b"b".to_vec(),
                version: 2,
            })
            .unwrap();
        assert_eq!(current.value, b"2");

        let current_snapshot = restored_state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert!(!current_snapshot.snapshot.get_ref().is_empty());
    }

    #[tokio::test]
    async fn region_state_machine_rejects_stale_snapshot_install() {
        let old_engine = AppliedKvEngine::new(7, MvccStore::default());
        let mut old_state_machine = RegionStateMachine::new(old_engine);
        old_state_machine
            .apply(vec![normal_entry(7, 1, b"a", b"1")])
            .await
            .unwrap();
        let mut old_builder = old_state_machine.get_snapshot_builder().await;
        let old_snapshot = old_builder.build_snapshot().await.unwrap();

        let current_engine = AppliedKvEngine::new(7, MvccStore::default());
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
}
