use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use openraft::{
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
    AnyError, BasicNode, CommittedLeaderId, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership,
    Vote,
};

use crate::{
    metrics, AppliedProposal, Error, NodeId, OpenRaftEntry, RaftEntryLog, RaftStoreConfig,
    RegionSnapshotEngine, SegmentedEntryLog,
};

use super::{decode_region_snapshot_status, RegionLogFlushOptions, RegionLogFlusher};

pub struct RegionLogStorage {
    log: Arc<Mutex<SegmentedEntryLog>>,
    flusher: RegionLogFlusher,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

impl RegionLogStorage {
    pub fn new(log: SegmentedEntryLog) -> Self {
        Self::new_with_options(log, RegionLogFlushOptions::default())
    }

    pub fn new_with_options(log: SegmentedEntryLog, options: RegionLogFlushOptions) -> Self {
        let log = Arc::new(Mutex::new(log));
        Self {
            flusher: RegionLogFlusher::new(log.clone(), options),
            log,
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

    pub fn log_id_at_index(&self, index: u64) -> Result<Option<LogId<NodeId>>, Error> {
        let log = self.lock_log()?;
        if let Some(purged) = log.last_purged_log_id()? {
            if purged.index == index {
                return Ok(Some(purged));
            }
        }
        Ok(log
            .recover_entries()?
            .into_iter()
            .find(|entry| entry.log_id.index == index)
            .map(|entry| entry.log_id))
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
    last_applied: Option<LogId<NodeId>>,
}

impl<E> RegionStateMachine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            membership: StoredMembership::default(),
            last_applied: None,
        }
    }

    pub fn with_membership(engine: E, membership: StoredMembership<NodeId, BasicNode>) -> Self {
        Self {
            engine,
            membership,
            last_applied: None,
        }
    }

    pub fn apply_engine(&self) -> &E {
        &self.engine
    }

    pub fn restore_membership(&mut self, membership: StoredMembership<NodeId, BasicNode>) {
        self.membership = membership;
    }

    pub fn restore_last_applied(&mut self, last_applied: Option<LogId<NodeId>>) {
        if last_applied.is_some() {
            self.last_applied = last_applied;
        }
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
        self.flusher.flush_now(ErrorVerb::Write).await?;
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
        self.flusher.flush_now(ErrorVerb::Write).await?;
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
        self.flusher.append_entries(&entries, callback)
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.flusher.flush_now(ErrorVerb::Delete).await?;
        self.lock_storage_log(ErrorVerb::Delete)
            .and_then(|mut log| {
                log.truncate_since(log_id)
                    .and_then(|_| log.sync())
                    .map_err(|err| storage_error(ErrorVerb::Delete, err.to_string()))
            })
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.flusher.flush_now(ErrorVerb::Delete).await?;
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
        let last_applied = self.last_applied.or_else(|| {
            (status.applied_index != 0).then(|| {
                LogId::new(
                    CommittedLeaderId::new(status.term, NodeId::default()),
                    status.applied_index,
                )
            })
        });
        Ok((last_applied, self.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppliedProposal>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = OpenRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let entry_count = entries.len() as u64;
        let apply_started = Instant::now();
        let mut membership = None;
        let last_applied = entries.last().map(|entry| entry.log_id);
        for entry in &entries {
            if let openraft::EntryPayload::Membership(next) = &entry.payload {
                membership = Some(StoredMembership::new(Some(entry.log_id), next.clone()));
            }
        }
        let applied = self
            .engine
            .apply_openraft_entries(entries)
            .map_err(|err| storage_error(ErrorVerb::Write, err.to_string()))?;
        metrics::record_state_machine_apply(entry_count, apply_started.elapsed());
        if let Some(membership) = membership {
            self.membership = membership;
        }
        if let Some(last_applied) = last_applied {
            self.last_applied = Some(last_applied);
        }
        Ok(applied)
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
        self.last_applied = meta.last_log_id;
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
#[path = "test.rs"]
mod test;
