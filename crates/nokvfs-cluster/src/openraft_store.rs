//! OpenRaft storage-v2 boundary for metadata command batches.
//!
//! This module keeps OpenRaft-specific types inside `nokvfs-cluster`. The
//! state machine applies semantic `MetadataCommand` batches through the
//! storage-neutral `MetadataStore` trait; it does not know about Holt trees or
//! filesystem service internals.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
#[cfg(test)]
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::time::Duration;

use nokvfs_meta::command::{
    CommitResult, DelimitedScanItem, DelimitedScanRequest, HistoryPruneOutcome,
    HistoryPruneRequest, KeyScanRequest, MetadataCommand, MetadataError, MetadataStore,
    MetadataStoreStats, MetadataStoreStatsProvider, ReadItem, ReadPurpose, ScanItem, ScanRequest,
};
use nokvfs_protocol::{
    WireMetadataRaftAppendEntriesRequest, WireMetadataRaftAppendEntriesResponse,
    WireMetadataRaftInstallSnapshotRequest, WireMetadataRaftInstallSnapshotResponse,
    WireMetadataRaftVoteRequest, WireMetadataRaftVoteResponse,
};
use nokvfs_types::RecordFamily;
use openraft::entry::EntryPayload;
#[cfg(test)]
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
#[cfg(test)]
use openraft::storage::LogFlushed;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Config, LogId, Raft, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};
#[cfg(test)]
use openraft::{ErrorSubject, ErrorVerb, LogState, RaftLogReader, Vote};

use crate::openraft_file_log::{FileMetadataRaftLog, FileMetadataRaftLogOptions};
use crate::openraft_log::{
    MetadataRaftApplyBatchResult, MetadataRaftCommandBatch, MetadataRaftConfig, MetadataRaftEntry,
};
use crate::openraft_wire;
use crate::NodeId;

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct InMemoryMetadataRaftLog {
    inner: Arc<Mutex<InMemoryMetadataRaftLogInner>>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct InMemoryMetadataRaftLogInner {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    entries: BTreeMap<u64, MetadataRaftEntry>,
}

#[derive(Debug)]
pub struct MetadataRaftStateMachine<M> {
    store: M,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    current_snapshot: Option<MetadataRaftSnapshotImage>,
}

#[derive(Clone, Debug)]
struct MetadataRaftSnapshotImage {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct MetadataRaftSnapshotBuilder {
    image: MetadataRaftSnapshotImage,
}

#[cfg(test)]
#[derive(Debug)]
pub struct MetadataRaftStorage<M> {
    pub log: InMemoryMetadataRaftLog,
    pub state_machine: MetadataRaftStateMachine<M>,
}

pub struct OpenRaftMetadataStore<M> {
    read_store: M,
    raft: Raft<MetadataRaftConfig>,
    runtime: tokio::runtime::Runtime,
}

#[derive(Clone)]
pub struct OpenRaftMetadataStatsHandle {
    metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<u64, BasicNode>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRaftMetadataStats {
    pub node_id: u64,
    pub current_term: u64,
    pub state: String,
    pub current_leader: Option<u64>,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub purged_index: Option<u64>,
    pub millis_since_quorum_ack: Option<u64>,
    pub voter_count: usize,
    pub learner_count: usize,
}

impl<M> Debug for OpenRaftMetadataStore<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenRaftMetadataStore")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetadataRaftNetworkFactory;

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct NoopMetadataRaftNetwork {
    target: u64,
}

#[cfg(test)]
impl InMemoryMetadataRaftLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OpenRaftMetadataStatsHandle {
    pub fn stats(&self) -> OpenRaftMetadataStats {
        let metrics = self.metrics.borrow().clone();
        let membership = metrics.membership_config.membership();
        let voter_count = membership.voter_ids().count();
        let learner_count = membership.learner_ids().count();
        OpenRaftMetadataStats {
            node_id: metrics.id,
            current_term: metrics.current_term,
            state: format!("{:?}", metrics.state),
            current_leader: metrics.current_leader,
            last_log_index: metrics.last_log_index,
            last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
            snapshot_index: metrics.snapshot.map(|log_id| log_id.index),
            purged_index: metrics.purged.map(|log_id| log_id.index),
            millis_since_quorum_ack: metrics.millis_since_quorum_ack,
            voter_count,
            learner_count,
        }
    }
}

#[cfg(test)]
impl InMemoryMetadataRaftLogInner {
    fn last_log_id(&self) -> Option<LogId<u64>> {
        self.entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(self.last_purged_log_id)
    }
}

impl<M> MetadataRaftStateMachine<M>
where
    M: MetadataStore + Send + Sync + 'static,
{
    pub fn new(store: M) -> Self {
        Self {
            store,
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
        }
    }

    #[cfg(test)]
    fn inner(&self) -> &M {
        &self.store
    }

    #[cfg(test)]
    fn last_applied(&self) -> Option<LogId<u64>> {
        self.last_applied
    }
}

#[cfg(test)]
impl<M> MetadataRaftStorage<M>
where
    M: MetadataStore + Send + Sync + 'static,
{
    pub fn in_memory(store: M) -> Self {
        Self {
            log: InMemoryMetadataRaftLog::new(),
            state_machine: MetadataRaftStateMachine::new(store),
        }
    }
}

impl<M> OpenRaftMetadataStore<M>
where
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    pub fn stats_handle(&self) -> OpenRaftMetadataStatsHandle {
        OpenRaftMetadataStatsHandle {
            metrics: self.raft.metrics(),
        }
    }

    #[cfg(test)]
    pub fn new_single_voter(store: M, node: NodeId) -> Result<Self, MetadataError> {
        Self::new_single_voter_with_log(store, node, InMemoryMetadataRaftLog::new(), true)
    }

    #[cfg(test)]
    pub fn new_single_voter_with_file_log(
        store: M,
        node: NodeId,
        log_path: impl AsRef<Path>,
        options: FileMetadataRaftLogOptions,
    ) -> Result<Self, MetadataError> {
        Self::new_single_voter_with_file_log_and_network(
            store,
            node,
            log_path,
            options,
            NoopMetadataRaftNetworkFactory,
        )
    }

    pub fn new_single_voter_with_file_log_and_network<N>(
        store: M,
        node: NodeId,
        log_path: impl AsRef<Path>,
        options: FileMetadataRaftLogOptions,
        network: N,
    ) -> Result<Self, MetadataError>
    where
        N: RaftNetworkFactory<MetadataRaftConfig> + Send + Sync + 'static,
    {
        let log = FileMetadataRaftLog::open(log_path, options)
            .map_err(|err| MetadataError::Backend(format!("openraft file log: {err}")))?;
        let should_initialize = log.last_log_id().is_none();
        Self::new_single_voter_with_log_and_network(store, node, log, should_initialize, network)
    }

    #[cfg(test)]
    fn new_single_voter_with_log<L>(
        store: M,
        node: NodeId,
        log: L,
        should_initialize: bool,
    ) -> Result<Self, MetadataError>
    where
        L: RaftLogStorage<MetadataRaftConfig> + Clone + Send + Sync + 'static,
    {
        Self::new_single_voter_with_log_and_network(
            store,
            node,
            log,
            should_initialize,
            NoopMetadataRaftNetworkFactory,
        )
    }

    fn new_single_voter_with_log_and_network<L, N>(
        store: M,
        node: NodeId,
        log: L,
        should_initialize: bool,
        network: N,
    ) -> Result<Self, MetadataError>
    where
        L: RaftLogStorage<MetadataRaftConfig> + Clone + Send + Sync + 'static,
        N: RaftNetworkFactory<MetadataRaftConfig> + Send + Sync + 'static,
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|err| MetadataError::Backend(format!("openraft runtime: {err}")))?;
        let raft_node = node.get();
        let raft_store = store.clone();
        let raft = runtime.block_on(async move {
            let state_machine = MetadataRaftStateMachine::new(raft_store);
            let raft = Raft::new(
                raft_node,
                Arc::new(
                    Config {
                        cluster_name: format!("nokvfs-metadata-{}", raft_node),
                        heartbeat_interval: 10,
                        election_timeout_min: 50,
                        election_timeout_max: 100,
                        ..Default::default()
                    }
                    .validate()
                    .map_err(|err| MetadataError::Backend(format!("openraft config: {err}")))?,
                ),
                network,
                log,
                state_machine,
            )
            .await
            .map_err(|err| MetadataError::Backend(format!("openraft start: {err}")))?;

            if should_initialize {
                let mut members = BTreeMap::new();
                members.insert(
                    raft_node,
                    BasicNode {
                        addr: format!("local-{raft_node}"),
                    },
                );
                raft.initialize(members)
                    .await
                    .map_err(|err| MetadataError::Backend(format!("openraft initialize: {err}")))?;
            }
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(raft_node, "single-voter metadata raft leader")
                .await
                .map_err(|err| MetadataError::Backend(format!("openraft leader wait: {err}")))?;
            Ok::<_, MetadataError>(raft)
        })?;
        Ok(Self {
            read_store: store,
            raft,
            runtime,
        })
    }

    pub fn shutdown(&self) -> Result<(), MetadataError> {
        self.runtime
            .block_on(self.raft.shutdown())
            .map_err(|err| MetadataError::Backend(format!("openraft shutdown: {err}")))
    }

    pub fn handle_vote_rpc(
        &self,
        request: WireMetadataRaftVoteRequest,
    ) -> Result<WireMetadataRaftVoteResponse, MetadataError> {
        let request = openraft_wire::vote_request(request)
            .map_err(|err| MetadataError::Backend(format!("metadata raft vote decode: {err}")))?;
        let response = self
            .runtime
            .block_on(self.raft.vote(request))
            .map_err(|err| MetadataError::Backend(format!("metadata raft vote: {err}")))?;
        Ok(openraft_wire::wire_vote_response(&response))
    }

    pub fn handle_append_entries_rpc(
        &self,
        request: WireMetadataRaftAppendEntriesRequest,
    ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataError> {
        let request = openraft_wire::append_entries_request(request).map_err(|err| {
            MetadataError::Backend(format!("metadata raft append entries decode: {err}"))
        })?;
        let response = self
            .runtime
            .block_on(self.raft.append_entries(request))
            .map_err(|err| {
                MetadataError::Backend(format!("metadata raft append entries: {err}"))
            })?;
        Ok(openraft_wire::wire_append_entries_response(&response))
    }

    pub fn handle_install_snapshot_rpc(
        &self,
        request: WireMetadataRaftInstallSnapshotRequest,
    ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataError> {
        let request = openraft_wire::install_snapshot_request(request).map_err(|err| {
            MetadataError::Backend(format!("metadata raft install snapshot decode: {err}"))
        })?;
        let response = self
            .runtime
            .block_on(self.raft.install_snapshot(request))
            .map_err(|err| {
                MetadataError::Backend(format!("metadata raft install snapshot: {err}"))
            })?;
        Ok(openraft_wire::wire_install_snapshot_response(&response))
    }

    fn commit_batch_via_raft(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        if commands.is_empty() {
            return Vec::new();
        }
        let batch = match MetadataRaftCommandBatch::new(commands.to_vec()) {
            Ok(batch) => batch,
            Err(err) => return commands.iter().map(|_| Err(err.clone())).collect(),
        };
        match self.runtime.block_on(self.raft.client_write(batch)) {
            Ok(response) => normalize_apply_results(commands.len(), response.data.results),
            Err(err) => {
                let error = MetadataError::Backend(format!("openraft client write: {err}"));
                commands.iter().map(|_| Err(error.clone())).collect()
            }
        }
    }
}

impl<M> MetadataStore for OpenRaftMetadataStore<M>
where
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: nokvfs_meta::Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.read_store.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.read_store.scan(request)
    }

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        self.read_store.scan_delimited(request)
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        self.read_store.scan_keys(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.commit_batch_via_raft(std::slice::from_ref(&command))
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                Err(MetadataError::Backend(
                    "openraft commit returned no result".to_owned(),
                ))
            })
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.commit_batch_via_raft(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.read_store.committed_request_result(request_id)
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.read_store.prune_history(request)
    }
}

impl<M> MetadataStoreStatsProvider for OpenRaftMetadataStore<M>
where
    M: MetadataStore + MetadataStoreStatsProvider + Clone + Send + Sync + 'static,
{
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.read_store.metadata_store_stats()
    }
}

#[cfg(test)]
impl RaftLogReader<MetadataRaftConfig> for InMemoryMetadataRaftLog {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<MetadataRaftEntry>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                "raft log mutex poisoned",
            )
        })?;
        Ok(inner
            .entries
            .iter()
            .filter(|(index, _)| range.contains(*index))
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

#[cfg(test)]
impl RaftLogStorage<MetadataRaftConfig> for InMemoryMetadataRaftLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MetadataRaftConfig>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                "raft log mutex poisoned",
            )
        })?;
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Vote,
                ErrorVerb::Write,
                "raft log mutex poisoned",
            )
        })?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Vote,
                ErrorVerb::Read,
                "raft log mutex poisoned",
            )
        })?;
        Ok(inner.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                "raft committed pointer mutex poisoned",
            )
        })?;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                "raft committed pointer mutex poisoned",
            )
        })?;
        Ok(inner.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<MetadataRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = MetadataRaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                "raft log mutex poisoned",
            )
        })?;
        for entry in entries {
            inner.entries.insert(entry.log_id.index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Log(log_id),
                ErrorVerb::Delete,
                "raft log mutex poisoned",
            )
        })?;
        let keys = inner
            .entries
            .range(log_id.index..)
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        for index in keys {
            inner.entries.remove(&index);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Log(log_id),
                ErrorVerb::Delete,
                "raft log mutex poisoned",
            )
        })?;
        let keys = inner
            .entries
            .range(..=log_id.index)
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        for index in keys {
            inner.entries.remove(&index);
        }
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }
}

impl<M> RaftStateMachine<MetadataRaftConfig> for MetadataRaftStateMachine<M>
where
    M: MetadataStore + Send + Sync + 'static,
{
    type SnapshotBuilder = MetadataRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<MetadataRaftApplyBatchResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = MetadataRaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut out = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {
                    out.push(MetadataRaftApplyBatchResult::success(Vec::new()));
                }
                EntryPayload::Normal(batch) => {
                    out.push(MetadataRaftApplyBatchResult {
                        results: self.store.commit_independent_batch(&batch.commands),
                    });
                }
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembership::new(Some(log_id), membership);
                    out.push(MetadataRaftApplyBatchResult::success(Vec::new()));
                }
            }
            self.last_applied = Some(log_id);
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetadataRaftSnapshotBuilder {
            image: self.current_snapshot_image(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot = Some(MetadataRaftSnapshotImage {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetadataRaftConfig>>, StorageError<u64>> {
        Ok(self.current_snapshot.clone().map(snapshot_from_image))
    }
}

impl<M> MetadataRaftStateMachine<M> {
    fn current_snapshot_image(&self) -> MetadataRaftSnapshotImage {
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: snapshot_id(self.last_applied),
        };
        MetadataRaftSnapshotImage {
            meta,
            data: Vec::new(),
        }
    }
}

impl RaftSnapshotBuilder<MetadataRaftConfig> for MetadataRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetadataRaftConfig>, StorageError<u64>> {
        Ok(snapshot_from_image(self.image.clone()))
    }
}

#[cfg(test)]
impl RaftNetworkFactory<MetadataRaftConfig> for NoopMetadataRaftNetworkFactory {
    type Network = NoopMetadataRaftNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        NoopMetadataRaftNetwork { target }
    }
}

#[cfg(test)]
impl RaftNetwork<MetadataRaftConfig> for NoopMetadataRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<MetadataRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<u64>,
        openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        Err(unreachable_rpc(self.target))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<MetadataRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        openraft::error::RPCError<
            u64,
            BasicNode,
            openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
        >,
    > {
        Err(unreachable_rpc(self.target))
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<u64>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<u64>,
        openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        Err(unreachable_rpc(self.target))
    }
}

fn snapshot_from_image(image: MetadataRaftSnapshotImage) -> Snapshot<MetadataRaftConfig> {
    Snapshot {
        meta: image.meta,
        snapshot: Box::new(Cursor::new(image.data)),
    }
}

fn snapshot_id(last_applied: Option<LogId<u64>>) -> String {
    match last_applied {
        Some(log_id) => format!("metadata-raft-{}-{}", log_id.leader_id.term, log_id.index),
        None => "metadata-raft-empty".to_owned(),
    }
}

#[cfg(test)]
fn storage_error(subject: ErrorSubject<u64>, verb: ErrorVerb, message: &str) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, std::io::Error::other(message.to_owned()))
}

#[cfg(test)]
fn unreachable_rpc<E>(
    target: u64,
) -> openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64, E>>
where
    E: std::error::Error,
{
    let error = std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        format!("metadata raft peer {target} has no network transport"),
    );
    openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&error))
}

fn normalize_apply_results(
    expected: usize,
    mut results: Vec<Result<CommitResult, MetadataError>>,
) -> Vec<Result<CommitResult, MetadataError>> {
    if results.len() == expected {
        return results;
    }
    let error = MetadataError::Backend(format!(
        "openraft apply returned {} results for {expected} commands",
        results.len()
    ));
    results.resize_with(expected, || Err(error.clone()));
    results.truncate(expected);
    results
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use nokvfs_meta::command::{
        CommandKind, MetadataCommand, Mutation, MutationOp, Predicate, PredicateRef, ReadPurpose,
        Value, Version, WatchProjection,
    };
    use nokvfs_meta::holtstore::HoltMetadataStore;
    use nokvfs_types::RecordFamily;
    use openraft::entry::FromAppData;
    use openraft::storage::RaftLogStorageExt;
    use openraft::{BasicNode, CommittedLeaderId, Config, LogId, Raft, RaftLogReader, Vote};

    use super::*;
    use crate::openraft_file_log::{FileMetadataRaftLog, FileMetadataRaftLogOptions};
    use crate::openraft_log::MetadataRaftCommandBatch;

    #[test]
    fn in_memory_log_storage_round_trips_entries() {
        runtime().block_on(async {
            let mut log = InMemoryMetadataRaftLog::new();
            let entry = metadata_entry(1, 1, metadata_command(b"req-1", b"dentry/a", 2));

            log.blocking_append([entry.clone()]).await.unwrap();
            assert_eq!(
                log.get_log_state().await.unwrap().last_log_id,
                Some(entry.log_id)
            );

            let mut reader = log.get_log_reader().await;
            assert_eq!(
                reader.try_get_log_entries(1..2).await.unwrap(),
                vec![entry.clone()]
            );

            log.truncate(entry.log_id).await.unwrap();
            assert!(log.try_get_log_entries(1..2).await.unwrap().is_empty());

            log.blocking_append([entry.clone()]).await.unwrap();
            log.purge(entry.log_id).await.unwrap();
            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id, Some(entry.log_id));
            assert_eq!(state.last_log_id, Some(entry.log_id));
        });
    }

    #[test]
    fn file_metadata_raft_log_recovers_openraft_state() {
        runtime().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("metadata-raft.log");
            let entry = metadata_entry(3, 1, metadata_command(b"req-file-log", b"dentry/file", 7));
            {
                let mut log =
                    FileMetadataRaftLog::open(&path, FileMetadataRaftLogOptions::default())
                        .unwrap();
                let vote = Vote::new_committed(3, 1);
                log.save_vote(&vote).await.unwrap();
                log.blocking_append([entry.clone()]).await.unwrap();
                log.save_committed(Some(entry.log_id)).await.unwrap();
            }

            let mut reopened =
                FileMetadataRaftLog::open(&path, FileMetadataRaftLogOptions::default()).unwrap();
            assert_eq!(
                reopened.read_vote().await.unwrap(),
                Some(Vote::new_committed(3, 1))
            );
            assert_eq!(reopened.read_committed().await.unwrap(), Some(entry.log_id));
            assert_eq!(
                reopened.get_log_state().await.unwrap().last_log_id,
                Some(entry.log_id)
            );
            assert_eq!(
                reopened.try_get_log_entries(1..2).await.unwrap(),
                vec![entry.clone()]
            );

            reopened.truncate(entry.log_id).await.unwrap();
            drop(reopened);
            let mut truncated =
                FileMetadataRaftLog::open(&path, FileMetadataRaftLogOptions::default()).unwrap();
            assert!(truncated
                .try_get_log_entries(1..2)
                .await
                .unwrap()
                .is_empty());
        });
    }

    #[test]
    fn state_machine_applies_metadata_command_batches() {
        runtime().block_on(async {
            let store = HoltMetadataStore::open_memory().unwrap();
            let mut sm = MetadataRaftStateMachine::new(store);
            let command = metadata_command(b"req-apply", b"dentry/apply", 2);
            let entry = metadata_entry(1, 1, command.clone());

            let applied = sm.apply([entry.clone()]).await.unwrap();
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0].results.len(), 1);
            assert!(applied[0].results[0].is_ok());
            assert_eq!(sm.last_applied(), Some(entry.log_id));

            let value = sm
                .inner()
                .get(
                    RecordFamily::Dentry,
                    b"dentry/apply",
                    Version::new(2).unwrap(),
                    ReadPurpose::UserStrong,
                )
                .unwrap()
                .unwrap();
            assert_eq!(value.0, b"inode=2".to_vec());
        });
    }

    #[test]
    fn single_node_openraft_applies_client_write() {
        runtime().block_on(async {
            let store = HoltMetadataStore::open_memory().unwrap();
            let storage = MetadataRaftStorage::in_memory(store);
            let raft = Raft::new(
                1,
                Arc::new(
                    Config {
                        cluster_name: "nokvfs-test".to_owned(),
                        heartbeat_interval: 10,
                        election_timeout_min: 50,
                        election_timeout_max: 100,
                        ..Default::default()
                    }
                    .validate()
                    .unwrap(),
                ),
                NoopMetadataRaftNetworkFactory,
                storage.log,
                storage.state_machine,
            )
            .await
            .unwrap();

            let mut members = BTreeMap::new();
            members.insert(
                1,
                BasicNode {
                    addr: "local".to_owned(),
                },
            );
            raft.initialize(members).await.unwrap();
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "single-node metadata raft leader")
                .await
                .unwrap();

            let command = metadata_command(b"req-client-write", b"dentry/raft", 3);
            let response = raft
                .client_write(MetadataRaftCommandBatch::new(vec![command]).unwrap())
                .await
                .unwrap();
            assert_eq!(response.data.results.len(), 1);
            assert!(response.data.results[0].is_ok());
            assert!(response.log_id.index > 0);

            raft.shutdown().await.unwrap();
        });
    }

    #[test]
    fn openraft_metadata_store_commits_through_client_write() {
        let store = HoltMetadataStore::open_memory().unwrap();
        let raft_store = OpenRaftMetadataStore::new_single_voter(store, NodeId::new(1).unwrap())
            .expect("single voter OpenRaft store opens");

        let command = metadata_command(b"req-store", b"dentry/store", 4);
        let result = raft_store.commit_metadata(command).unwrap();
        assert_eq!(result.commit_version, Version::new(4).unwrap());
        let value = raft_store
            .get(
                RecordFamily::Dentry,
                b"dentry/store",
                Version::new(4).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap();
        assert_eq!(value.0, b"inode=2".to_vec());

        raft_store.shutdown().unwrap();
    }

    #[test]
    fn openraft_metadata_store_file_log_reopens_and_commits() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("metadata-raft.log");
        let first_store = HoltMetadataStore::open_file(dir.path().join("metadata")).unwrap();
        let raft_store = OpenRaftMetadataStore::new_single_voter_with_file_log(
            first_store,
            NodeId::new(1).unwrap(),
            &log_path,
            FileMetadataRaftLogOptions::default(),
        )
        .expect("file-backed single voter opens");

        let command = metadata_command(b"req-file-backed", b"dentry/persistent", 8);
        raft_store.commit_metadata(command).unwrap();
        raft_store.shutdown().unwrap();

        let reopened_store = HoltMetadataStore::open_file(dir.path().join("metadata")).unwrap();
        let reopened = OpenRaftMetadataStore::new_single_voter_with_file_log(
            reopened_store,
            NodeId::new(1).unwrap(),
            &log_path,
            FileMetadataRaftLogOptions::default(),
        )
        .expect("file-backed single voter reopens");
        let value = reopened
            .get(
                RecordFamily::Dentry,
                b"dentry/persistent",
                Version::new(8).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap();
        assert_eq!(value.0, b"inode=2".to_vec());

        let second = metadata_command(b"req-file-backed-2", b"dentry/persistent-2", 9);
        assert_eq!(
            reopened.commit_metadata(second).unwrap().commit_version,
            Version::new(9).unwrap()
        );
        reopened.shutdown().unwrap();
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    fn metadata_entry(term: u64, index: u64, command: MetadataCommand) -> MetadataRaftEntry {
        let mut entry =
            MetadataRaftEntry::from_app_data(MetadataRaftCommandBatch::new(vec![command]).unwrap());
        entry.log_id = LogId::new(CommittedLeaderId::new(term, 1), index);
        entry
    }

    fn metadata_command(request_id: &[u8], key: &[u8], commit_version: u64) -> MetadataCommand {
        MetadataCommand {
            request_id: request_id.to_vec(),
            kind: CommandKind::CreateFile,
            read_version: Version::new(1).unwrap(),
            commit_version: Version::new(commit_version).unwrap(),
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
                value: Some(Value(b"inode=2".to_vec())),
            }],
            watch: vec![WatchProjection {
                family: RecordFamily::Dentry,
                key: key.to_vec(),
                event: b"create".to_vec(),
            }],
        }
    }
}
