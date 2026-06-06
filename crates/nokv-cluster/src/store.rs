//! OpenRaft storage-v2 boundary for metadata command batches.
//!
//! This module keeps OpenRaft-specific types inside `nokv-cluster`. The
//! state machine applies semantic `MetadataCommand` batches through the
//! storage-neutral `MetadataStore` trait; it does not know about Holt trees or
//! filesystem service internals.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::io::Cursor;
#[cfg(test)]
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use nokv_meta::command::{
    CommitResult, DelimitedScanItem, DelimitedScanRequest, HistoryPruneOutcome,
    HistoryPruneRequest, KeyScanRequest, MetadataCheckpointStore, MetadataCommand, MetadataError,
    MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, ReadItem, ReadPurpose, ScanItem,
    ScanRequest,
};
use nokv_protocol::{
    WireMetadataRaftAppendEntriesRequest, WireMetadataRaftAppendEntriesResponse,
    WireMetadataRaftInstallSnapshotRequest, WireMetadataRaftInstallSnapshotResponse,
    WireMetadataRaftVoteRequest, WireMetadataRaftVoteResponse,
};
use nokv_types::RecordFamily;
use openraft::entry::EntryPayload;
#[cfg(test)]
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
#[cfg(test)]
use openraft::storage::LogFlushed;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Config, ErrorSubject, ErrorVerb, LogId, Raft, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership,
};
#[cfg(test)]
use openraft::{LogState, RaftLogReader, Vote};

use crate::file_log::{FileMetadataRaftLog, FileMetadataRaftLogOptions};
use crate::log::{
    MetadataRaftApplyBatchResult, MetadataRaftCommandBatch, MetadataRaftConfig, MetadataRaftEntry,
};
use crate::wire;
use crate::{LogIndex, LogPosition, LogTerm, NodeId};

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
pub struct MetadataRaftSnapshotBuilder<M> {
    store: M,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
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
    counters: Arc<OpenRaftMetadataCounters>,
    coalescer: Arc<ProposalCoalescer>,
}

#[derive(Clone)]
pub struct OpenRaftMetadataStatsHandle {
    metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<u64, BasicNode>>,
    counters: Arc<OpenRaftMetadataCounters>,
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
    pub proposal_batch_total: u64,
    pub proposal_command_total: u64,
    pub proposal_max_batch: u64,
    pub proposal_ns_total: u64,
    pub proposal_queue_wait_ns_total: u64,
    pub proposal_queue_max_wait_ns: u64,
}

#[derive(Default)]
struct OpenRaftMetadataCounters {
    proposal_batch_total: AtomicU64,
    proposal_command_total: AtomicU64,
    proposal_max_batch: AtomicU64,
    proposal_ns_total: AtomicU64,
    proposal_queue_wait_ns_total: AtomicU64,
    proposal_queue_max_wait_ns: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProposalCoalescerOptions {
    pub max_commands: usize,
    pub max_bytes: usize,
    pub max_delay: Duration,
}

#[derive(Debug)]
struct ProposalCoalescer {
    options: ProposalCoalescerOptions,
    inner: Mutex<ProposalCoalescerState>,
    cvar: Condvar,
}

#[derive(Debug, Default)]
struct ProposalCoalescerState {
    queue: VecDeque<QueuedProposal>,
    draining: bool,
}

#[derive(Debug)]
struct QueuedProposal {
    command: MetadataCommand,
    estimated_bytes: usize,
    enqueued_at: Instant,
    result: Arc<Mutex<Option<Result<CommitResult, MetadataError>>>>,
}

impl Default for ProposalCoalescerOptions {
    fn default() -> Self {
        Self {
            max_commands: 64,
            max_bytes: 1024 * 1024,
            max_delay: Duration::from_micros(200),
        }
    }
}

impl ProposalCoalescer {
    fn new(options: ProposalCoalescerOptions) -> Self {
        Self {
            options,
            inner: Mutex::new(ProposalCoalescerState::default()),
            cvar: Condvar::new(),
        }
    }
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
            proposal_batch_total: self.counters.proposal_batch_total.load(Ordering::Relaxed),
            proposal_command_total: self.counters.proposal_command_total.load(Ordering::Relaxed),
            proposal_max_batch: self.counters.proposal_max_batch.load(Ordering::Relaxed),
            proposal_ns_total: self.counters.proposal_ns_total.load(Ordering::Relaxed),
            proposal_queue_wait_ns_total: self
                .counters
                .proposal_queue_wait_ns_total
                .load(Ordering::Relaxed),
            proposal_queue_max_wait_ns: self
                .counters
                .proposal_queue_max_wait_ns
                .load(Ordering::Relaxed),
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
    M: MetadataCheckpointStore + MetadataStore + Clone + Send + Sync + 'static,
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
    M: MetadataCheckpointStore + MetadataStore + Clone + Send + Sync + 'static,
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
    M: MetadataCheckpointStore + MetadataStore + Clone + Send + Sync + 'static,
{
    pub fn stats_handle(&self) -> OpenRaftMetadataStatsHandle {
        OpenRaftMetadataStatsHandle {
            metrics: self.raft.metrics(),
            counters: Arc::clone(&self.counters),
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
        let raft = Self::new_initialized_voter_group_with_file_log_and_network(
            store,
            node,
            log_path,
            options,
            network,
            &single_voter_members(node)?,
        )?;
        raft.wait_for_current_leader(node, Duration::from_secs(3))?;
        Ok(raft)
    }

    pub fn new_initialized_voter_group_with_file_log_and_network<N>(
        store: M,
        node: NodeId,
        log_path: impl AsRef<Path>,
        options: FileMetadataRaftLogOptions,
        network: N,
        voters: &BTreeMap<NodeId, String>,
    ) -> Result<Self, MetadataError>
    where
        N: RaftNetworkFactory<MetadataRaftConfig> + Send + Sync + 'static,
    {
        let (raft, _) = Self::new_voter_group_with_file_log_and_network(
            store, node, log_path, options, network, voters,
        )?;
        Ok(raft)
    }

    pub fn new_voter_group_with_file_log_and_network<N>(
        store: M,
        node: NodeId,
        log_path: impl AsRef<Path>,
        options: FileMetadataRaftLogOptions,
        network: N,
        voters: &BTreeMap<NodeId, String>,
    ) -> Result<(Self, bool), MetadataError>
    where
        N: RaftNetworkFactory<MetadataRaftConfig> + Send + Sync + 'static,
    {
        let log = FileMetadataRaftLog::open(log_path, options)
            .map_err(|err| MetadataError::Backend(format!("openraft file log: {err}")))?;
        let should_initialize = log.last_log_id().is_none();
        let raft = Self::new_with_log_and_network(store, node, log, network)?;
        if should_initialize {
            raft.initialize_voters(voters)?;
        }
        Ok((raft, should_initialize))
    }

    pub fn new_uninitialized_with_file_log_and_network<N>(
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
        Self::new_with_log_and_network(store, node, log, network)
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
        let raft =
            Self::new_with_log_and_network(store, node, log, NoopMetadataRaftNetworkFactory)?;
        if should_initialize {
            raft.initialize_voters(&single_voter_members(node)?)?;
        }
        raft.wait_for_current_leader(node, Duration::from_secs(3))?;
        Ok(raft)
    }

    fn new_with_log_and_network<L, N>(
        store: M,
        node: NodeId,
        log: L,
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
                        cluster_name: format!("nokv-metadata-{}", raft_node),
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

            Ok::<_, MetadataError>(raft)
        })?;
        Ok(Self {
            read_store: store,
            raft,
            runtime,
            counters: Arc::new(OpenRaftMetadataCounters::default()),
            coalescer: Arc::new(ProposalCoalescer::new(ProposalCoalescerOptions::default())),
        })
    }

    pub fn initialize_voters(
        &self,
        voters: &BTreeMap<NodeId, String>,
    ) -> Result<(), MetadataError> {
        let members = metadata_raft_members(voters)?;
        self.runtime
            .block_on(self.raft.initialize(members))
            .map_err(|err| MetadataError::Backend(format!("openraft initialize: {err}")))
    }

    pub fn add_learner(
        &self,
        node: NodeId,
        address: impl Into<String>,
        blocking: bool,
    ) -> Result<LogPosition, MetadataError> {
        let response = self
            .runtime
            .block_on(self.raft.add_learner(
                node.get(),
                BasicNode {
                    addr: address.into(),
                },
                blocking,
            ))
            .map_err(|err| {
                MetadataError::Backend(format!("openraft add learner {}: {err}", node.get()))
            })?;
        log_position_from_id(response.log_id)
    }

    pub fn replace_voters(
        &self,
        voters: &BTreeSet<NodeId>,
        retain_removed_as_learners: bool,
    ) -> Result<LogPosition, MetadataError> {
        if voters.is_empty() {
            return Err(MetadataError::Backend(
                "metadata raft requires at least one voter".to_owned(),
            ));
        }
        let voter_ids = voters
            .iter()
            .map(|node| node.get())
            .collect::<BTreeSet<_>>();
        let response = self
            .runtime
            .block_on(
                self.raft
                    .change_membership(voter_ids, retain_removed_as_learners),
            )
            .map_err(|err| MetadataError::Backend(format!("openraft change membership: {err}")))?;
        log_position_from_id(response.log_id)
    }

    pub fn wait_for_membership_counts(
        &self,
        voters: usize,
        learners: usize,
        timeout: Duration,
    ) -> Result<(), MetadataError> {
        self.runtime
            .block_on(self.raft.wait(Some(timeout)).metrics(
                |metrics| {
                    let membership = metrics.membership_config.membership();
                    membership.voter_ids().count() == voters
                        && membership.learner_ids().count() == learners
                },
                "metadata raft membership counts",
            ))
            .map_err(|err| MetadataError::Backend(format!("openraft membership wait: {err}")))?;
        Ok(())
    }

    pub fn wait_for_leader(&self, timeout: Duration) -> Result<NodeId, MetadataError> {
        let metrics = self
            .runtime
            .block_on(self.raft.wait(Some(timeout)).metrics(
                |metrics| metrics.current_leader.is_some(),
                "metadata raft leader",
            ))
            .map_err(|err| MetadataError::Backend(format!("openraft leader wait: {err}")))?;
        let leader = metrics.current_leader.ok_or_else(|| {
            MetadataError::Backend("openraft leader wait completed without leader".to_owned())
        })?;
        NodeId::new(leader).map_err(|err| {
            MetadataError::Backend(format!(
                "openraft reported invalid leader id {leader}: {err}"
            ))
        })
    }

    pub fn wait_for_current_leader(
        &self,
        leader: NodeId,
        timeout: Duration,
    ) -> Result<(), MetadataError> {
        self.runtime
            .block_on(
                self.raft
                    .wait(Some(timeout))
                    .current_leader(leader.get(), "metadata raft leader"),
            )
            .map_err(|err| MetadataError::Backend(format!("openraft leader wait: {err}")))?;
        Ok(())
    }

    pub fn shutdown(&self) -> Result<(), MetadataError> {
        self.runtime
            .block_on(self.raft.shutdown())
            .map_err(|err| MetadataError::Backend(format!("openraft shutdown: {err}")))
    }

    pub fn trigger_snapshot(&self) -> Result<(), MetadataError> {
        let target = self
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|log_id| log_id.index);
        self.runtime
            .block_on(self.raft.trigger().snapshot())
            .map_err(|err| MetadataError::Backend(format!("openraft snapshot: {err}")))?;
        if let Some(target) = target {
            self.runtime
                .block_on(self.raft.wait(Some(Duration::from_secs(3))).metrics(
                    |metrics| {
                        metrics
                            .snapshot
                            .map(|snapshot| snapshot.index >= target)
                            .unwrap_or(false)
                    },
                    "metadata raft snapshot",
                ))
                .map_err(|err| MetadataError::Backend(format!("openraft snapshot wait: {err}")))?;
        }
        Ok(())
    }

    pub fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        let applied_index = self
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|log_id| log_id.index)
            .unwrap_or(0);
        self.read_store.export_checkpoint_image(applied_index)
    }

    pub fn handle_vote_rpc(
        &self,
        request: WireMetadataRaftVoteRequest,
    ) -> Result<WireMetadataRaftVoteResponse, MetadataError> {
        let request = wire::vote_request(request)
            .map_err(|err| MetadataError::Backend(format!("metadata raft vote decode: {err}")))?;
        let response = self
            .runtime
            .block_on(self.raft.vote(request))
            .map_err(|err| MetadataError::Backend(format!("metadata raft vote: {err}")))?;
        Ok(wire::wire_vote_response(&response))
    }

    pub fn handle_append_entries_rpc(
        &self,
        request: WireMetadataRaftAppendEntriesRequest,
    ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataError> {
        let request = wire::append_entries_request(request).map_err(|err| {
            MetadataError::Backend(format!("metadata raft append entries decode: {err}"))
        })?;
        let response = self
            .runtime
            .block_on(self.raft.append_entries(request))
            .map_err(|err| {
                MetadataError::Backend(format!("metadata raft append entries: {err}"))
            })?;
        Ok(wire::wire_append_entries_response(&response))
    }

    pub fn handle_install_snapshot_rpc(
        &self,
        request: WireMetadataRaftInstallSnapshotRequest,
    ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataError> {
        let request = wire::install_snapshot_request(request).map_err(|err| {
            MetadataError::Backend(format!("metadata raft install snapshot decode: {err}"))
        })?;
        let response = self
            .runtime
            .block_on(self.raft.install_snapshot(request))
            .map_err(|err| {
                MetadataError::Backend(format!("metadata raft install snapshot: {err}"))
            })?;
        Ok(wire::wire_install_snapshot_response(&response))
    }

    fn commit_batch_direct(
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
        let started = Instant::now();
        let command_count = commands.len();
        let result = match self.runtime.block_on(self.raft.client_write(batch)) {
            Ok(response) => normalize_apply_results(commands.len(), response.data.results),
            Err(err) => {
                let error = if let Some(forward) = err.forward_to_leader::<BasicNode>() {
                    MetadataError::ForwardToLeader {
                        leader_id: forward.leader_id,
                        address: forward.leader_node.as_ref().map(|node| node.addr.clone()),
                    }
                } else {
                    MetadataError::Backend(format!("openraft client write: {err}"))
                };
                commands.iter().map(|_| Err(error.clone())).collect()
            }
        };
        self.counters
            .proposal_batch_total
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .proposal_command_total
            .fetch_add(command_count as u64, Ordering::Relaxed);
        self.counters
            .proposal_max_batch
            .fetch_max(command_count as u64, Ordering::Relaxed);
        self.counters.proposal_ns_total.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        result
    }

    fn commit_single_via_coalescer(
        &self,
        command: MetadataCommand,
    ) -> Result<CommitResult, MetadataError> {
        if self.coalescer.options.max_commands <= 1 {
            return self
                .commit_batch_direct(std::slice::from_ref(&command))
                .into_iter()
                .next()
                .unwrap_or_else(|| {
                    Err(MetadataError::Backend(
                        "openraft commit returned no result".to_owned(),
                    ))
                });
        }
        let result = Arc::new(Mutex::new(None));
        let queued = QueuedProposal {
            estimated_bytes: estimated_command_bytes(&command),
            command,
            enqueued_at: Instant::now(),
            result: Arc::clone(&result),
        };
        {
            let mut state = self.coalescer.inner.lock().map_err(|_| {
                MetadataError::Backend("metadata raft coalescer lock poisoned".to_owned())
            })?;
            state.queue.push_back(queued);
            self.coalescer.cvar.notify_all();
        }
        loop {
            if let Some(result) = result
                .lock()
                .map_err(|_| {
                    MetadataError::Backend("metadata raft proposal result lock poisoned".to_owned())
                })?
                .take()
            {
                return result;
            }
            let mut should_drain = false;
            {
                let mut state = self.coalescer.inner.lock().map_err(|_| {
                    MetadataError::Backend("metadata raft coalescer lock poisoned".to_owned())
                })?;
                if !state.draining {
                    state.draining = true;
                    should_drain = true;
                } else {
                    drop(self.coalescer.cvar.wait(state).map_err(|_| {
                        MetadataError::Backend(
                            "metadata raft coalescer wait lock poisoned".to_owned(),
                        )
                    })?);
                }
            }
            if should_drain {
                self.drain_coalesced_proposals();
            }
        }
    }

    fn drain_coalesced_proposals(&self) {
        let mut proposals = {
            let mut state = match self.coalescer.inner.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            let mut queue_len = state.queue.len();
            let mut deadline = Instant::now() + self.coalescer.options.max_delay;
            while !coalescer_should_flush(&state, self.coalescer.options) {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let timeout = deadline.saturating_duration_since(now);
                let (next_state, _) = match self.coalescer.cvar.wait_timeout(state, timeout) {
                    Ok(pair) => pair,
                    Err(_) => return,
                };
                state = next_state;
                if state.queue.len() > queue_len {
                    queue_len = state.queue.len();
                    deadline = Instant::now() + self.coalescer.options.max_delay;
                }
            }
            let mut bytes = 0_usize;
            let mut count = 0_usize;
            for queued in &state.queue {
                let next_bytes = bytes.saturating_add(queued.estimated_bytes);
                if count > 0 && next_bytes > self.coalescer.options.max_bytes {
                    break;
                }
                bytes = next_bytes;
                count += 1;
                if count >= self.coalescer.options.max_commands {
                    break;
                }
            }
            state.queue.drain(..count).collect::<Vec<_>>()
        };
        if proposals.is_empty() {
            if let Ok(mut state) = self.coalescer.inner.lock() {
                state.draining = false;
                self.coalescer.cvar.notify_all();
            }
            return;
        }
        let now = Instant::now();
        let queue_wait_ns = proposals
            .iter()
            .map(|proposal| {
                u64::try_from(now.duration_since(proposal.enqueued_at).as_nanos())
                    .unwrap_or(u64::MAX)
            })
            .collect::<Vec<_>>();
        let commands = proposals
            .iter()
            .map(|proposal| proposal.command.clone())
            .collect::<Vec<_>>();
        let results = self.commit_batch_direct(&commands);
        for (proposal, result) in proposals.drain(..).zip(results) {
            if let Ok(mut slot) = proposal.result.lock() {
                *slot = Some(result);
            }
        }
        for wait_ns in queue_wait_ns {
            self.counters
                .proposal_queue_wait_ns_total
                .fetch_add(wait_ns, Ordering::Relaxed);
            self.counters
                .proposal_queue_max_wait_ns
                .fetch_max(wait_ns, Ordering::Relaxed);
        }
        if let Ok(mut state) = self.coalescer.inner.lock() {
            state.draining = false;
            self.coalescer.cvar.notify_all();
        }
    }
}

impl<M> MetadataStore for OpenRaftMetadataStore<M>
where
    M: MetadataCheckpointStore + MetadataStore + Clone + Send + Sync + 'static,
{
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: nokv_meta::Version,
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
        self.commit_single_via_coalescer(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.commit_batch_direct(commands)
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
    M: MetadataCheckpointStore
        + MetadataStore
        + MetadataStoreStatsProvider
        + Clone
        + Send
        + Sync
        + 'static,
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
    M: MetadataCheckpointStore + MetadataStore + Clone + Send + Sync + 'static,
{
    type SnapshotBuilder = MetadataRaftSnapshotBuilder<M>;

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
            store: self.store.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
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
        let checkpoint_applied = self.store.install_checkpoint_image(&data).map_err(|err| {
            metadata_storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err)
        })?;
        let expected_applied = meta.last_log_id.map(|log_id| log_id.index).unwrap_or(0);
        if checkpoint_applied != expected_applied {
            return Err(metadata_storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                MetadataError::Backend(format!(
                    "metadata checkpoint applied index {checkpoint_applied} does not match snapshot meta {expected_applied}"
                )),
            ));
        }
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot = Some(MetadataRaftSnapshotImage {
            meta: meta.clone(),
            data,
        });
        self.store.reclaim_unreachable_storage().map_err(|err| {
            metadata_storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Delete, err)
        })?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetadataRaftConfig>>, StorageError<u64>> {
        Ok(self.current_snapshot.clone().map(snapshot_from_image))
    }
}

impl<M> RaftSnapshotBuilder<MetadataRaftConfig> for MetadataRaftSnapshotBuilder<M>
where
    M: MetadataCheckpointStore + Clone + Send + Sync + 'static,
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetadataRaftConfig>, StorageError<u64>> {
        let applied_index = self.last_applied.map(|log_id| log_id.index).unwrap_or(0);
        self.store.commit_durable(applied_index).map_err(|err| {
            metadata_storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, err)
        })?;
        let data = self
            .store
            .export_checkpoint_image(applied_index)
            .map_err(|err| {
                metadata_storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, err)
            })?;
        self.store.reclaim_unreachable_storage().map_err(|err| {
            metadata_storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Delete, err)
        })?;
        Ok(snapshot_from_image(MetadataRaftSnapshotImage {
            meta: SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.last_membership.clone(),
                snapshot_id: snapshot_id(self.last_applied),
            },
            data,
        }))
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

fn storage_error(subject: ErrorSubject<u64>, verb: ErrorVerb, message: &str) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, std::io::Error::other(message.to_owned()))
}

fn metadata_storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    err: MetadataError,
) -> StorageError<u64> {
    storage_error(subject, verb, &err.to_string())
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

fn coalescer_should_flush(
    state: &ProposalCoalescerState,
    options: ProposalCoalescerOptions,
) -> bool {
    if state.queue.is_empty() {
        return true;
    }
    if state.queue.len() >= options.max_commands.max(1) {
        return true;
    }
    let bytes = state
        .queue
        .iter()
        .map(|proposal| proposal.estimated_bytes)
        .fold(0_usize, usize::saturating_add);
    bytes >= options.max_bytes.max(1)
}

fn estimated_command_bytes(command: &MetadataCommand) -> usize {
    let predicates = command
        .predicates
        .iter()
        .map(|predicate| predicate.key.len() + 16)
        .fold(0_usize, usize::saturating_add);
    let mutations = command
        .mutations
        .iter()
        .map(|mutation| {
            mutation.key.len()
                + mutation
                    .value
                    .as_ref()
                    .map(|value| value.0.len())
                    .unwrap_or(0)
                + 16
        })
        .fold(0_usize, usize::saturating_add);
    let watches = command
        .watch
        .iter()
        .map(|watch| watch.key.len() + watch.event.len() + 16)
        .fold(0_usize, usize::saturating_add);
    64_usize
        .saturating_add(command.request_id.len())
        .saturating_add(command.primary_key.len())
        .saturating_add(predicates)
        .saturating_add(mutations)
        .saturating_add(watches)
}

fn single_voter_members(node: NodeId) -> Result<BTreeMap<NodeId, String>, MetadataError> {
    let mut voters = BTreeMap::new();
    voters.insert(node, format!("local-{}", node.get()));
    Ok(voters)
}

fn metadata_raft_members(
    voters: &BTreeMap<NodeId, String>,
) -> Result<BTreeMap<u64, BasicNode>, MetadataError> {
    if voters.is_empty() {
        return Err(MetadataError::Backend(
            "metadata raft voters cannot be empty".to_owned(),
        ));
    }
    Ok(voters
        .iter()
        .map(|(node, address)| {
            (
                node.get(),
                BasicNode {
                    addr: address.clone(),
                },
            )
        })
        .collect())
}

fn log_position_from_id(log_id: LogId<u64>) -> Result<LogPosition, MetadataError> {
    let term = LogTerm::new(log_id.leader_id.term)
        .map_err(|err| MetadataError::Backend(format!("openraft returned invalid term: {err}")))?;
    let index = LogIndex::new(log_id.index)
        .map_err(|err| MetadataError::Backend(format!("openraft returned invalid index: {err}")))?;
    Ok(LogPosition { term, index })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    use nokv_meta::command::{
        CommandKind, MetadataCommand, Mutation, MutationOp, Predicate, PredicateRef, ReadPurpose,
        Value, Version, WatchProjection,
    };
    use nokv_meta::holtstore::HoltMetadataStore;
    use nokv_protocol::{
        WireMetadataRaftAppendEntriesRequest, WireMetadataRaftAppendEntriesResponse,
        WireMetadataRaftInstallSnapshotRequest, WireMetadataRaftInstallSnapshotResponse,
        WireMetadataRaftVoteRequest, WireMetadataRaftVoteResponse,
    };
    use nokv_types::RecordFamily;
    use openraft::entry::FromAppData;
    use openraft::storage::RaftLogStorageExt;
    use openraft::{
        BasicNode, CommittedLeaderId, Config, LogId, Raft, RaftLogReader, RaftSnapshotBuilder, Vote,
    };

    use super::*;
    use crate::file_log::{FileMetadataRaftLog, FileMetadataRaftLogOptions};
    use crate::log::MetadataRaftCommandBatch;
    use crate::network::{MetadataRaftRpcClient, MetadataRaftRpcNetworkFactory};
    use crate::MetadataRaftError;

    fn persistent_holt_store(root: &Path, name: &str) -> HoltMetadataStore {
        HoltMetadataStore::open_raft_materialized(root.join(name)).unwrap()
    }

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
            let dir = tempfile::tempdir().unwrap();
            let store = persistent_holt_store(dir.path(), "metadata-state.holt");
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
    fn file_metadata_raft_log_compacts_after_purge() {
        runtime().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("metadata-raft.log");
            let entries = (1..=12)
                .map(|index| {
                    metadata_entry(
                        4,
                        index,
                        metadata_command(
                            format!("req-compact-{index}").as_bytes(),
                            format!("dentry/compact/{index}").as_bytes(),
                            index + 20,
                        ),
                    )
                })
                .collect::<Vec<_>>();
            let mut log =
                FileMetadataRaftLog::open(&path, FileMetadataRaftLogOptions::default()).unwrap();
            log.blocking_append(entries.clone()).await.unwrap();
            let before = std::fs::metadata(&path).unwrap().len();

            log.purge(entries[9].log_id).await.unwrap();
            let after = std::fs::metadata(&path).unwrap().len();
            assert!(
                after < before,
                "purge should compact the physical metadata raft log: before={before} after={after}"
            );
            drop(log);

            let mut reopened =
                FileMetadataRaftLog::open(&path, FileMetadataRaftLogOptions::default()).unwrap();
            assert_eq!(
                reopened.get_log_state().await.unwrap().last_purged_log_id,
                Some(entries[9].log_id)
            );
            assert_eq!(
                reopened.try_get_log_entries(1..11).await.unwrap(),
                Vec::<MetadataRaftEntry>::new()
            );
            assert_eq!(
                reopened.try_get_log_entries(11..13).await.unwrap(),
                entries[10..].to_vec()
            );
        });
    }

    #[test]
    fn state_machine_snapshot_installs_holt_checkpoint_image() {
        runtime().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = persistent_holt_store(dir.path(), "metadata-state.holt");
            let mut sm = MetadataRaftStateMachine::new(store.clone());
            let command = metadata_command(b"req-snapshot", b"dentry/snapshot", 6);
            let entry = metadata_entry(2, 5, command);

            sm.apply([entry.clone()]).await.unwrap();
            let mut builder = sm.get_snapshot_builder().await;
            let snapshot = builder.build_snapshot().await.unwrap();
            assert!(
                !snapshot.snapshot.get_ref().is_empty(),
                "OpenRaft snapshot must include the Holt checkpoint image"
            );

            let restored = persistent_holt_store(dir.path(), "restored-state.holt");
            let mut restored_sm = MetadataRaftStateMachine::new(restored.clone());
            restored_sm
                .install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
                .unwrap();

            assert_eq!(
                restored
                    .get(
                        RecordFamily::Dentry,
                        b"dentry/snapshot",
                        Version::new(6).unwrap(),
                        ReadPurpose::UserStrong,
                    )
                    .unwrap()
                    .unwrap()
                    .0,
                b"inode=2".to_vec()
            );
            assert_eq!(
                restored_sm.applied_state().await.unwrap().0,
                Some(entry.log_id)
            );
        });
    }

    #[test]
    fn single_node_openraft_applies_client_write() {
        runtime().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = persistent_holt_store(dir.path(), "metadata-state.holt");
            let storage = MetadataRaftStorage::in_memory(store);
            let raft = Raft::new(
                1,
                Arc::new(
                    Config {
                        cluster_name: "nokv-test".to_owned(),
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
        let dir = tempfile::tempdir().unwrap();
        let store = persistent_holt_store(dir.path(), "metadata-state.holt");
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
    fn openraft_metadata_store_coalesces_concurrent_single_command_commits() {
        let dir = tempfile::tempdir().unwrap();
        let store = persistent_holt_store(dir.path(), "metadata-state.holt");
        let raft_store = Arc::new(
            OpenRaftMetadataStore::new_single_voter(store, NodeId::new(1).unwrap())
                .expect("single voter OpenRaft store opens"),
        );
        let barrier = Arc::new(Barrier::new(16));
        let mut handles = Vec::new();
        for index in 0..16_u64 {
            let raft_store = Arc::clone(&raft_store);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let request_id = format!("req-coalesced-{index}");
                let key = format!("dentry/coalesced-{index}");
                barrier.wait();
                raft_store
                    .commit_metadata(metadata_command(
                        request_id.as_bytes(),
                        key.as_bytes(),
                        100 + index,
                    ))
                    .unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let stats = raft_store.stats_handle().stats();
        assert!(
            stats.proposal_max_batch > 1,
            "expected coalesced raft proposal batch, got max={}",
            stats.proposal_max_batch
        );
        assert!(stats.proposal_queue_wait_ns_total > 0);
        raft_store.shutdown().unwrap();
    }

    #[test]
    fn openraft_metadata_store_file_log_replays_state_machine_and_commits() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("metadata-raft.log");
        let state_path = dir.path().join("metadata-state.holt");
        let first_store = HoltMetadataStore::open_raft_materialized(&state_path).unwrap();
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

        let reopened_store = HoltMetadataStore::open_raft_materialized(&state_path).unwrap();
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

    #[test]
    fn three_node_openraft_group_replicates_client_write() {
        let cluster = TestMetadataRaftCluster::start(&[1, 2, 3]);
        let leader = cluster.wait_for_leader(None);

        let command = metadata_command(b"req-three-node", b"dentry/three-node", 11);
        let result = cluster.node(leader).commit_metadata(command).unwrap();
        assert_eq!(result.commit_version, Version::new(11).unwrap());

        cluster.wait_for_key_on_all(b"dentry/three-node", 11, None);
        cluster.shutdown_all(None);
    }

    #[test]
    fn three_node_openraft_group_elects_new_leader_after_leader_shutdown() {
        let cluster = TestMetadataRaftCluster::start(&[1, 2, 3]);
        let leader = cluster.wait_for_leader(None);

        cluster.client.unregister(leader);
        cluster.node(leader).shutdown().unwrap();
        let new_leader = cluster.wait_for_leader(Some(leader));

        let command = metadata_command(b"req-after-crash", b"dentry/after-crash", 12);
        let result = cluster.node(new_leader).commit_metadata(command).unwrap();
        assert_eq!(result.commit_version, Version::new(12).unwrap());

        cluster.wait_for_key_on_all(b"dentry/after-crash", 12, Some(leader));
        cluster.shutdown_all(Some(leader));
    }

    #[test]
    fn openraft_membership_adds_promotes_and_removes_nodes() {
        let mut cluster = TestMetadataRaftCluster::start_with_voters(&[1, 2, 3, 4], &[1, 2, 3]);
        let leader = cluster.wait_for_leader(None);
        cluster.wait_for_membership_counts(3, 0);
        cluster
            .node(leader)
            .commit_metadata(metadata_command(
                b"req-membership-bootstrap",
                b"dentry/membership-bootstrap",
                12,
            ))
            .unwrap();
        cluster.wait_for_key_on_nodes(b"dentry/membership-bootstrap", 12, &[1, 2, 3]);

        cluster
            .node(leader)
            .add_learner(NodeId::new(4).unwrap(), "metadata-raft-node-4", true)
            .unwrap();
        cluster.wait_for_membership_counts(3, 1);

        let learner_command = metadata_command(b"req-learner", b"dentry/learner", 13);
        cluster
            .node(leader)
            .commit_metadata(learner_command)
            .unwrap();
        cluster.wait_for_key_on_nodes(b"dentry/learner", 13, &[1, 2, 3, 4]);

        cluster
            .node(leader)
            .replace_voters(&node_set(&[1, 2, 3, 4]), true)
            .unwrap();
        cluster.wait_for_membership_counts(4, 0);

        let promoted_leader = cluster.wait_for_leader(None);
        let promoted_command = metadata_command(b"req-promoted", b"dentry/promoted", 14);
        cluster
            .node(promoted_leader)
            .commit_metadata(promoted_command)
            .unwrap();
        cluster.wait_for_key_on_nodes(b"dentry/promoted", 14, &[1, 2, 3, 4]);

        cluster.restart_node(4);
        cluster.wait_for_membership_counts(4, 0);
        let restart_leader = cluster.wait_for_leader(None);
        let restart_command = metadata_command(b"req-after-restart", b"dentry/after-restart", 15);
        cluster
            .node(restart_leader)
            .commit_metadata(restart_command)
            .unwrap();
        cluster.wait_for_key_on_nodes(b"dentry/after-restart", 15, &[1, 2, 3, 4]);

        cluster
            .node(restart_leader)
            .replace_voters(&node_set(&[1, 3, 4]), false)
            .unwrap();
        cluster.wait_for_membership_counts(3, 0);

        let reduced_leader = cluster.wait_for_leader(Some(2));
        let reduced_command = metadata_command(b"req-reduced", b"dentry/reduced", 16);
        cluster
            .node(reduced_leader)
            .commit_metadata(reduced_command)
            .unwrap();
        cluster.wait_for_key_on_nodes(b"dentry/reduced", 16, &[1, 3, 4]);

        cluster.shutdown_all(None);
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

    fn node_set(ids: &[u64]) -> BTreeSet<NodeId> {
        ids.iter()
            .map(|id| NodeId::new(*id).unwrap())
            .collect::<BTreeSet<_>>()
    }

    struct TestMetadataRaftCluster {
        _dir: tempfile::TempDir,
        client: DirectMetadataRaftRpcClient,
        nodes: BTreeMap<u64, Arc<OpenRaftMetadataStore<HoltMetadataStore>>>,
    }

    impl TestMetadataRaftCluster {
        fn start(ids: &[u64]) -> Self {
            Self::start_with_voters(ids, ids)
        }

        fn start_with_voters(ids: &[u64], voter_ids: &[u64]) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let client = DirectMetadataRaftRpcClient::default();
            let mut voters = BTreeMap::new();
            for id in voter_ids {
                voters.insert(
                    NodeId::new(*id).unwrap(),
                    format!("metadata-raft-node-{id}"),
                );
            }

            let mut nodes = BTreeMap::new();
            for id in ids {
                let node = NodeId::new(*id).unwrap();
                let store = HoltMetadataStore::open_raft_materialized(
                    dir.path().join(format!("metadata-state-{id}.holt")),
                )
                .unwrap();
                let raft = Arc::new(
                    OpenRaftMetadataStore::new_uninitialized_with_file_log_and_network(
                        store,
                        node,
                        dir.path().join(format!("metadata-raft-{id}.log")),
                        FileMetadataRaftLogOptions::default(),
                        MetadataRaftRpcNetworkFactory::new(client.clone()),
                    )
                    .unwrap(),
                );
                client.register(*id, Arc::clone(&raft));
                nodes.insert(*id, raft);
            }

            nodes
                .get(&voter_ids[0])
                .unwrap()
                .initialize_voters(&voters)
                .unwrap();
            Self {
                _dir: dir,
                client,
                nodes,
            }
        }

        fn node(&self, id: u64) -> Arc<OpenRaftMetadataStore<HoltMetadataStore>> {
            Arc::clone(self.nodes.get(&id).unwrap())
        }

        fn restart_node(&mut self, id: u64) {
            self.client.unregister(id);
            if let Some(old) = self.nodes.remove(&id) {
                old.shutdown().unwrap();
            }
            let node = NodeId::new(id).unwrap();
            let store = HoltMetadataStore::open_raft_materialized(
                self._dir.path().join(format!("metadata-state-{id}.holt")),
            )
            .unwrap();
            let raft = Arc::new(
                OpenRaftMetadataStore::new_uninitialized_with_file_log_and_network(
                    store,
                    node,
                    self._dir.path().join(format!("metadata-raft-{id}.log")),
                    FileMetadataRaftLogOptions::default(),
                    MetadataRaftRpcNetworkFactory::new(self.client.clone()),
                )
                .unwrap(),
            );
            self.client.register(id, Arc::clone(&raft));
            self.nodes.insert(id, raft);
        }

        fn wait_for_membership_counts(&self, voters: usize, learners: usize) {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                if self.nodes.values().any(|node| {
                    node.wait_for_membership_counts(voters, learners, Duration::from_millis(100))
                        .is_ok()
                }) {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "metadata raft cluster did not reach membership counts voters={voters} learners={learners}"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }

        fn wait_for_leader(&self, excluded: Option<u64>) -> u64 {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                for (id, node) in &self.nodes {
                    if excluded == Some(*id) {
                        continue;
                    }
                    if let Ok(leader) = node.wait_for_leader(Duration::from_millis(100)) {
                        let leader = leader.get();
                        if excluded != Some(leader) && self.nodes.contains_key(&leader) {
                            return leader;
                        }
                    }
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "metadata raft cluster did not elect a usable leader"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }

        fn wait_for_key_on_all(&self, key: &[u8], version: u64, excluded: Option<u64>) {
            let ids = self
                .nodes
                .keys()
                .copied()
                .filter(|id| excluded != Some(*id))
                .collect::<Vec<_>>();
            self.wait_for_key_on_nodes(key, version, &ids);
        }

        fn wait_for_key_on_nodes(&self, key: &[u8], version: u64, ids: &[u64]) {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                let all_visible = ids.iter().all(|id| {
                    self.nodes
                        .get(id)
                        .unwrap()
                        .get(
                            RecordFamily::Dentry,
                            key,
                            Version::new(version).unwrap(),
                            ReadPurpose::UserStrong,
                        )
                        .unwrap()
                        .is_some()
                });
                if all_visible {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "metadata raft key did not replicate to requested nodes"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }

        fn shutdown_all(&self, excluded: Option<u64>) {
            for (id, node) in &self.nodes {
                if excluded != Some(*id) {
                    node.shutdown().unwrap();
                }
            }
        }
    }

    #[derive(Clone, Default)]
    struct DirectMetadataRaftRpcClient {
        peers: Arc<Mutex<BTreeMap<u64, Arc<OpenRaftMetadataStore<HoltMetadataStore>>>>>,
        unavailable: Arc<Mutex<BTreeSet<u64>>>,
    }

    impl DirectMetadataRaftRpcClient {
        fn register(&self, id: u64, peer: Arc<OpenRaftMetadataStore<HoltMetadataStore>>) {
            self.unavailable.lock().unwrap().remove(&id);
            self.peers.lock().unwrap().insert(id, peer);
        }

        fn unregister(&self, id: u64) {
            self.unavailable.lock().unwrap().insert(id);
            self.peers.lock().unwrap().remove(&id);
        }

        fn peer(
            &self,
            target: u64,
        ) -> Result<Arc<OpenRaftMetadataStore<HoltMetadataStore>>, MetadataRaftError> {
            if self.unavailable.lock().unwrap().contains(&target) {
                return Err(MetadataRaftError::Backend(format!(
                    "metadata raft test peer {target} is unavailable"
                )));
            }
            self.peers
                .lock()
                .unwrap()
                .get(&target)
                .cloned()
                .ok_or_else(|| {
                    MetadataRaftError::Backend(format!(
                        "metadata raft test peer {target} is unknown"
                    ))
                })
        }
    }

    impl MetadataRaftRpcClient for DirectMetadataRaftRpcClient {
        fn vote_metadata_raft(
            &self,
            target: u64,
            _address: &str,
            request: WireMetadataRaftVoteRequest,
        ) -> Result<WireMetadataRaftVoteResponse, MetadataRaftError> {
            self.peer(target)?
                .handle_vote_rpc(request)
                .map_err(|err| MetadataRaftError::Backend(err.to_string()))
        }

        fn append_metadata_raft_entries(
            &self,
            target: u64,
            _address: &str,
            request: WireMetadataRaftAppendEntriesRequest,
        ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataRaftError> {
            self.peer(target)?
                .handle_append_entries_rpc(request)
                .map_err(|err| MetadataRaftError::Backend(err.to_string()))
        }

        fn install_metadata_raft_snapshot(
            &self,
            target: u64,
            _address: &str,
            request: WireMetadataRaftInstallSnapshotRequest,
        ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataRaftError> {
            self.peer(target)?
                .handle_install_snapshot_rpc(request)
                .map_err(|err| MetadataRaftError::Backend(err.to_string()))
        }
    }
}
