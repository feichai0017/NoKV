use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, sync_channel, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use nokvfs_cluster::{
    CheckpointCatalog, DurableReceipt, FileCheckpointCatalog, FileSharedLog,
    InstallCheckpointRequest, LearnerBootstrapPlan, LogIndex, LogPosition, LogTerm,
    MetadataLogEntry, MetadataMembership, NodeId, SharedLogError, SharedMetadataLog,
};
use nokvfs_meta::command::MetadataCommand;
use nokvfs_types::MountId;

use crate::options::MetadataLogPeerOptions;
use crate::rpc;

const LEARNER_REPLICATION_QUEUE_CAPACITY: usize = 1024;

pub(crate) struct MajorityMetadataLog {
    local_node: NodeId,
    membership: MetadataMembership,
    local: Arc<FileSharedLog>,
    peers: BTreeMap<NodeId, Arc<dyn MetadataLogPeerAppender>>,
    bootstrapper: Option<Arc<dyn MetadataLogPeerBootstrapper>>,
    learner_replicators: BTreeMap<NodeId, LearnerReplicator>,
    replication_stats: Arc<MajorityMetadataLogReplicationCounters>,
    append_gate: Mutex<()>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MajorityMetadataLogReplicationStats {
    pub remote_voter_append_total: u64,
    pub remote_voter_append_success_total: u64,
    pub remote_voter_append_failure_total: u64,
    pub remote_voter_quorum_success_total: u64,
    pub remote_voter_quorum_failure_total: u64,
    pub remote_voter_quorum_wait_ns_total: u64,
    pub learner_wakeup_total: u64,
    pub learner_wakeup_coalesced_total: u64,
    pub learner_wakeup_disconnected_total: u64,
    pub learner_catchup_success_total: u64,
    pub learner_catchup_failure_total: u64,
    pub learner_catchup_ns_total: u64,
}

#[derive(Debug, Default)]
struct MajorityMetadataLogReplicationCounters {
    remote_voter_append_total: AtomicU64,
    remote_voter_append_success_total: AtomicU64,
    remote_voter_append_failure_total: AtomicU64,
    remote_voter_quorum_success_total: AtomicU64,
    remote_voter_quorum_failure_total: AtomicU64,
    remote_voter_quorum_wait_ns_total: AtomicU64,
    learner_wakeup_total: AtomicU64,
    learner_wakeup_coalesced_total: AtomicU64,
    learner_wakeup_disconnected_total: AtomicU64,
    learner_catchup_success_total: AtomicU64,
    learner_catchup_failure_total: AtomicU64,
    learner_catchup_ns_total: AtomicU64,
}

pub(crate) trait MetadataLogPeerAppender: Send + Sync {
    fn append_entry(&self, leader: NodeId, entry: &MetadataLogEntry) -> Result<(), SharedLogError>;
}

pub(crate) trait MetadataLogPeerBootstrapper: Send + Sync {
    fn bootstrap_before(&self, peer: NodeId, before: LogIndex) -> Result<(), SharedLogError>;
}

pub(crate) struct FramedMetadataLogPeer {
    client: rpc::FramedRpcClient,
}

struct FramedMetadataLogPeerBootstrapper {
    local_node: NodeId,
    mount: MountId,
    local: Arc<FileSharedLog>,
    checkpoints: Arc<FileCheckpointCatalog>,
    peers: BTreeMap<NodeId, SocketAddr>,
}

struct LearnerReplicator {
    sender: SyncSender<()>,
    target_index: Arc<AtomicU64>,
    replication_stats: Arc<MajorityMetadataLogReplicationCounters>,
    _worker: thread::JoinHandle<()>,
}

impl MajorityMetadataLog {
    pub(crate) fn new(
        local_node: NodeId,
        membership: MetadataMembership,
        local: Arc<FileSharedLog>,
        peer_options: &[MetadataLogPeerOptions],
        checkpoints: Option<Arc<FileCheckpointCatalog>>,
    ) -> Self {
        let peer_addresses = peer_options
            .iter()
            .map(|peer| (peer.node, peer.address))
            .collect::<BTreeMap<_, _>>();
        let peers = peer_addresses
            .iter()
            .map(|(node, address)| {
                (
                    *node,
                    Arc::new(FramedMetadataLogPeer::new(*address))
                        as Arc<dyn MetadataLogPeerAppender>,
                )
            })
            .collect();
        let bootstrapper = checkpoints.map(|checkpoints| {
            Arc::new(FramedMetadataLogPeerBootstrapper {
                local_node,
                mount: membership.mount,
                local: Arc::clone(&local),
                checkpoints,
                peers: peer_addresses,
            }) as Arc<dyn MetadataLogPeerBootstrapper>
        });
        Self::with_peers(local_node, membership, local, peers, bootstrapper)
    }

    pub(crate) fn append_entry(
        &self,
        entry: MetadataLogEntry,
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        self.local.append_entry(entry)
    }

    fn with_peers(
        local_node: NodeId,
        membership: MetadataMembership,
        local: Arc<FileSharedLog>,
        peers: BTreeMap<NodeId, Arc<dyn MetadataLogPeerAppender>>,
        bootstrapper: Option<Arc<dyn MetadataLogPeerBootstrapper>>,
    ) -> Self {
        let replication_stats = Arc::new(MajorityMetadataLogReplicationCounters::default());
        let learner_replicators = membership
            .learners
            .iter()
            .filter_map(|learner| {
                peers.get(learner).map(|peer| {
                    (
                        *learner,
                        LearnerReplicator::spawn(
                            local_node,
                            *learner,
                            Arc::clone(&local),
                            Arc::clone(peer),
                            bootstrapper.clone(),
                            Arc::clone(&replication_stats),
                        ),
                    )
                })
            })
            .collect();
        Self {
            local_node,
            membership,
            local,
            peers,
            bootstrapper,
            learner_replicators,
            replication_stats,
            append_gate: Mutex::new(()),
        }
    }

    pub(crate) fn replication_stats(&self) -> MajorityMetadataLogReplicationStats {
        MajorityMetadataLogReplicationStats {
            remote_voter_append_total: self
                .replication_stats
                .remote_voter_append_total
                .load(Ordering::Relaxed),
            remote_voter_append_success_total: self
                .replication_stats
                .remote_voter_append_success_total
                .load(Ordering::Relaxed),
            remote_voter_append_failure_total: self
                .replication_stats
                .remote_voter_append_failure_total
                .load(Ordering::Relaxed),
            remote_voter_quorum_success_total: self
                .replication_stats
                .remote_voter_quorum_success_total
                .load(Ordering::Relaxed),
            remote_voter_quorum_failure_total: self
                .replication_stats
                .remote_voter_quorum_failure_total
                .load(Ordering::Relaxed),
            remote_voter_quorum_wait_ns_total: self
                .replication_stats
                .remote_voter_quorum_wait_ns_total
                .load(Ordering::Relaxed),
            learner_wakeup_total: self
                .replication_stats
                .learner_wakeup_total
                .load(Ordering::Relaxed),
            learner_wakeup_coalesced_total: self
                .replication_stats
                .learner_wakeup_coalesced_total
                .load(Ordering::Relaxed),
            learner_wakeup_disconnected_total: self
                .replication_stats
                .learner_wakeup_disconnected_total
                .load(Ordering::Relaxed),
            learner_catchup_success_total: self
                .replication_stats
                .learner_catchup_success_total
                .load(Ordering::Relaxed),
            learner_catchup_failure_total: self
                .replication_stats
                .learner_catchup_failure_total
                .load(Ordering::Relaxed),
            learner_catchup_ns_total: self
                .replication_stats
                .learner_catchup_ns_total
                .load(Ordering::Relaxed),
        }
    }

    fn append_majority_entry(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: &[MetadataCommand],
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        if commands.is_empty() {
            return Err(SharedLogError::EmptyBatch);
        }
        let (entry, receipts) = {
            let _append = self.append_gate.lock().map_err(|_| {
                SharedLogError::Backend("metadata majority log mutex poisoned".to_owned())
            })?;
            self.validate_local_append(term)?;
            let position = self.local.next_append_position(term)?;
            let entry = MetadataLogEntry {
                position,
                mount,
                commands: commands.to_vec(),
            };
            let quorum = majority(self.membership.voters.len())?;
            if quorum > 1 {
                self.append_remote_voter_quorum(&entry, quorum)?;
            }
            let receipts = self.local.append_entry(entry.clone())?;
            (entry, receipts)
        };
        self.replicate_learners_after_commit(&entry);
        Ok(receipts)
    }

    fn append_remote_voter_quorum(
        &self,
        entry: &MetadataLogEntry,
        quorum: usize,
    ) -> Result<(), SharedLogError> {
        let required_remote = quorum.saturating_sub(1);
        let targets = self
            .membership
            .voters
            .iter()
            .filter_map(|voter| {
                if *voter == self.local_node {
                    return None;
                }
                self.peers.get(voter).map(|peer| (*voter, Arc::clone(peer)))
            })
            .collect::<Vec<_>>();
        let mut remote_successes = 0_usize;
        let remote_total = targets.len();
        let (result_tx, result_rx) = channel();
        let quorum_started = Instant::now();
        for (voter, peer) in targets {
            let result_tx = result_tx.clone();
            let local_node = self.local_node;
            let local = Arc::clone(&self.local);
            let bootstrapper = self.bootstrapper.clone();
            let entry = entry.clone();
            let replication_stats = Arc::clone(&self.replication_stats);
            thread::spawn(move || {
                replication_stats
                    .remote_voter_append_total
                    .fetch_add(1, Ordering::Relaxed);
                let result = append_peer_with_catchup_from_log(
                    local_node,
                    local.as_ref(),
                    bootstrapper.as_deref(),
                    voter,
                    peer.as_ref(),
                    &entry,
                );
                if result.is_ok() {
                    replication_stats
                        .remote_voter_append_success_total
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    replication_stats
                        .remote_voter_append_failure_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                let _ = result_tx.send(result.is_ok());
            });
        }
        drop(result_tx);
        for remote_result in result_rx.iter().take(remote_total) {
            if remote_result {
                remote_successes = remote_successes.saturating_add(1);
                if remote_successes >= required_remote {
                    self.replication_stats
                        .remote_voter_quorum_success_total
                        .fetch_add(1, Ordering::Relaxed);
                    record_elapsed_ns(
                        &self.replication_stats.remote_voter_quorum_wait_ns_total,
                        quorum_started,
                    );
                    return Ok(());
                }
            }
        }
        if remote_successes < required_remote {
            self.replication_stats
                .remote_voter_quorum_failure_total
                .fetch_add(1, Ordering::Relaxed);
            record_elapsed_ns(
                &self.replication_stats.remote_voter_quorum_wait_ns_total,
                quorum_started,
            );
            return Err(SharedLogError::NoQuorum {
                required: quorum,
                available: remote_successes.saturating_add(1),
            });
        }
        Ok(())
    }

    fn replicate_learners_after_commit(&self, entry: &MetadataLogEntry) {
        for replicator in self.learner_replicators.values() {
            replicator.enqueue(entry);
        }
    }

    fn validate_local_append(&self, term: LogTerm) -> Result<(), SharedLogError> {
        if !self.membership.is_voter(self.local_node) {
            return Err(SharedLogError::LearnerCannotAppend(self.local_node));
        }
        self.membership.authorize_leader(self.local_node)?;
        if term < self.membership.term {
            return Err(SharedLogError::StaleTerm {
                current: self.membership.term,
                proposed: term,
            });
        }
        Ok(())
    }
}

impl LearnerReplicator {
    fn spawn(
        local_node: NodeId,
        learner: NodeId,
        local: Arc<FileSharedLog>,
        peer: Arc<dyn MetadataLogPeerAppender>,
        bootstrapper: Option<Arc<dyn MetadataLogPeerBootstrapper>>,
        replication_stats: Arc<MajorityMetadataLogReplicationCounters>,
    ) -> Self {
        let (sender, receiver) = sync_channel::<()>(LEARNER_REPLICATION_QUEUE_CAPACITY);
        let target_index = Arc::new(AtomicU64::new(0));
        let worker_target_index = Arc::clone(&target_index);
        let worker_replication_stats = Arc::clone(&replication_stats);
        let worker = thread::spawn(move || {
            while receiver.recv().is_ok() {
                let started = Instant::now();
                let result = replicate_learner_to_target(
                    local_node,
                    local.as_ref(),
                    bootstrapper.as_deref(),
                    learner,
                    peer.as_ref(),
                    worker_target_index.as_ref(),
                );
                if result.is_ok() {
                    worker_replication_stats
                        .learner_catchup_success_total
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    worker_replication_stats
                        .learner_catchup_failure_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                record_elapsed_ns(&worker_replication_stats.learner_catchup_ns_total, started);
            }
        });
        Self {
            sender,
            target_index,
            replication_stats,
            _worker: worker,
        }
    }

    fn enqueue(&self, entry: &MetadataLogEntry) {
        record_max(&self.target_index, entry.position.index.get());
        self.replication_stats
            .learner_wakeup_total
            .fetch_add(1, Ordering::Relaxed);
        match self.sender.try_send(()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.replication_stats
                    .learner_wakeup_coalesced_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.replication_stats
                    .learner_wakeup_disconnected_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl FramedMetadataLogPeer {
    fn new(address: SocketAddr) -> Self {
        Self {
            client: rpc::FramedRpcClient::new(address),
        }
    }
}

impl SharedMetadataLog for MajorityMetadataLog {
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: &[MetadataCommand],
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        self.append_majority_entry(term, mount, commands)
    }

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError> {
        self.local.read_from(start, limit)
    }

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError> {
        self.local.compact_through(index)
    }

    fn committed_position(&self) -> Option<LogPosition> {
        self.local.committed_position()
    }
}

impl MetadataLogPeerAppender for FramedMetadataLogPeer {
    fn append_entry(&self, leader: NodeId, entry: &MetadataLogEntry) -> Result<(), SharedLogError> {
        rpc::call_append_metadata_log_with_client(&self.client, leader, entry)
            .map_err(|err| SharedLogError::Backend(err.to_string()))
    }
}

impl MetadataLogPeerBootstrapper for FramedMetadataLogPeerBootstrapper {
    fn bootstrap_before(&self, peer: NodeId, before: LogIndex) -> Result<(), SharedLogError> {
        let address = *self
            .peers
            .get(&peer)
            .ok_or(SharedLogError::UnknownNode(peer))?;
        let checkpoint = self.checkpoints.latest_for_mount(self.mount)?.ok_or(
            SharedLogError::CheckpointRequired {
                node: peer,
                compacted: before,
            },
        )?;
        let replay_start = checkpoint.frontier.min_retained_index;
        let checkpoint_only = InstallCheckpointRequest::from_plan(
            self.local_node,
            LearnerBootstrapPlan {
                node: peer,
                checkpoint: checkpoint.clone(),
                replay_start,
                replayed_index: checkpoint.frontier.applied_position.index,
            },
        );
        rpc::call_install_metadata_checkpoint(address, checkpoint_only)
            .map_err(|err| SharedLogError::Backend(err.to_string()))?;
        let append_client = rpc::FramedRpcClient::new(address);
        for entry in self.local.read_from(replay_start, 0)? {
            if entry.position.index >= before {
                break;
            }
            rpc::call_append_metadata_log_with_client(&append_client, self.local_node, &entry)
                .map_err(|err| SharedLogError::Backend(err.to_string()))?;
        }
        Ok(())
    }
}

fn majority(voters: usize) -> Result<usize, SharedLogError> {
    if voters == 0 {
        return Err(SharedLogError::NoVoters);
    }
    Ok(voters / 2 + 1)
}

fn replicate_learner_to_target(
    local_node: NodeId,
    local: &FileSharedLog,
    bootstrapper: Option<&dyn MetadataLogPeerBootstrapper>,
    learner: NodeId,
    peer: &dyn MetadataLogPeerAppender,
    target_index: &AtomicU64,
) -> Result<(), SharedLogError> {
    loop {
        let target = target_index.load(Ordering::Relaxed);
        let Some(entry) = read_target_entry(local, target) else {
            return Ok(());
        };
        append_peer_with_catchup_from_log(local_node, local, bootstrapper, learner, peer, &entry)?;
        if target_index.load(Ordering::Relaxed) == target {
            return Ok(());
        }
    }
}

fn read_target_entry(local: &FileSharedLog, raw_index: u64) -> Option<MetadataLogEntry> {
    let index = LogIndex::new(raw_index).ok()?;
    local
        .read_from(index, 1)
        .ok()?
        .into_iter()
        .find(|entry| entry.position.index == index)
}

fn record_max(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn record_elapsed_ns(value: &AtomicU64, started: Instant) {
    let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
    value.fetch_add(elapsed.max(1), Ordering::Relaxed);
}

fn append_peer_with_catchup_from_log(
    local_node: NodeId,
    local: &FileSharedLog,
    bootstrapper: Option<&dyn MetadataLogPeerBootstrapper>,
    peer_node: NodeId,
    peer: &dyn MetadataLogPeerAppender,
    entry: &MetadataLogEntry,
) -> Result<(), SharedLogError> {
    match peer.append_entry(local_node, entry) {
        Ok(()) => Ok(()),
        Err(first_err) => {
            match catch_up_peer_from_log(local_node, local, peer, entry.position.index) {
                Ok(()) => {}
                Err(SharedLogError::Compacted { .. }) => {
                    let Some(bootstrapper) = bootstrapper else {
                        return Err(first_err);
                    };
                    bootstrapper.bootstrap_before(peer_node, entry.position.index)?;
                }
                Err(err) => return Err(err),
            }
            peer.append_entry(local_node, entry).map_err(|retry_err| {
                SharedLogError::Backend(format!(
                    "metadata peer append failed after catch-up: {retry_err}; initial error: {first_err}"
                ))
            })
        }
    }
}

fn catch_up_peer_from_log(
    local_node: NodeId,
    local: &FileSharedLog,
    peer: &dyn MetadataLogPeerAppender,
    before: LogIndex,
) -> Result<(), SharedLogError> {
    let entries = local.read_from(LogIndex::new(1)?, 0)?;
    for entry in entries {
        if entry.position.index >= before {
            break;
        }
        peer.append_entry(local_node, &entry)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_cluster::{FileSharedLogOptions, FileSharedLogSync};
    use nokvfs_meta::command::{
        CommandKind, Mutation, MutationOp, PredicateRef, Value, Version, WatchProjection,
    };
    use nokvfs_types::RecordFamily;
    use std::sync::{mpsc, Condvar, Mutex};
    use std::time::Duration;
    use tempfile::{tempdir, TempDir};

    struct RecordingPeer {
        result: Result<(), SharedLogError>,
        entries: Mutex<Vec<MetadataLogEntry>>,
    }

    struct CoordinatedPeer {
        state: Arc<(Mutex<usize>, Condvar)>,
        entries: Mutex<Vec<MetadataLogEntry>>,
    }

    #[derive(Default)]
    struct BlockingPeerState {
        started: usize,
        release: bool,
    }

    struct BlockingPeer {
        state: Arc<(Mutex<BlockingPeerState>, Condvar)>,
    }

    struct FilePeer {
        log: Arc<FileSharedLog>,
    }

    struct CompactingBootstrapper {
        log: Arc<FileSharedLog>,
        calls: Mutex<Vec<(NodeId, LogIndex)>>,
    }

    impl RecordingPeer {
        fn ok() -> Self {
            Self {
                result: Ok(()),
                entries: Mutex::new(Vec::new()),
            }
        }

        fn failing() -> Self {
            Self {
                result: Err(SharedLogError::Backend("peer unavailable".to_owned())),
                entries: Mutex::new(Vec::new()),
            }
        }
    }

    impl CoordinatedPeer {
        fn new(state: Arc<(Mutex<usize>, Condvar)>) -> Self {
            Self {
                state,
                entries: Mutex::new(Vec::new()),
            }
        }
    }

    impl BlockingPeer {
        fn new(state: Arc<(Mutex<BlockingPeerState>, Condvar)>) -> Self {
            Self { state }
        }
    }

    impl MetadataLogPeerAppender for RecordingPeer {
        fn append_entry(
            &self,
            _leader: NodeId,
            entry: &MetadataLogEntry,
        ) -> Result<(), SharedLogError> {
            self.entries.lock().unwrap().push(entry.clone());
            self.result.clone()
        }
    }

    impl MetadataLogPeerAppender for CoordinatedPeer {
        fn append_entry(
            &self,
            _leader: NodeId,
            entry: &MetadataLogEntry,
        ) -> Result<(), SharedLogError> {
            let (lock, cvar) = &*self.state;
            let mut active = lock.lock().unwrap();
            *active += 1;
            cvar.notify_all();
            let (active, timeout) = cvar
                .wait_timeout_while(active, Duration::from_secs(1), |active| *active < 2)
                .unwrap();
            if timeout.timed_out() && *active < 2 {
                return Err(SharedLogError::Backend(
                    "remote voter append was not concurrent".to_owned(),
                ));
            }
            self.entries.lock().unwrap().push(entry.clone());
            Ok(())
        }
    }

    impl MetadataLogPeerAppender for BlockingPeer {
        fn append_entry(
            &self,
            _leader: NodeId,
            _entry: &MetadataLogEntry,
        ) -> Result<(), SharedLogError> {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap();
            state.started = state.started.saturating_add(1);
            cvar.notify_all();
            while !state.release {
                state = cvar.wait(state).unwrap();
            }
            Ok(())
        }
    }

    impl MetadataLogPeerAppender for FilePeer {
        fn append_entry(
            &self,
            _leader: NodeId,
            entry: &MetadataLogEntry,
        ) -> Result<(), SharedLogError> {
            self.log.append_entry(entry.clone()).map(|_| ())
        }
    }

    impl MetadataLogPeerBootstrapper for CompactingBootstrapper {
        fn bootstrap_before(&self, peer: NodeId, before: LogIndex) -> Result<(), SharedLogError> {
            self.calls.lock().unwrap().push((peer, before));
            let compacted = before.get().checked_sub(1).ok_or_else(|| {
                SharedLogError::Backend("test bootstrap before index underflow".to_owned())
            })?;
            self.log.compact_through(LogIndex::new(compacted)?)
        }
    }

    fn node(raw: u64) -> NodeId {
        NodeId::new(raw).unwrap()
    }

    fn term(raw: u64) -> LogTerm {
        LogTerm::new(raw).unwrap()
    }

    fn mount() -> MountId {
        MountId::new(1).unwrap()
    }

    fn command(id: &[u8], commit_version: u64) -> MetadataCommand {
        MetadataCommand {
            request_id: id.to_vec(),
            kind: CommandKind::CreateFile,
            read_version: Version::new(commit_version - 1).unwrap(),
            commit_version: Version::new(commit_version).unwrap(),
            primary_family: RecordFamily::Dentry,
            primary_key: id.to_vec(),
            predicates: Vec::<PredicateRef>::new(),
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: id.to_vec(),
                op: MutationOp::Put,
                value: Some(Value(b"value".to_vec())),
            }],
            watch: Vec::<WatchProjection>::new(),
        }
    }

    fn entry(index: u64, command: MetadataCommand) -> MetadataLogEntry {
        MetadataLogEntry {
            position: LogPosition {
                term: term(1),
                index: LogIndex::new(index).unwrap(),
            },
            mount: mount(),
            commands: vec![command],
        }
    }

    fn file_log() -> (TempDir, Arc<FileSharedLog>) {
        let dir = tempdir().unwrap();
        let log = FileSharedLog::open(
            dir.path().join("metadata.log"),
            FileSharedLogOptions {
                sync: FileSharedLogSync::None,
            },
        )
        .unwrap();
        (dir, Arc::new(log))
    }

    fn wait_until(message: &str, condition: impl Fn() -> bool) {
        for _ in 0..100 {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(condition(), "{message}");
    }

    fn release_blocking_peer(state: &Arc<(Mutex<BlockingPeerState>, Condvar)>) {
        let (lock, cvar) = &**state;
        let mut state = lock.lock().unwrap();
        state.release = true;
        cvar.notify_all();
    }

    fn membership(leader: NodeId) -> MetadataMembership {
        MetadataMembership::new(mount(), term(1), leader, [node(1), node(2), node(3)], []).unwrap()
    }

    fn membership_with_learner(leader: NodeId) -> MetadataMembership {
        MetadataMembership::new(
            mount(),
            term(1),
            leader,
            [node(1), node(2), node(3)],
            [node(4)],
        )
        .unwrap()
    }

    #[test]
    fn majority_append_writes_remote_before_local() {
        let (_dir, local) = file_log();
        let peer2 = Arc::new(RecordingPeer::ok());
        let peer3 = Arc::new(RecordingPeer::ok());
        let mut peers = BTreeMap::new();
        peers.insert(node(2), peer2.clone() as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), peer3.clone() as Arc<dyn MetadataLogPeerAppender>);
        let log = MajorityMetadataLog::with_peers(node(1), membership(node(1)), local, peers, None);

        let receipts = log
            .append_batch(term(1), mount(), &[command(b"a", 2)])
            .unwrap();

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].position.index.get(), 1);
        assert_eq!(log.committed_position().unwrap().index.get(), 1);
        for peer in [&peer2, &peer3] {
            wait_until("remote voter should eventually receive entry", || {
                peer.entries.lock().unwrap().len() == 1
            });
            let entries = peer.entries.lock().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].position.index.get(), 1);
            assert_eq!(entries[0].commands[0].request_id, b"a");
        }
        wait_until("remote voter stats should include both voters", || {
            let stats = log.replication_stats();
            stats.remote_voter_append_success_total == 2
                && stats.remote_voter_quorum_success_total == 1
        });
        let stats = log.replication_stats();
        assert_eq!(stats.remote_voter_append_total, 2);
        assert_eq!(stats.remote_voter_append_failure_total, 0);
        assert!(stats.remote_voter_quorum_wait_ns_total > 0);
    }

    #[test]
    fn majority_append_sends_remote_voter_appends_concurrently() {
        let (_dir, local) = file_log();
        let state = Arc::new((Mutex::new(0_usize), Condvar::new()));
        let peer2 = Arc::new(CoordinatedPeer::new(Arc::clone(&state)));
        let peer3 = Arc::new(CoordinatedPeer::new(Arc::clone(&state)));
        let mut peers = BTreeMap::new();
        peers.insert(node(2), peer2.clone() as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), peer3.clone() as Arc<dyn MetadataLogPeerAppender>);
        let log = MajorityMetadataLog::with_peers(node(1), membership(node(1)), local, peers, None);

        let receipts = log
            .append_batch(term(1), mount(), &[command(b"a", 2)])
            .unwrap();

        assert_eq!(receipts[0].position.index.get(), 1);
        wait_until("remote voter 2 should receive concurrent append", || {
            peer2.entries.lock().unwrap().len() == 1
        });
        wait_until("remote voter 3 should receive concurrent append", || {
            peer3.entries.lock().unwrap().len() == 1
        });
        assert_eq!(peer2.entries.lock().unwrap().len(), 1);
        assert_eq!(peer3.entries.lock().unwrap().len(), 1);
    }

    #[test]
    fn majority_append_returns_after_quorum_without_waiting_for_slow_voter() {
        let (_dir, local) = file_log();
        let slow_state = Arc::new((Mutex::new(BlockingPeerState::default()), Condvar::new()));
        let peer2 = Arc::new(BlockingPeer::new(Arc::clone(&slow_state)));
        let peer3 = Arc::new(RecordingPeer::ok());
        let mut peers = BTreeMap::new();
        peers.insert(node(2), peer2 as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), peer3 as Arc<dyn MetadataLogPeerAppender>);
        let log = Arc::new(MajorityMetadataLog::with_peers(
            node(1),
            membership(node(1)),
            local,
            peers,
            None,
        ));
        let (done_tx, done_rx) = mpsc::channel();
        let writer_log = Arc::clone(&log);
        let writer = thread::spawn(move || {
            let result = writer_log.append_batch(term(1), mount(), &[command(b"a", 2)]);
            done_tx
                .send(result.map(|receipts| receipts[0].position.index))
                .unwrap();
        });

        wait_until("slow voter append should start", || {
            let (lock, _) = &*slow_state;
            lock.lock().unwrap().started > 0
        });
        assert_eq!(
            done_rx
                .recv_timeout(Duration::from_millis(100))
                .unwrap()
                .unwrap(),
            LogIndex::new(1).unwrap()
        );
        release_blocking_peer(&slow_state);
        writer.join().unwrap();
    }

    #[test]
    fn failed_quorum_does_not_append_local_entry() {
        let (_dir, local) = file_log();
        let peer = Arc::new(RecordingPeer::failing());
        let mut peers = BTreeMap::new();
        peers.insert(node(2), peer as Arc<dyn MetadataLogPeerAppender>);
        let log = MajorityMetadataLog::with_peers(node(1), membership(node(1)), local, peers, None);

        let err = log
            .append_batch(term(1), mount(), &[command(b"a", 2)])
            .unwrap_err();

        assert_eq!(
            err,
            SharedLogError::NoQuorum {
                required: 2,
                available: 1
            }
        );
        assert_eq!(log.committed_position(), None);
        assert!(log
            .read_from(LogIndex::new(1).unwrap(), 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn lagging_peer_catches_up_before_current_append() {
        let (_leader_dir, local) = file_log();
        local
            .append_entry(entry(1, command(b"first", 2)))
            .expect("leader should hold prior entry");
        let (_peer_dir, peer_log) = file_log();
        let peer = Arc::new(FilePeer {
            log: Arc::clone(&peer_log),
        });
        let peer3 = Arc::new(RecordingPeer::ok());
        let mut peers = BTreeMap::new();
        peers.insert(node(2), peer as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), peer3 as Arc<dyn MetadataLogPeerAppender>);
        let log = MajorityMetadataLog::with_peers(node(1), membership(node(1)), local, peers, None);

        let receipts = log
            .append_batch(term(1), mount(), &[command(b"second", 3)])
            .unwrap();

        assert_eq!(receipts[0].position.index.get(), 2);
        wait_until("lagging voter should catch up in background", || {
            peer_log
                .read_from(LogIndex::new(1).unwrap(), 0)
                .unwrap()
                .len()
                == 2
        });
        let entries = peer_log.read_from(LogIndex::new(1).unwrap(), 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].commands[0].request_id, b"first");
        assert_eq!(entries[1].commands[0].request_id, b"second");
    }

    #[test]
    fn compacted_leader_bootstraps_peer_before_current_append() {
        let (_leader_dir, local) = file_log();
        local
            .append_entry(entry(1, command(b"first", 2)))
            .expect("leader should hold first entry");
        local
            .append_entry(entry(2, command(b"second", 3)))
            .expect("leader should hold second entry");
        local
            .compact_through(LogIndex::new(2).unwrap())
            .expect("leader should compact checkpointed prefix");
        let (_peer_dir, peer_log) = file_log();
        let peer = Arc::new(FilePeer {
            log: Arc::clone(&peer_log),
        });
        let peer3 = Arc::new(RecordingPeer::ok());
        let bootstrapper = Arc::new(CompactingBootstrapper {
            log: Arc::clone(&peer_log),
            calls: Mutex::new(Vec::new()),
        });
        let mut peers = BTreeMap::new();
        peers.insert(node(2), peer as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), peer3 as Arc<dyn MetadataLogPeerAppender>);
        let log = MajorityMetadataLog::with_peers(
            node(1),
            membership(node(1)),
            local,
            peers,
            Some(bootstrapper.clone() as Arc<dyn MetadataLogPeerBootstrapper>),
        );

        let receipts = log
            .append_batch(term(1), mount(), &[command(b"third", 4)])
            .unwrap();

        assert_eq!(receipts[0].position.index.get(), 3);
        wait_until("compacted voter should bootstrap in background", || {
            !bootstrapper.calls.lock().unwrap().is_empty()
        });
        assert_eq!(
            bootstrapper.calls.lock().unwrap().as_slice(),
            &[(node(2), LogIndex::new(3).unwrap())]
        );
        assert!(matches!(
            peer_log.read_from(LogIndex::new(1).unwrap(), 0),
            Err(SharedLogError::Compacted { .. })
        ));
        let entries = peer_log.read_from(LogIndex::new(3).unwrap(), 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].commands[0].request_id, b"third");
    }

    #[test]
    fn learner_receives_entry_after_quorum_commit() {
        let (_dir, local) = file_log();
        let voter2 = Arc::new(RecordingPeer::ok());
        let voter3 = Arc::new(RecordingPeer::ok());
        let learner4 = Arc::new(RecordingPeer::ok());
        let mut peers = BTreeMap::new();
        peers.insert(node(2), voter2.clone() as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), voter3.clone() as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(
            node(4),
            learner4.clone() as Arc<dyn MetadataLogPeerAppender>,
        );
        let log = MajorityMetadataLog::with_peers(
            node(1),
            membership_with_learner(node(1)),
            local,
            peers,
            None,
        );

        let receipts = log
            .append_batch(term(1), mount(), &[command(b"a", 2)])
            .unwrap();

        assert_eq!(receipts[0].position.index.get(), 1);
        wait_until("learner should receive committed entry", || {
            learner4.entries.lock().unwrap().len() == 1
        });
        let learner_entries = learner4.entries.lock().unwrap();
        assert_eq!(learner_entries.len(), 1);
        assert_eq!(learner_entries[0].commands[0].request_id, b"a");
    }

    #[test]
    fn learner_append_does_not_block_quorum_commit() {
        let (_dir, local) = file_log();
        let voter2 = Arc::new(RecordingPeer::ok());
        let voter3 = Arc::new(RecordingPeer::ok());
        let learner_state = Arc::new((Mutex::new(BlockingPeerState::default()), Condvar::new()));
        let learner4 = Arc::new(BlockingPeer::new(Arc::clone(&learner_state)));
        let mut peers = BTreeMap::new();
        peers.insert(node(2), voter2 as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), voter3 as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(4), learner4 as Arc<dyn MetadataLogPeerAppender>);
        let log = Arc::new(MajorityMetadataLog::with_peers(
            node(1),
            membership_with_learner(node(1)),
            local,
            peers,
            None,
        ));
        let (done_tx, done_rx) = mpsc::channel();
        let writer_log = Arc::clone(&log);
        let writer = thread::spawn(move || {
            let result = writer_log.append_batch(term(1), mount(), &[command(b"a", 2)]);
            done_tx
                .send(result.map(|receipts| receipts[0].position.index))
                .unwrap();
        });

        wait_until("blocking learner append should start", || {
            let (lock, _) = &*learner_state;
            lock.lock().unwrap().started > 0
        });
        assert_eq!(
            done_rx
                .recv_timeout(Duration::from_millis(100))
                .unwrap()
                .unwrap(),
            LogIndex::new(1).unwrap()
        );
        release_blocking_peer(&learner_state);
        writer.join().unwrap();
        wait_until("learner catch-up stats should advance", || {
            let stats = log.replication_stats();
            stats.learner_wakeup_total == 1 && stats.learner_catchup_success_total == 1
        });
        let stats = log.replication_stats();
        assert_eq!(stats.learner_catchup_failure_total, 0);
        assert!(stats.learner_catchup_ns_total > 0);
    }

    #[test]
    fn learner_failure_does_not_break_quorum_commit() {
        let (_dir, local) = file_log();
        let voter2 = Arc::new(RecordingPeer::ok());
        let voter3 = Arc::new(RecordingPeer::ok());
        let learner4 = Arc::new(RecordingPeer::failing());
        let mut peers = BTreeMap::new();
        peers.insert(node(2), voter2 as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), voter3 as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(
            node(4),
            learner4.clone() as Arc<dyn MetadataLogPeerAppender>,
        );
        let log = MajorityMetadataLog::with_peers(
            node(1),
            membership_with_learner(node(1)),
            local,
            peers,
            None,
        );

        let receipts = log
            .append_batch(term(1), mount(), &[command(b"a", 2)])
            .unwrap();

        assert_eq!(receipts[0].position.index.get(), 1);
        assert_eq!(log.committed_position().unwrap().index.get(), 1);
        wait_until("learner failure should be attempted in background", || {
            learner4.entries.lock().unwrap().len() >= 2
        });
        let attempts = learner4.entries.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].commands[0].request_id, b"a");
        assert_eq!(attempts[1].commands[0].request_id, b"a");
    }

    #[test]
    fn compacted_leader_bootstraps_learner_after_quorum_commit() {
        let (_leader_dir, local) = file_log();
        local
            .append_entry(entry(1, command(b"first", 2)))
            .expect("leader should hold first entry");
        local
            .append_entry(entry(2, command(b"second", 3)))
            .expect("leader should hold second entry");
        local
            .compact_through(LogIndex::new(2).unwrap())
            .expect("leader should compact checkpointed prefix");
        let (_learner_dir, learner_log) = file_log();
        let learner = Arc::new(FilePeer {
            log: Arc::clone(&learner_log),
        });
        let voter2 = Arc::new(RecordingPeer::ok());
        let voter3 = Arc::new(RecordingPeer::ok());
        let bootstrapper = Arc::new(CompactingBootstrapper {
            log: Arc::clone(&learner_log),
            calls: Mutex::new(Vec::new()),
        });
        let mut peers = BTreeMap::new();
        peers.insert(node(2), voter2 as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(3), voter3 as Arc<dyn MetadataLogPeerAppender>);
        peers.insert(node(4), learner as Arc<dyn MetadataLogPeerAppender>);
        let log = MajorityMetadataLog::with_peers(
            node(1),
            membership_with_learner(node(1)),
            local,
            peers,
            Some(bootstrapper.clone() as Arc<dyn MetadataLogPeerBootstrapper>),
        );

        let receipts = log
            .append_batch(term(1), mount(), &[command(b"third", 4)])
            .unwrap();

        assert_eq!(receipts[0].position.index.get(), 3);
        wait_until("learner bootstrap should run in background", || {
            !bootstrapper.calls.lock().unwrap().is_empty()
        });
        assert_eq!(
            bootstrapper.calls.lock().unwrap().as_slice(),
            &[(node(4), LogIndex::new(3).unwrap())]
        );
        assert!(matches!(
            learner_log.read_from(LogIndex::new(1).unwrap(), 0),
            Err(SharedLogError::Compacted { .. })
        ));
        let entries = learner_log.read_from(LogIndex::new(3).unwrap(), 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].commands[0].request_id, b"third");
    }

    #[test]
    fn non_leader_cannot_append() {
        let (_dir, local) = file_log();
        let log = MajorityMetadataLog::with_peers(
            node(2),
            membership(node(1)),
            local,
            BTreeMap::new(),
            None,
        );

        let err = log
            .append_batch(term(1), mount(), &[command(b"a", 2)])
            .unwrap_err();

        assert_eq!(
            err,
            SharedLogError::UnauthorizedLeader {
                expected: node(1),
                proposed: node(2)
            }
        );
        assert_eq!(log.committed_position(), None);
    }
}
