use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use nokvfs_cluster::{
    CheckpointCatalog, DurableReceipt, FileCheckpointCatalog, FileSharedLog,
    InstallCheckpointRequest, LearnerBootstrapPlan, LogIndex, LogPosition, LogTerm,
    MetadataLogEntry, MetadataMembership, NodeId, SharedLogError, SharedMetadataLog,
};
use nokvfs_meta::command::MetadataCommand;
use nokvfs_types::MountId;

use crate::options::MetadataLogPeerOptions;
use crate::rpc;

pub(crate) struct MajorityMetadataLog {
    local_node: NodeId,
    membership: MetadataMembership,
    local: Arc<FileSharedLog>,
    peers: BTreeMap<NodeId, Arc<dyn MetadataLogPeerAppender>>,
    bootstrapper: Option<Arc<dyn MetadataLogPeerBootstrapper>>,
    append_gate: Mutex<()>,
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
        Self {
            local_node,
            membership,
            local,
            peers,
            bootstrapper,
            append_gate: Mutex::new(()),
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
                let required_remote = quorum.saturating_sub(1);
                let mut remote_successes = 0_usize;
                for voter in &self.membership.voters {
                    if *voter == self.local_node {
                        continue;
                    }
                    let Some(peer) = self.peers.get(voter) else {
                        continue;
                    };
                    if self
                        .append_peer_with_catchup(*voter, peer.as_ref(), &entry)
                        .is_ok()
                    {
                        remote_successes = remote_successes.saturating_add(1);
                    }
                }
                if remote_successes < required_remote {
                    return Err(SharedLogError::NoQuorum {
                        required: quorum,
                        available: remote_successes.saturating_add(1),
                    });
                }
            }
            let receipts = self.local.append_entry(entry.clone())?;
            (entry, receipts)
        };
        self.replicate_learners_after_commit(&entry);
        Ok(receipts)
    }

    fn replicate_learners_after_commit(&self, entry: &MetadataLogEntry) {
        for learner in &self.membership.learners {
            let Some(peer) = self.peers.get(learner) else {
                continue;
            };
            let _ = self.append_peer_with_catchup(*learner, peer.as_ref(), entry);
        }
    }

    fn append_peer_with_catchup(
        &self,
        peer_node: NodeId,
        peer: &dyn MetadataLogPeerAppender,
        entry: &MetadataLogEntry,
    ) -> Result<(), SharedLogError> {
        match peer.append_entry(self.local_node, entry) {
            Ok(()) => Ok(()),
            Err(first_err) => {
                match self.catch_up_peer(peer, entry.position.index) {
                    Ok(()) => {}
                    Err(SharedLogError::Compacted { .. }) => {
                        let Some(bootstrapper) = self.bootstrapper.as_ref() else {
                            return Err(first_err);
                        };
                        bootstrapper.bootstrap_before(peer_node, entry.position.index)?;
                    }
                    Err(err) => return Err(err),
                }
                peer.append_entry(self.local_node, entry).map_err(|retry_err| {
                    SharedLogError::Backend(format!(
                        "metadata peer append failed after catch-up: {retry_err}; initial error: {first_err}"
                    ))
                })
            }
        }
    }

    fn catch_up_peer(
        &self,
        peer: &dyn MetadataLogPeerAppender,
        before: LogIndex,
    ) -> Result<(), SharedLogError> {
        let entries = self.local.read_from(LogIndex::new(1)?, 0)?;
        for entry in entries {
            if entry.position.index >= before {
                break;
            }
            peer.append_entry(self.local_node, &entry)?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_cluster::{FileSharedLogOptions, FileSharedLogSync};
    use nokvfs_meta::command::{
        CommandKind, Mutation, MutationOp, PredicateRef, Value, Version, WatchProjection,
    };
    use nokvfs_types::RecordFamily;
    use std::sync::Mutex;
    use tempfile::{tempdir, TempDir};

    struct RecordingPeer {
        result: Result<(), SharedLogError>,
        entries: Mutex<Vec<MetadataLogEntry>>,
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
        for peer in [peer2, peer3] {
            let entries = peer.entries.lock().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].position.index.get(), 1);
            assert_eq!(entries[0].commands[0].request_id, b"a");
        }
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
        let learner_entries = learner4.entries.lock().unwrap();
        assert_eq!(learner_entries.len(), 1);
        assert_eq!(learner_entries[0].commands[0].request_id, b"a");
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
