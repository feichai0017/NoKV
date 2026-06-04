use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

use nokvfs_meta::command::MetadataCommand;
use nokvfs_types::MountId;

use crate::{
    CheckpointCatalog, DurableReceipt, LearnerBootstrapPlan, LogIndex, LogPosition, LogTerm,
    MetadataLogEntry, NodeId, SharedLogError, SharedMetadataLog,
};

#[derive(Debug)]
pub struct InMemoryQuorumLog {
    inner: Mutex<QuorumState>,
}

#[derive(Debug)]
struct QuorumState {
    voters: BTreeMap<NodeId, ReplicaState>,
    learners: BTreeMap<NodeId, ReplicaState>,
    committed_entries: VecDeque<MetadataLogEntry>,
    next_index: u64,
    committed_index: LogIndex,
    compacted_through: LogIndex,
}

#[derive(Debug)]
struct ReplicaState {
    available: bool,
    entries: VecDeque<MetadataLogEntry>,
    compacted_through: LogIndex,
    checkpoint_index: LogIndex,
}

impl InMemoryQuorumLog {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Result<Self, SharedLogError> {
        Self::with_learners(voters, [])
    }

    pub fn with_learners(
        voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, SharedLogError> {
        let mut seen = BTreeSet::new();
        let mut voter_map = BTreeMap::new();
        for voter in voters {
            if !seen.insert(voter) {
                return Err(SharedLogError::DuplicateNode(voter));
            }
            voter_map.insert(voter, ReplicaState::new());
        }
        if voter_map.is_empty() {
            return Err(SharedLogError::NoVoters);
        }

        let mut learner_map = BTreeMap::new();
        for learner in learners {
            if !seen.insert(learner) {
                return Err(SharedLogError::DuplicateNode(learner));
            }
            learner_map.insert(learner, ReplicaState::new());
        }

        Ok(Self {
            inner: Mutex::new(QuorumState {
                voters: voter_map,
                learners: learner_map,
                committed_entries: VecDeque::new(),
                next_index: 1,
                committed_index: LogIndex::ZERO,
                compacted_through: LogIndex::ZERO,
            }),
        })
    }

    pub fn set_node_available(&self, node: NodeId, available: bool) -> Result<(), SharedLogError> {
        let mut inner = self.lock()?;
        let replica = inner
            .replica_mut(node)
            .ok_or(SharedLogError::UnknownNode(node))?;
        replica.available = available;
        Ok(())
    }

    pub fn sync_learner(&self, node: NodeId) -> Result<LogIndex, SharedLogError> {
        let mut inner = self.lock()?;
        if !inner.learners.contains_key(&node) {
            return Err(SharedLogError::UnknownNode(node));
        }
        sync_replica(&mut inner, node)
    }

    pub fn bootstrap_learner_from_checkpoint<C>(
        &self,
        node: NodeId,
        mount: MountId,
        checkpoints: &C,
    ) -> Result<LearnerBootstrapPlan, SharedLogError>
    where
        C: CheckpointCatalog,
    {
        let compacted_through = {
            let inner = self.lock()?;
            if !inner.learners.contains_key(&node) {
                return Err(SharedLogError::UnknownNode(node));
            }
            inner.compacted_through
        };
        let checkpoint =
            checkpoints
                .latest_for_mount(mount)?
                .ok_or(SharedLogError::CheckpointRequired {
                    node,
                    compacted: compacted_through,
                })?;
        let checkpoint_compacted = checkpoint
            .frontier
            .compact_through()
            .unwrap_or(LogIndex::ZERO);
        if checkpoint_compacted < compacted_through {
            return Err(SharedLogError::CheckpointTooOld {
                node,
                checkpoint_compacted,
                required: compacted_through,
            });
        }
        let replay_start = checkpoint.frontier.min_retained_index;
        let replayed_index = {
            let mut inner = self.lock()?;
            if !inner.learners.contains_key(&node) {
                return Err(SharedLogError::UnknownNode(node));
            }
            let entries = inner
                .committed_entries
                .iter()
                .filter(|entry| entry.position.index >= replay_start)
                .cloned()
                .collect::<Vec<_>>();
            let replica = inner
                .replica_mut(node)
                .ok_or(SharedLogError::UnknownNode(node))?;
            replica.entries.clear();
            replica.compacted_through = checkpoint_compacted;
            replica.checkpoint_index = checkpoint.frontier.applied_position.index;
            replica.entries.extend(entries);
            replica.last_index()
        };
        Ok(LearnerBootstrapPlan {
            node,
            checkpoint,
            replay_start,
            replayed_index,
        })
    }

    pub fn sync_node(&self, node: NodeId) -> Result<LogIndex, SharedLogError> {
        let mut inner = self.lock()?;
        sync_replica(&mut inner, node)
    }

    pub fn read_from_node(
        &self,
        node: NodeId,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError> {
        let inner = self.lock()?;
        let replica = inner
            .replica(node)
            .ok_or(SharedLogError::UnknownNode(node))?;
        if start <= replica.compacted_through {
            return Err(SharedLogError::Compacted {
                requested: start,
                compacted: replica.compacted_through,
            });
        }
        let limit = if limit == 0 { usize::MAX } else { limit };
        Ok(replica
            .entries
            .iter()
            .filter(|entry| entry.position.index >= start)
            .take(limit)
            .cloned()
            .collect())
    }

    pub fn replica_committed_index(&self, node: NodeId) -> Result<LogIndex, SharedLogError> {
        let inner = self.lock()?;
        inner
            .replica(node)
            .map(ReplicaState::last_index)
            .ok_or(SharedLogError::UnknownNode(node))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, QuorumState>, SharedLogError> {
        self.inner
            .lock()
            .map_err(|_| SharedLogError::Backend("quorum log mutex poisoned".to_owned()))
    }
}

fn sync_replica(inner: &mut QuorumState, node: NodeId) -> Result<LogIndex, SharedLogError> {
    let current = inner
        .replica(node)
        .ok_or(SharedLogError::UnknownNode(node))?
        .last_index();
    let start = next_replay_index(current)?;
    if start <= inner.compacted_through {
        return Err(SharedLogError::Compacted {
            requested: start,
            compacted: inner.compacted_through,
        });
    }
    let entries = inner
        .committed_entries
        .iter()
        .filter(|entry| entry.position.index >= start)
        .cloned()
        .collect::<Vec<_>>();
    let replica = inner
        .replica_mut(node)
        .ok_or(SharedLogError::UnknownNode(node))?;
    replica.entries.extend(entries);
    Ok(replica.last_index())
}

impl SharedMetadataLog for InMemoryQuorumLog {
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: &[MetadataCommand],
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        if commands.is_empty() {
            return Err(SharedLogError::EmptyBatch);
        }

        let mut inner = self.lock()?;
        let available_voters = inner
            .voters
            .values()
            .filter(|replica| replica.available)
            .count();
        let quorum = quorum_size(inner.voters.len());
        if available_voters < quorum {
            return Err(SharedLogError::NoQuorum {
                required: quorum,
                available: available_voters,
            });
        }

        let index = LogIndex::new(inner.next_index)?;
        inner.next_index = inner.next_index.saturating_add(1);
        let position = LogPosition { term, index };
        let entry = MetadataLogEntry {
            position,
            mount,
            commands: commands.to_vec(),
        };
        for replica in inner.voters.values_mut() {
            if replica.available {
                replica.entries.push_back(entry.clone());
            }
        }
        for replica in inner.learners.values_mut() {
            if replica.available {
                replica.entries.push_back(entry.clone());
            }
        }
        inner.committed_entries.push_back(entry);
        inner.committed_index = index;

        Ok(commands
            .iter()
            .enumerate()
            .map(|(batch_position, command)| DurableReceipt {
                position,
                mount,
                batch_position,
                request_id: command.request_id.clone(),
                commit_version: command.commit_version,
            })
            .collect())
    }

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError> {
        let inner = self.lock()?;
        if start <= inner.compacted_through {
            return Err(SharedLogError::Compacted {
                requested: start,
                compacted: inner.compacted_through,
            });
        }
        let limit = if limit == 0 { usize::MAX } else { limit };
        Ok(inner
            .committed_entries
            .iter()
            .filter(|entry| entry.position.index >= start)
            .take(limit)
            .cloned()
            .collect())
    }

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError> {
        let mut inner = self.lock()?;
        inner.compacted_through = inner.compacted_through.max(index);
        let compacted_through = inner.compacted_through;
        compact_entries(&mut inner.committed_entries, compacted_through);
        for replica in inner.voters.values_mut() {
            replica.compacted_through = replica.compacted_through.max(index);
            compact_entries(&mut replica.entries, replica.compacted_through);
        }
        for replica in inner.learners.values_mut() {
            replica.compacted_through = replica.compacted_through.max(index);
            compact_entries(&mut replica.entries, replica.compacted_through);
        }
        Ok(())
    }

    fn committed_index(&self) -> LogIndex {
        self.inner
            .lock()
            .map(|inner| inner.committed_index)
            .unwrap_or(LogIndex::ZERO)
    }
}

impl QuorumState {
    fn replica(&self, node: NodeId) -> Option<&ReplicaState> {
        self.voters.get(&node).or_else(|| self.learners.get(&node))
    }

    fn replica_mut(&mut self, node: NodeId) -> Option<&mut ReplicaState> {
        self.voters
            .get_mut(&node)
            .or_else(|| self.learners.get_mut(&node))
    }
}

impl ReplicaState {
    fn new() -> Self {
        Self {
            available: true,
            entries: VecDeque::new(),
            compacted_through: LogIndex::ZERO,
            checkpoint_index: LogIndex::ZERO,
        }
    }

    fn last_index(&self) -> LogIndex {
        self.entries
            .back()
            .map(|entry| entry.position.index)
            .unwrap_or(self.checkpoint_index)
    }
}

fn quorum_size(voters: usize) -> usize {
    voters / 2 + 1
}

fn compact_entries(entries: &mut VecDeque<MetadataLogEntry>, compacted_through: LogIndex) {
    while entries
        .front()
        .is_some_and(|entry| entry.position.index <= compacted_through)
    {
        entries.pop_front();
    }
}

fn next_replay_index(current: LogIndex) -> Result<LogIndex, SharedLogError> {
    LogIndex::new(current.get().saturating_add(1))
}
