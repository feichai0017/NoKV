use std::collections::BTreeSet;

use nokvfs_meta::command::{MetadataCommand, Version};
use nokvfs_types::MountId;

use crate::SharedLogError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogTerm(u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogPosition {
    pub term: LogTerm,
    pub index: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogEntry {
    pub position: LogPosition,
    pub mount: MountId,
    pub commands: Vec<MetadataCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableReceipt {
    pub position: LogPosition,
    pub mount: MountId,
    pub batch_position: usize,
    pub request_id: Vec<u8>,
    pub commit_version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedMetadataCommand {
    pub receipt: DurableReceipt,
    pub applied_mutations: usize,
    pub watch_events: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApplyFrontier {
    pub position: LogPosition,
    pub commit_version: Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadFreshness {
    AnyApplied,
    AppliedThrough(LogPosition),
    CurrentCommitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointFrontier {
    pub durable_position: LogPosition,
    pub applied_position: LogPosition,
    pub min_retained_index: LogIndex,
    pub max_commit_version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointArtifact {
    pub uri: Vec<u8>,
    pub digest: Vec<u8>,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointManifest {
    pub id: Vec<u8>,
    pub mount: MountId,
    pub frontier: CheckpointFrontier,
    pub artifact: CheckpointArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LearnerBootstrapPlan {
    pub node: NodeId,
    pub checkpoint: CheckpointManifest,
    pub replay_start: LogIndex,
    pub replayed_index: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataMembership {
    pub mount: MountId,
    pub term: LogTerm,
    pub leader: NodeId,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
}

impl LogTerm {
    pub fn new(term: u64) -> Result<Self, SharedLogError> {
        if term == 0 {
            return Err(SharedLogError::ZeroTerm);
        }
        Ok(Self(term))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl LogIndex {
    pub const ZERO: Self = Self(0);

    pub fn new(index: u64) -> Result<Self, SharedLogError> {
        if index == 0 {
            return Err(SharedLogError::ZeroIndex);
        }
        Ok(Self(index))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl NodeId {
    pub fn new(id: u64) -> Result<Self, SharedLogError> {
        if id == 0 {
            return Err(SharedLogError::ZeroNodeId);
        }
        Ok(Self(id))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl CheckpointFrontier {
    pub fn compact_through(self) -> Option<LogIndex> {
        let compacted = self.min_retained_index.get().checked_sub(1)?;
        if compacted == 0 {
            return None;
        }
        LogIndex::new(compacted).ok()
    }
}

impl CheckpointManifest {
    pub fn new(
        id: impl Into<Vec<u8>>,
        mount: MountId,
        frontier: CheckpointFrontier,
        artifact: CheckpointArtifact,
    ) -> Result<Self, SharedLogError> {
        let id = id.into();
        if id.is_empty() {
            return Err(SharedLogError::EmptyCheckpointId);
        }
        if artifact.uri.is_empty() {
            return Err(SharedLogError::EmptyCheckpointArtifactUri);
        }
        Ok(Self {
            id,
            mount,
            frontier,
            artifact,
        })
    }
}

impl CheckpointArtifact {
    pub fn new(
        uri: impl Into<Vec<u8>>,
        digest: impl Into<Vec<u8>>,
        size_bytes: u64,
    ) -> Result<Self, SharedLogError> {
        let uri = uri.into();
        if uri.is_empty() {
            return Err(SharedLogError::EmptyCheckpointArtifactUri);
        }
        Ok(Self {
            uri,
            digest: digest.into(),
            size_bytes,
        })
    }
}

impl MetadataMembership {
    pub fn new(
        mount: MountId,
        term: LogTerm,
        leader: NodeId,
        voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, SharedLogError> {
        let mut seen = BTreeSet::new();
        let mut voter_set = BTreeSet::new();
        for voter in voters {
            if !seen.insert(voter) {
                return Err(SharedLogError::DuplicateNode(voter));
            }
            voter_set.insert(voter);
        }
        if voter_set.is_empty() {
            return Err(SharedLogError::NoVoters);
        }
        if !voter_set.contains(&leader) {
            return Err(SharedLogError::LeaderNotVoter(leader));
        }

        let mut learner_set = BTreeSet::new();
        for learner in learners {
            if !seen.insert(learner) {
                return Err(SharedLogError::DuplicateNode(learner));
            }
            learner_set.insert(learner);
        }

        Ok(Self {
            mount,
            term,
            leader,
            voters: voter_set.into_iter().collect(),
            learners: learner_set.into_iter().collect(),
        })
    }

    pub fn single_voter(
        mount: MountId,
        term: LogTerm,
        node: NodeId,
    ) -> Result<Self, SharedLogError> {
        Self::new(mount, term, node, [node], [])
    }

    pub fn authorize_leader(&self, proposed: NodeId) -> Result<(), SharedLogError> {
        if proposed != self.leader {
            return Err(SharedLogError::UnauthorizedLeader {
                expected: self.leader,
                proposed,
            });
        }
        Ok(())
    }

    pub fn is_voter(&self, node: NodeId) -> bool {
        self.voters.contains(&node)
    }

    pub fn is_learner(&self, node: NodeId) -> bool {
        self.learners.contains(&node)
    }
}
