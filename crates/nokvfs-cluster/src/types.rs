use nokvfs_meta::command::{MetadataCommand, Version};
use nokvfs_types::MountId;

use crate::SharedLogError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogTerm(u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(u64);

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
pub struct CheckpointFrontier {
    pub durable_position: LogPosition,
    pub applied_position: LogPosition,
    pub min_retained_index: LogIndex,
    pub max_commit_version: Version,
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
