use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use nokv_proto::nokv::metadata::v1 as metadatapb;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metadata store mutex poisoned")]
    Poisoned,
    #[error("metadata backend error: {0}")]
    Backend(String),
    #[error("metadata decode error: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Put = 0,
    Delete = 1,
}

impl ValueKind {
    pub fn from_i32(raw: i32) -> Self {
        match raw {
            1 => Self::Delete,
            _ => Self::Put,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub kind: ValueKind,
    pub start_version: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
    pub retention_pin_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub writes: Vec<MetadataSnapshotWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshotWrite {
    pub family: i32,
    pub key: Vec<u8>,
    pub commit_version: u64,
    pub value: VersionedValue,
}

#[derive(Debug, Default)]
pub(crate) struct Inner {
    pub(crate) writes: BTreeMap<Vec<u8>, BTreeMap<u64, VersionedValue>>,
}

/// In-memory metadata-store implementation used by tests and memory-backed
/// raftstore regions.
#[derive(Debug, Clone, Default)]
pub struct MemoryMetadataStore {
    pub(crate) inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataApplyResult {
    pub commit_version: u64,
    pub applied_mutations: u64,
    pub error: Option<metadatapb::MetadataKeyError>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataRetentionResult {
    pub retention_floor: u64,
    pub pruned_versions: u64,
    pub pruned_watch_events: u64,
    pub retained_anchor_versions: u64,
}

pub trait MetadataEngine: Clone + Send + Sync + 'static {
    fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> Result<metadatapb::MetadataGetResponse>;

    fn batch_get_metadata(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> Result<metadatapb::MetadataBatchGetResponse>;

    fn scan_metadata(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> Result<metadatapb::MetadataScanResponse>;

    fn commit_metadata(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> Result<MetadataApplyResult>;

    fn commit_metadata_batch(
        &self,
        commands: &[metadatapb::MetadataCommand],
        commit_versions: &[u64],
    ) -> Result<Vec<MetadataApplyResult>> {
        if commands.len() != commit_versions.len() {
            return Err(Error::Backend(
                "metadata command batch length mismatch".to_owned(),
            ));
        }
        commands
            .iter()
            .zip(commit_versions)
            .map(|(command, commit_version)| self.commit_metadata(command, *commit_version))
            .collect()
    }
}

pub trait MetadataSnapshotEngine: Clone + Send + Sync + 'static {
    fn export_metadata_snapshot(&self) -> Result<MetadataSnapshot>;
    fn install_metadata_snapshot(&self, snapshot: MetadataSnapshot) -> Result<()>;
}

pub trait MetadataRetentionEngine: Clone + Send + Sync + 'static {
    fn prune_metadata_versions(&self, retention_floor: u64) -> Result<MetadataRetentionResult>;
}
