use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("mvcc store mutex poisoned")]
    Poisoned,
    #[error("mvcc backend error: {0}")]
    Backend(String),
    #[error("mvcc decode error: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub kind: kvpb::mutation::Op,
    pub start_version: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockRecord {
    pub primary: Vec<u8>,
    pub start_version: u64,
    pub start_time: u64,
    pub ttl: u64,
    pub min_commit_ts: u64,
    pub op: kvpb::mutation::Op,
    pub value: Vec<u8>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshot {
    pub writes: Vec<MvccSnapshotWrite>,
    pub locks: Vec<MvccSnapshotLock>,
    pub rollbacks: Vec<MvccSnapshotRollback>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshotWrite {
    pub key: Vec<u8>,
    pub commit_version: u64,
    pub value: VersionedValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshotLock {
    pub key: Vec<u8>,
    pub lock: LockRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshotRollback {
    pub key: Vec<u8>,
    pub start_version: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Inner {
    pub(crate) writes: BTreeMap<Vec<u8>, BTreeMap<u64, VersionedValue>>,
    pub(crate) locks: BTreeMap<Vec<u8>, LockRecord>,
    pub(crate) rollbacks: BTreeSet<(Vec<u8>, u64)>,
}

/// In-memory metadata MVCC implementation used by tests and memory-backed
/// raftstore regions.
#[derive(Debug, Clone, Default)]
pub struct MvccStore {
    pub(crate) inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataApplyResult {
    pub commit_version: u64,
    pub applied_mutations: u64,
    pub error: Option<kvpb::KeyError>,
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
}

pub trait MvccSnapshotEngine: Clone + Send + Sync + 'static {
    fn export_mvcc_snapshot(&self) -> Result<MvccSnapshot>;
    fn install_mvcc_snapshot(&self, snapshot: MvccSnapshot) -> Result<()>;
}
