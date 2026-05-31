use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use nokv_proto::nokv::kv::v1 as kvpb;

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

/// In-memory MVCC implementation used by the first Rust raftstore service
/// slice and by tests. The Holt-backed implementation will reuse the same
/// request semantics and move persistence below this boundary.
#[derive(Debug, Clone, Default)]
pub struct MvccStore {
    pub(crate) inner: Arc<Mutex<Inner>>,
}

pub trait KvEngine: Clone + Send + Sync + 'static {
    fn get(&self, req: &kvpb::GetRequest) -> Result<kvpb::GetResponse>;
    fn batch_get(&self, req: &kvpb::BatchGetRequest) -> Result<kvpb::BatchGetResponse>;
    fn scan(&self, req: &kvpb::ScanRequest) -> Result<kvpb::ScanResponse>;
    fn prewrite(&self, req: &kvpb::PrewriteRequest) -> Result<kvpb::PrewriteResponse>;
    fn commit(&self, req: &kvpb::CommitRequest) -> Result<kvpb::CommitResponse>;
    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> Result<kvpb::BatchRollbackResponse>;
    fn resolve_lock(&self, req: &kvpb::ResolveLockRequest) -> Result<kvpb::ResolveLockResponse>;
    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> Result<kvpb::CheckTxnStatusResponse>;
    fn txn_heartbeat(&self, req: &kvpb::TxnHeartBeatRequest) -> Result<kvpb::TxnHeartBeatResponse>;
    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> Result<kvpb::TryAtomicMutateResponse>;
    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> Result<kvpb::InstallPreparedMvccEntriesResponse>;
    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> Result<kvpb::MvccMaintenanceResponse>;
}

pub trait MvccSnapshotEngine: Clone + Send + Sync + 'static {
    fn export_mvcc_snapshot(&self) -> Result<MvccSnapshot>;
    fn install_mvcc_snapshot(&self, snapshot: MvccSnapshot) -> Result<()>;
}
