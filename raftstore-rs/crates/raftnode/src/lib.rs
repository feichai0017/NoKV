//! OpenRaft boundary for Rust raftstore.
//!
//! The crate intentionally exposes a small NoKV-owned trait instead of leaking
//! OpenRaft types into server, MVCC, or proto-facing code. The concrete
//! OpenRaft-backed implementation will fill this boundary as region replication
//! is brought up.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nokv_mvcc::{KvEngine, MvccStore};
use nokv_proto::nokv::kv::v1 as kvpb;

pub type NodeId = u64;
pub type RegionId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub region_id: RegionId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProposal {
    pub region_id: RegionId,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("raft replication is not wired yet")]
    NotReady,
}

pub trait RegionRaft: Send + Sync {
    fn propose(&self, proposal: Proposal) -> Result<AppliedProposal, Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyStatus {
    pub region_id: RegionId,
    pub term: u64,
    pub applied_index: u64,
}

pub trait ApplyStatusProvider: Clone + Send + Sync + 'static {
    fn apply_status(&self) -> ApplyStatus;
}

#[derive(Debug)]
struct AppliedKvInner<E> {
    region_id: RegionId,
    term: u64,
    applied_index: AtomicU64,
    engine: Mutex<E>,
}

/// Region-local apply boundary used before the OpenRaft-backed implementation
/// is complete. Reads go through the current state-machine view; writes advance
/// a monotonically increasing applied index under the region apply mutex.
#[derive(Debug, Clone)]
pub struct AppliedKvEngine<E = MvccStore> {
    inner: Arc<AppliedKvInner<E>>,
}

impl<E> AppliedKvEngine<E> {
    pub fn new(region_id: RegionId, engine: E) -> Self {
        Self {
            inner: Arc::new(AppliedKvInner {
                region_id,
                term: 1,
                applied_index: AtomicU64::new(0),
                engine: Mutex::new(engine),
            }),
        }
    }

    pub fn status(&self) -> ApplyStatus {
        ApplyStatus {
            region_id: self.inner.region_id,
            term: self.inner.term,
            applied_index: self.inner.applied_index.load(Ordering::Acquire),
        }
    }
}

impl<E> ApplyStatusProvider for AppliedKvEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn apply_status(&self) -> ApplyStatus {
        self.status()
    }
}

impl<E> AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn read<T>(&self, f: impl FnOnce(&E) -> nokv_mvcc::Result<T>) -> nokv_mvcc::Result<T> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        f(&engine)
    }

    fn apply<T>(&self, f: impl FnOnce(&E) -> nokv_mvcc::Result<T>) -> nokv_mvcc::Result<T> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        let result = f(&engine)?;
        self.inner.applied_index.fetch_add(1, Ordering::AcqRel);
        Ok(result)
    }
}

impl<E> KvEngine for AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn get(&self, req: &kvpb::GetRequest) -> nokv_mvcc::Result<kvpb::GetResponse> {
        self.read(|engine| engine.get(req))
    }

    fn batch_get(&self, req: &kvpb::BatchGetRequest) -> nokv_mvcc::Result<kvpb::BatchGetResponse> {
        self.read(|engine| engine.batch_get(req))
    }

    fn scan(&self, req: &kvpb::ScanRequest) -> nokv_mvcc::Result<kvpb::ScanResponse> {
        self.read(|engine| engine.scan(req))
    }

    fn prewrite(&self, req: &kvpb::PrewriteRequest) -> nokv_mvcc::Result<kvpb::PrewriteResponse> {
        self.apply(|engine| engine.prewrite(req))
    }

    fn commit(&self, req: &kvpb::CommitRequest) -> nokv_mvcc::Result<kvpb::CommitResponse> {
        self.apply(|engine| engine.commit(req))
    }

    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> nokv_mvcc::Result<kvpb::BatchRollbackResponse> {
        self.apply(|engine| engine.batch_rollback(req))
    }

    fn resolve_lock(
        &self,
        req: &kvpb::ResolveLockRequest,
    ) -> nokv_mvcc::Result<kvpb::ResolveLockResponse> {
        self.apply(|engine| engine.resolve_lock(req))
    }

    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> nokv_mvcc::Result<kvpb::CheckTxnStatusResponse> {
        self.apply(|engine| engine.check_txn_status(req))
    }

    fn txn_heartbeat(
        &self,
        req: &kvpb::TxnHeartBeatRequest,
    ) -> nokv_mvcc::Result<kvpb::TxnHeartBeatResponse> {
        self.apply(|engine| engine.txn_heartbeat(req))
    }

    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> nokv_mvcc::Result<kvpb::TryAtomicMutateResponse> {
        self.apply(|engine| engine.try_atomic_mutate(req))
    }

    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> nokv_mvcc::Result<kvpb::InstallPreparedMvccEntriesResponse> {
        self.apply(|engine| engine.install_prepared(req))
    }

    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> nokv_mvcc::Result<kvpb::MvccMaintenanceResponse> {
        self.apply(|engine| engine.mvcc_maintenance(req))
    }
}

/// Marker for the future OpenRaft-backed implementation. Keeping this type in
/// place makes the dependency explicit while the first slice runs single-node
/// MVCC behind the existing wire contract.
#[derive(Debug, Default)]
pub struct OpenRaftRegion {
    _openraft: PhantomData<openraft::RaftMetrics<u64, ()>>,
}

impl OpenRaftRegion {
    pub fn new() -> Self {
        Self {
            _openraft: PhantomData,
        }
    }
}

impl RegionRaft for OpenRaftRegion {
    fn propose(&self, _proposal: Proposal) -> Result<AppliedProposal, Error> {
        Err(Error::NotReady)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openraft_boundary_does_not_claim_replication_yet() {
        let raft = OpenRaftRegion::new();
        let err = raft
            .propose(Proposal {
                region_id: 1,
                payload: b"cmd".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(err, Error::NotReady));
    }

    #[test]
    fn applied_kv_engine_advances_index_only_for_writes() {
        let engine = AppliedKvEngine::new(7, MvccStore::new());
        assert_eq!(engine.status().applied_index, 0);

        let get = engine
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 1,
            })
            .unwrap();
        assert!(get.not_found);
        assert_eq!(engine.status().applied_index, 0);

        engine
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                commit_version: 2,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(engine.status().applied_index, 1);
    }
}
