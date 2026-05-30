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
use nokv_proto::nokv::raft::v1 as raftpb;
use tokio::sync::broadcast;

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

pub trait ApplyWatchProvider: Clone + Send + Sync + 'static {
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent>;
}

#[derive(Debug)]
struct AppliedKvInner<E> {
    region_id: RegionId,
    term: u64,
    applied_index: AtomicU64,
    engine: Mutex<E>,
    watch: broadcast::Sender<kvpb::ApplyWatchEvent>,
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
                watch: broadcast::channel(1024).0,
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

    pub fn subscribe(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.inner.watch.subscribe()
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

impl<E> ApplyWatchProvider for AppliedKvEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.subscribe()
    }
}

impl<E> AppliedKvEngine<E>
where
    E: KvEngine,
{
    pub fn execute_raft_command(
        &self,
        req: &raftpb::RaftCmdRequest,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
        let mut responses = Vec::with_capacity(req.requests.len());
        for request in &req.requests {
            responses.push(self.execute_raft_request(request)?);
        }
        Ok(raftpb::RaftCmdResponse {
            header: req.header.clone(),
            responses,
            region_error: None,
        })
    }

    fn execute_raft_request(&self, req: &raftpb::Request) -> nokv_mvcc::Result<raftpb::Response> {
        use raftpb::request::Cmd as RequestCmd;
        use raftpb::response::Cmd as ResponseCmd;

        let cmd = raftpb::CmdType::try_from(req.cmd_type).unwrap_or(raftpb::CmdType::CmdInvalid);
        let response = match (cmd, req.cmd.as_ref()) {
            (raftpb::CmdType::CmdGet, Some(RequestCmd::Get(inner))) => {
                ResponseCmd::Get(self.get(inner)?)
            }
            (raftpb::CmdType::CmdScan, Some(RequestCmd::Scan(inner))) => {
                ResponseCmd::Scan(self.scan(inner)?)
            }
            (raftpb::CmdType::CmdPrewrite, Some(RequestCmd::Prewrite(inner))) => {
                ResponseCmd::Prewrite(self.prewrite(inner)?)
            }
            (raftpb::CmdType::CmdCommit, Some(RequestCmd::Commit(inner))) => {
                ResponseCmd::Commit(self.commit(inner)?)
            }
            (raftpb::CmdType::CmdBatchRollback, Some(RequestCmd::BatchRollback(inner))) => {
                ResponseCmd::BatchRollback(self.batch_rollback(inner)?)
            }
            (raftpb::CmdType::CmdResolveLock, Some(RequestCmd::ResolveLock(inner))) => {
                ResponseCmd::ResolveLock(self.resolve_lock(inner)?)
            }
            (raftpb::CmdType::CmdCheckTxnStatus, Some(RequestCmd::CheckTxnStatus(inner))) => {
                ResponseCmd::CheckTxnStatus(self.check_txn_status(inner)?)
            }
            (raftpb::CmdType::CmdMvccMaintenance, Some(RequestCmd::MvccMaintenance(inner))) => {
                ResponseCmd::MvccMaintenance(self.mvcc_maintenance(inner)?)
            }
            (raftpb::CmdType::CmdTxnHeartBeat, Some(RequestCmd::TxnHeartBeat(inner))) => {
                ResponseCmd::TxnHeartBeat(self.txn_heartbeat(inner)?)
            }
            (raftpb::CmdType::CmdTryAtomicMutate, Some(RequestCmd::TryAtomicMutate(inner))) => {
                ResponseCmd::TryAtomicMutate(self.try_atomic_mutate(inner)?)
            }
            (
                raftpb::CmdType::CmdInstallPreparedMvcc,
                Some(RequestCmd::InstallPreparedMvcc(inner)),
            ) => ResponseCmd::InstallPreparedMvcc(self.install_prepared(inner)?),
            _ => {
                return Err(invalid_raft_command(
                    "command type and payload do not match",
                ))
            }
        };
        Ok(raftpb::Response {
            cmd: Some(response),
        })
    }

    fn read<T>(&self, f: impl FnOnce(&E) -> nokv_mvcc::Result<T>) -> nokv_mvcc::Result<T> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        f(&engine)
    }

    fn apply<T>(&self, f: impl FnOnce(&E) -> nokv_mvcc::Result<T>) -> nokv_mvcc::Result<(u64, T)> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        let result = f(&engine)?;
        let index = self.inner.applied_index.fetch_add(1, Ordering::AcqRel) + 1;
        Ok((index, result))
    }

    fn publish_apply(
        &self,
        index: u64,
        source: kvpb::ApplyWatchEventSource,
        commit_version: u64,
        keys: Vec<Vec<u8>>,
    ) {
        if keys.is_empty() {
            return;
        }
        let _ = self.inner.watch.send(kvpb::ApplyWatchEvent {
            region_id: self.inner.region_id,
            term: self.inner.term,
            index,
            source: source as i32,
            commit_version,
            keys,
        });
    }
}

fn invalid_raft_command(detail: &str) -> nokv_mvcc::Error {
    nokv_mvcc::Error::Backend(format!("invalid raft command: {detail}"))
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
            .map(|(_, result)| result)
    }

    fn commit(&self, req: &kvpb::CommitRequest) -> nokv_mvcc::Result<kvpb::CommitResponse> {
        let (index, result) = self.apply(|engine| engine.commit(req))?;
        self.publish_apply(
            index,
            kvpb::ApplyWatchEventSource::Commit,
            req.commit_version,
            req.keys.clone(),
        );
        Ok(result)
    }

    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> nokv_mvcc::Result<kvpb::BatchRollbackResponse> {
        self.apply(|engine| engine.batch_rollback(req))
            .map(|(_, result)| result)
    }

    fn resolve_lock(
        &self,
        req: &kvpb::ResolveLockRequest,
    ) -> nokv_mvcc::Result<kvpb::ResolveLockResponse> {
        let (index, result) = self.apply(|engine| engine.resolve_lock(req))?;
        if req.commit_version != 0 {
            self.publish_apply(
                index,
                kvpb::ApplyWatchEventSource::ResolveLock,
                req.commit_version,
                req.keys.clone(),
            );
        }
        Ok(result)
    }

    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> nokv_mvcc::Result<kvpb::CheckTxnStatusResponse> {
        self.apply(|engine| engine.check_txn_status(req))
            .map(|(_, result)| result)
    }

    fn txn_heartbeat(
        &self,
        req: &kvpb::TxnHeartBeatRequest,
    ) -> nokv_mvcc::Result<kvpb::TxnHeartBeatResponse> {
        self.apply(|engine| engine.txn_heartbeat(req))
            .map(|(_, result)| result)
    }

    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> nokv_mvcc::Result<kvpb::TryAtomicMutateResponse> {
        let keys = req
            .mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect::<Vec<_>>();
        let (index, result) = self.apply(|engine| engine.try_atomic_mutate(req))?;
        self.publish_apply(
            index,
            kvpb::ApplyWatchEventSource::Commit,
            req.commit_version,
            keys,
        );
        Ok(result)
    }

    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> nokv_mvcc::Result<kvpb::InstallPreparedMvccEntriesResponse> {
        let keys = if req.watch_keys.is_empty() {
            req.entries
                .iter()
                .map(|entry| entry.key.clone())
                .collect::<Vec<_>>()
        } else {
            req.watch_keys.clone()
        };
        let (index, result) = self.apply(|engine| engine.install_prepared(req))?;
        self.publish_apply(
            index,
            kvpb::ApplyWatchEventSource::Commit,
            req.commit_version,
            keys,
        );
        Ok(result)
    }

    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> nokv_mvcc::Result<kvpb::MvccMaintenanceResponse> {
        self.apply(|engine| engine.mvcc_maintenance(req))
            .map(|(_, result)| result)
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

    #[test]
    fn applied_kv_engine_publishes_watch_events_for_writes() {
        let engine = AppliedKvEngine::new(7, MvccStore::new());
        let mut watch = engine.subscribe();
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
        let event = watch.try_recv().unwrap();
        assert_eq!(event.region_id, 7);
        assert_eq!(event.index, 1);
        assert_eq!(event.commit_version, 2);
        assert_eq!(event.keys, vec![b"k".to_vec()]);
    }

    #[test]
    fn applied_kv_engine_executes_existing_raft_command_payload() {
        let engine = AppliedKvEngine::new(7, MvccStore::new());
        let response = engine
            .execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(raftpb::CmdHeader {
                    region_id: 7,
                    ..Default::default()
                }),
                requests: vec![
                    raftpb::Request {
                        cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                        cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                            kvpb::TryAtomicMutateRequest {
                                mutations: vec![kvpb::Mutation {
                                    key: b"k".to_vec(),
                                    value: b"v".to_vec(),
                                    op: kvpb::mutation::Op::Put as i32,
                                    ..Default::default()
                                }],
                                commit_version: 2,
                                ..Default::default()
                            },
                        )),
                    },
                    raftpb::Request {
                        cmd_type: raftpb::CmdType::CmdGet as i32,
                        cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                            key: b"k".to_vec(),
                            version: 2,
                        })),
                    },
                ],
            })
            .unwrap();

        assert_eq!(response.responses.len(), 2);
        let Some(raftpb::response::Cmd::TryAtomicMutate(out)) = response.responses[0].cmd.as_ref()
        else {
            panic!("missing atomic mutate response");
        };
        assert_eq!(out.applied_keys, 1);
        let Some(raftpb::response::Cmd::Get(out)) = response.responses[1].cmd.as_ref() else {
            panic!("missing get response");
        };
        assert_eq!(out.value, b"v".to_vec());
        assert_eq!(engine.status().applied_index, 1);
    }
}
