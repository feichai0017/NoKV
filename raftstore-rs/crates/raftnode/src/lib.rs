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
use prost::Message;
use tokio::sync::broadcast;

mod log_codec;
mod log_store;

pub use log_codec::{decode_log_entry, encode_log_entry};
pub use log_store::{RaftEntryLog, SegmentedEntryLog};

pub type NodeId = u64;
pub type RegionId = u64;
pub type OpenRaftEntry = openraft::Entry<RaftStoreConfig>;

openraft::declare_raft_types!(
    pub RaftStoreConfig:
        D = Proposal,
        R = AppliedProposal,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<RaftStoreConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub region_id: RegionId,
    pub payload: Vec<u8>,
}

impl Proposal {
    pub fn from_raft_command(req: &raftpb::RaftCmdRequest) -> Result<Self, Error> {
        let region_id = req
            .header
            .as_ref()
            .map(|header| header.region_id)
            .ok_or(Error::MissingRegionHeader)?;
        if region_id == 0 {
            return Err(Error::MissingRegionHeader);
        }
        let mut payload = Vec::with_capacity(req.encoded_len());
        req.encode(&mut payload)?;
        Ok(Self { region_id, payload })
    }

    pub fn decode_raft_command(&self) -> Result<raftpb::RaftCmdRequest, Error> {
        let req = raftpb::RaftCmdRequest::decode(self.payload.as_slice())?;
        let region_id = req
            .header
            .as_ref()
            .map(|header| header.region_id)
            .ok_or(Error::MissingRegionHeader)?;
        if region_id != self.region_id {
            return Err(Error::RegionMismatch {
                proposal_region_id: self.region_id,
                command_region_id: region_id,
            });
        }
        Ok(req)
    }
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
    #[error("raft command header region id is required")]
    MissingRegionHeader,
    #[error("raft proposal region {proposal_region_id} does not match command region {command_region_id}")]
    RegionMismatch {
        proposal_region_id: RegionId,
        command_region_id: RegionId,
    },
    #[error("raft log record region {record_region_id} does not match proposal region {proposal_region_id}")]
    LogRegionMismatch {
        record_region_id: RegionId,
        proposal_region_id: RegionId,
    },
    #[error("invalid raft log payload: {0}")]
    InvalidLogPayload(String),
    #[error("raft log error: {0}")]
    RaftLog(#[from] nokv_raftlog::Error),
    #[error("raft command encode error: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("raft command decode error: {0}")]
    Decode(#[from] prost::DecodeError),
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

pub trait RaftCommandExecutor: Clone + Send + Sync + 'static {
    fn execute_raft_command(
        &self,
        req: &raftpb::RaftCmdRequest,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse>;
}

pub trait ApplyStatusSink: Clone + Send + Sync + 'static {
    fn save_apply_status(&self, status: &ApplyStatus) -> nokv_mvcc::Result<()>;
}

#[derive(Debug)]
struct AppliedKvInner<E> {
    region_id: RegionId,
    term: AtomicU64,
    applied_index: AtomicU64,
    engine: Mutex<E>,
    watch: broadcast::Sender<kvpb::ApplyWatchEvent>,
}

#[derive(Debug)]
struct ApplyEvent {
    source: kvpb::ApplyWatchEventSource,
    commit_version: u64,
    keys: Vec<Vec<u8>>,
}

/// Region-local apply boundary used before the OpenRaft-backed implementation
/// is complete. Reads go through the current state-machine view; writes advance
/// a monotonically increasing applied index under the region apply mutex.
#[derive(Debug, Clone)]
pub struct AppliedKvEngine<E = MvccStore> {
    inner: Arc<AppliedKvInner<E>>,
}

#[derive(Debug, Clone)]
pub struct PersistentAppliedKvEngine<E, S> {
    engine: AppliedKvEngine<E>,
    sink: S,
}

impl<E, S> PersistentAppliedKvEngine<E, S> {
    pub fn new(engine: AppliedKvEngine<E>, sink: S) -> Self {
        Self { engine, sink }
    }

    pub fn inner(&self) -> &AppliedKvEngine<E> {
        &self.engine
    }
}

impl<E> AppliedKvEngine<E> {
    pub fn new(region_id: RegionId, engine: E) -> Self {
        Self::with_status(
            ApplyStatus {
                region_id,
                term: 1,
                applied_index: 0,
            },
            engine,
        )
    }

    pub fn with_status(status: ApplyStatus, engine: E) -> Self {
        Self {
            inner: Arc::new(AppliedKvInner {
                region_id: status.region_id,
                term: AtomicU64::new(status.term),
                applied_index: AtomicU64::new(status.applied_index),
                engine: Mutex::new(engine),
                watch: broadcast::channel(1024).0,
            }),
        }
    }

    pub fn status(&self) -> ApplyStatus {
        ApplyStatus {
            region_id: self.inner.region_id,
            term: self.inner.term.load(Ordering::Acquire),
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

impl<E, S> ApplyStatusProvider for PersistentAppliedKvEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: ApplyStatusSink,
{
    fn apply_status(&self) -> ApplyStatus {
        self.engine.status()
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

impl<E, S> ApplyWatchProvider for PersistentAppliedKvEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: ApplyStatusSink,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.engine.subscribe()
    }
}

impl<E> AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn execute_raft_command_inner(
        &self,
        req: &raftpb::RaftCmdRequest,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
        self.execute_raft_command_at(req, None)
    }

    pub fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        let mut applied = Vec::new();
        for entry in entries {
            let index = entry.log_id.index;
            let term = entry.log_id.leader_id.term;
            match entry.payload {
                openraft::EntryPayload::Blank | openraft::EntryPayload::Membership(_) => {
                    self.record_applied_status(term, index);
                    applied.push(AppliedProposal {
                        region_id: self.inner.region_id,
                        index,
                        term,
                        payload: Vec::new(),
                    });
                }
                openraft::EntryPayload::Normal(proposal) => {
                    if proposal.region_id != self.inner.region_id {
                        return Err(nokv_mvcc::Error::Backend(
                            Error::LogRegionMismatch {
                                record_region_id: self.inner.region_id,
                                proposal_region_id: proposal.region_id,
                            }
                            .to_string(),
                        ));
                    }
                    let req = proposal
                        .decode_raft_command()
                        .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
                    let response = self.execute_raft_command_at(&req, Some((term, index)))?;
                    applied.push(AppliedProposal {
                        region_id: proposal.region_id,
                        index,
                        term,
                        payload: encode_raft_response(&response)?,
                    });
                }
            }
        }
        Ok(applied)
    }

    fn execute_raft_command_at(
        &self,
        req: &raftpb::RaftCmdRequest,
        forced_status: Option<(u64, u64)>,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
        let (responses, writes, events) = {
            let engine =
                self.inner.engine.lock().map_err(|_| {
                    nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned())
                })?;
            let mut responses = Vec::with_capacity(req.requests.len());
            let mut writes = false;
            let mut events = Vec::new();
            for request in &req.requests {
                let (response, write, event) = Self::execute_raft_request_on(&*engine, request)?;
                writes |= write;
                if let Some(event) = event {
                    events.push(event);
                }
                responses.push(response);
            }
            (responses, writes, events)
        };

        let applied_status = if let Some((term, index)) = forced_status {
            self.record_applied_status(term, index);
            Some((term, index))
        } else if writes {
            Some(self.advance_apply_index())
        } else {
            None
        };
        if let Some((term, index)) = applied_status {
            for event in events {
                self.publish_apply(index, term, event.source, event.commit_version, event.keys);
            }
        }
        Ok(raftpb::RaftCmdResponse {
            header: req.header.clone(),
            responses,
            region_error: None,
        })
    }

    fn execute_raft_request_on(
        engine: &E,
        req: &raftpb::Request,
    ) -> nokv_mvcc::Result<(raftpb::Response, bool, Option<ApplyEvent>)> {
        use raftpb::request::Cmd as RequestCmd;
        use raftpb::response::Cmd as ResponseCmd;

        let cmd = raftpb::CmdType::try_from(req.cmd_type).unwrap_or(raftpb::CmdType::CmdInvalid);
        let (response, write, event) = match (cmd, req.cmd.as_ref()) {
            (raftpb::CmdType::CmdGet, Some(RequestCmd::Get(inner))) => {
                (ResponseCmd::Get(engine.get(inner)?), false, None)
            }
            (raftpb::CmdType::CmdScan, Some(RequestCmd::Scan(inner))) => {
                (ResponseCmd::Scan(engine.scan(inner)?), false, None)
            }
            (raftpb::CmdType::CmdPrewrite, Some(RequestCmd::Prewrite(inner))) => {
                (ResponseCmd::Prewrite(engine.prewrite(inner)?), true, None)
            }
            (raftpb::CmdType::CmdCommit, Some(RequestCmd::Commit(inner))) => (
                ResponseCmd::Commit(engine.commit(inner)?),
                true,
                Some(ApplyEvent {
                    source: kvpb::ApplyWatchEventSource::Commit,
                    commit_version: inner.commit_version,
                    keys: inner.keys.clone(),
                }),
            ),
            (raftpb::CmdType::CmdBatchRollback, Some(RequestCmd::BatchRollback(inner))) => (
                ResponseCmd::BatchRollback(engine.batch_rollback(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdResolveLock, Some(RequestCmd::ResolveLock(inner))) => {
                let event = (inner.commit_version != 0).then(|| ApplyEvent {
                    source: kvpb::ApplyWatchEventSource::ResolveLock,
                    commit_version: inner.commit_version,
                    keys: inner.keys.clone(),
                });
                (
                    ResponseCmd::ResolveLock(engine.resolve_lock(inner)?),
                    true,
                    event,
                )
            }
            (raftpb::CmdType::CmdCheckTxnStatus, Some(RequestCmd::CheckTxnStatus(inner))) => (
                ResponseCmd::CheckTxnStatus(engine.check_txn_status(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdMvccMaintenance, Some(RequestCmd::MvccMaintenance(inner))) => (
                ResponseCmd::MvccMaintenance(engine.mvcc_maintenance(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdTxnHeartBeat, Some(RequestCmd::TxnHeartBeat(inner))) => (
                ResponseCmd::TxnHeartBeat(engine.txn_heartbeat(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdTryAtomicMutate, Some(RequestCmd::TryAtomicMutate(inner))) => {
                let keys = inner
                    .mutations
                    .iter()
                    .map(|mutation| mutation.key.clone())
                    .collect::<Vec<_>>();
                (
                    ResponseCmd::TryAtomicMutate(engine.try_atomic_mutate(inner)?),
                    true,
                    Some(ApplyEvent {
                        source: kvpb::ApplyWatchEventSource::Commit,
                        commit_version: inner.commit_version,
                        keys,
                    }),
                )
            }
            (
                raftpb::CmdType::CmdInstallPreparedMvcc,
                Some(RequestCmd::InstallPreparedMvcc(inner)),
            ) => {
                let keys = if inner.watch_keys.is_empty() {
                    inner
                        .entries
                        .iter()
                        .map(|entry| entry.key.clone())
                        .collect::<Vec<_>>()
                } else {
                    inner.watch_keys.clone()
                };
                (
                    ResponseCmd::InstallPreparedMvcc(engine.install_prepared(inner)?),
                    true,
                    Some(ApplyEvent {
                        source: kvpb::ApplyWatchEventSource::Commit,
                        commit_version: inner.commit_version,
                        keys,
                    }),
                )
            }
            _ => {
                return Err(invalid_raft_command(
                    "command type and payload do not match",
                ))
            }
        };
        Ok((
            raftpb::Response {
                cmd: Some(response),
            },
            write,
            event,
        ))
    }

    fn read<T>(&self, f: impl FnOnce(&E) -> nokv_mvcc::Result<T>) -> nokv_mvcc::Result<T> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        f(&engine)
    }

    fn apply<T>(
        &self,
        f: impl FnOnce(&E) -> nokv_mvcc::Result<T>,
    ) -> nokv_mvcc::Result<(u64, u64, T)> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        let result = f(&engine)?;
        let (term, index) = self.advance_apply_index();
        Ok((term, index, result))
    }

    fn advance_apply_index(&self) -> (u64, u64) {
        let index = self.inner.applied_index.fetch_add(1, Ordering::AcqRel) + 1;
        let term = self.inner.term.load(Ordering::Acquire);
        (term, index)
    }

    fn record_applied_status(&self, term: u64, index: u64) {
        self.inner.term.store(term, Ordering::Release);
        let mut current = self.inner.applied_index.load(Ordering::Acquire);
        while current < index {
            match self.inner.applied_index.compare_exchange(
                current,
                index,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn publish_apply(
        &self,
        index: u64,
        term: u64,
        source: kvpb::ApplyWatchEventSource,
        commit_version: u64,
        keys: Vec<Vec<u8>>,
    ) {
        if keys.is_empty() {
            return;
        }
        let _ = self.inner.watch.send(kvpb::ApplyWatchEvent {
            region_id: self.inner.region_id,
            term,
            index,
            source: source as i32,
            commit_version,
            keys,
        });
    }
}

impl<E> RaftCommandExecutor for AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn execute_raft_command(
        &self,
        req: &raftpb::RaftCmdRequest,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
        self.execute_raft_command_inner(req)
    }
}

impl<E, S> RaftCommandExecutor for PersistentAppliedKvEngine<E, S>
where
    E: KvEngine,
    S: ApplyStatusSink,
{
    fn execute_raft_command(
        &self,
        req: &raftpb::RaftCmdRequest,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
        let before = self.engine.status().applied_index;
        let response = self.engine.execute_raft_command(req)?;
        self.persist_if_advanced(before)?;
        Ok(response)
    }
}

impl<E, S> PersistentAppliedKvEngine<E, S>
where
    E: KvEngine,
    S: ApplyStatusSink,
{
    pub fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        let before = self.engine.status().applied_index;
        let applied = self.engine.apply_openraft_entries(entries)?;
        self.persist_if_advanced(before)?;
        Ok(applied)
    }

    fn persist_if_advanced(&self, before: u64) -> nokv_mvcc::Result<()> {
        let status = self.engine.status();
        if status.applied_index != before {
            self.sink.save_apply_status(&status)?;
        }
        Ok(())
    }
}

fn invalid_raft_command(detail: &str) -> nokv_mvcc::Error {
    nokv_mvcc::Error::Backend(format!("invalid raft command: {detail}"))
}

fn encode_raft_response(response: &raftpb::RaftCmdResponse) -> nokv_mvcc::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut payload)
        .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
    Ok(payload)
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
            .map(|(_, _, result)| result)
    }

    fn commit(&self, req: &kvpb::CommitRequest) -> nokv_mvcc::Result<kvpb::CommitResponse> {
        let (term, index, result) = self.apply(|engine| engine.commit(req))?;
        self.publish_apply(
            index,
            term,
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
            .map(|(_, _, result)| result)
    }

    fn resolve_lock(
        &self,
        req: &kvpb::ResolveLockRequest,
    ) -> nokv_mvcc::Result<kvpb::ResolveLockResponse> {
        let (term, index, result) = self.apply(|engine| engine.resolve_lock(req))?;
        if req.commit_version != 0 {
            self.publish_apply(
                index,
                term,
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
            .map(|(_, _, result)| result)
    }

    fn txn_heartbeat(
        &self,
        req: &kvpb::TxnHeartBeatRequest,
    ) -> nokv_mvcc::Result<kvpb::TxnHeartBeatResponse> {
        self.apply(|engine| engine.txn_heartbeat(req))
            .map(|(_, _, result)| result)
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
        let (term, index, result) = self.apply(|engine| engine.try_atomic_mutate(req))?;
        self.publish_apply(
            index,
            term,
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
        let (term, index, result) = self.apply(|engine| engine.install_prepared(req))?;
        self.publish_apply(
            index,
            term,
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
            .map(|(_, _, result)| result)
    }
}

/// Marker for the future OpenRaft-backed implementation. Keeping this type in
/// place makes the dependency explicit while the first slice runs single-node
/// MVCC behind the existing wire contract.
#[derive(Debug, Default)]
pub struct OpenRaftRegion {
    _openraft: PhantomData<RaftStoreConfig>,
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
    use prost::Message;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct RecordingApplyStatusSink {
        statuses: Arc<Mutex<Vec<ApplyStatus>>>,
    }

    impl ApplyStatusSink for RecordingApplyStatusSink {
        fn save_apply_status(&self, status: &ApplyStatus) -> nokv_mvcc::Result<()> {
            self.statuses.lock().unwrap().push(status.clone());
            Ok(())
        }
    }

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
    fn proposal_round_trips_existing_raft_command_payload() {
        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 11,
                request_id: 7,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdGet as i32,
                cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 9,
                })),
            }],
        };
        let proposal = Proposal::from_raft_command(&command).unwrap();
        assert_eq!(proposal.region_id, 11);
        assert_eq!(proposal.decode_raft_command().unwrap(), command);
    }

    #[test]
    fn proposal_rejects_region_mismatch() {
        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 11,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut proposal = Proposal::from_raft_command(&command).unwrap();
        proposal.region_id = 12;
        let err = proposal.decode_raft_command().unwrap_err();
        assert!(matches!(err, Error::RegionMismatch { .. }));
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
    fn applied_kv_engine_can_start_from_persisted_status() {
        let engine = AppliedKvEngine::with_status(
            ApplyStatus {
                region_id: 7,
                term: 3,
                applied_index: 41,
            },
            MvccStore::new(),
        );
        assert_eq!(
            engine.status(),
            ApplyStatus {
                region_id: 7,
                term: 3,
                applied_index: 41,
            }
        );
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
        assert_eq!(engine.status().applied_index, 42);
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

    #[test]
    fn raft_command_with_multiple_writes_advances_index_once() {
        let engine = AppliedKvEngine::new(7, MvccStore::new());
        let mut watch = engine.subscribe();

        engine
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
                                    key: b"k1".to_vec(),
                                    value: b"v1".to_vec(),
                                    op: kvpb::mutation::Op::Put as i32,
                                    ..Default::default()
                                }],
                                commit_version: 2,
                                ..Default::default()
                            },
                        )),
                    },
                    raftpb::Request {
                        cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                        cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                            kvpb::TryAtomicMutateRequest {
                                mutations: vec![kvpb::Mutation {
                                    key: b"k2".to_vec(),
                                    value: b"v2".to_vec(),
                                    op: kvpb::mutation::Op::Put as i32,
                                    ..Default::default()
                                }],
                                commit_version: 3,
                                ..Default::default()
                            },
                        )),
                    },
                ],
            })
            .unwrap();

        assert_eq!(engine.status().applied_index, 1);
        assert_eq!(watch.try_recv().unwrap().index, 1);
        assert_eq!(watch.try_recv().unwrap().index, 1);
    }

    #[test]
    fn apply_openraft_entry_uses_committed_log_status() {
        let engine = AppliedKvEngine::new(7, MvccStore::new());
        let mut watch = engine.subscribe();
        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 55,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                    kvpb::TryAtomicMutateRequest {
                        mutations: vec![kvpb::Mutation {
                            key: b"k".to_vec(),
                            value: b"v".to_vec(),
                            op: kvpb::mutation::Op::Put as i32,
                            ..Default::default()
                        }],
                        commit_version: 9,
                        ..Default::default()
                    },
                )),
            }],
        };
        let entry = OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(Proposal::from_raft_command(&command).unwrap()),
        };

        let applied = engine.apply_openraft_entries([entry]).unwrap();

        assert_eq!(
            engine.status(),
            ApplyStatus {
                region_id: 7,
                term: 5,
                applied_index: 42,
            }
        );
        let event = watch.try_recv().unwrap();
        assert_eq!(event.term, 5);
        assert_eq!(event.index, 42);
        assert_eq!(event.commit_version, 9);
        let response = raftpb::RaftCmdResponse::decode(applied[0].payload.as_slice()).unwrap();
        assert_eq!(response.responses.len(), 1);
    }

    #[test]
    fn persistent_applied_engine_saves_status_after_write_command() {
        let sink = RecordingApplyStatusSink::default();
        let statuses = sink.statuses.clone();
        let engine =
            PersistentAppliedKvEngine::new(AppliedKvEngine::new(7, MvccStore::new()), sink);

        engine
            .execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(raftpb::CmdHeader {
                    region_id: 7,
                    ..Default::default()
                }),
                requests: vec![raftpb::Request {
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
                }],
            })
            .unwrap();

        assert_eq!(
            statuses.lock().unwrap().as_slice(),
            &[ApplyStatus {
                region_id: 7,
                term: 1,
                applied_index: 1,
            }]
        );
    }

    #[test]
    fn persistent_applied_engine_does_not_save_status_after_read_command() {
        let sink = RecordingApplyStatusSink::default();
        let statuses = sink.statuses.clone();
        let engine =
            PersistentAppliedKvEngine::new(AppliedKvEngine::new(7, MvccStore::new()), sink);

        engine
            .execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(raftpb::CmdHeader {
                    region_id: 7,
                    ..Default::default()
                }),
                requests: vec![raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdGet as i32,
                    cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                        key: b"k".to_vec(),
                        version: 1,
                    })),
                }],
            })
            .unwrap();

        assert!(statuses.lock().unwrap().is_empty());
    }
}
