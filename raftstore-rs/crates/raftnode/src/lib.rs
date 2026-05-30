//! OpenRaft boundary for Rust raftstore.
//!
//! The crate intentionally exposes a small NoKV-owned trait instead of leaking
//! OpenRaft types into server, MVCC, or proto-facing code. The concrete
//! OpenRaft-backed implementation will fill this boundary as region replication
//! is brought up.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nokv_mvcc::{KvEngine, MvccSnapshotEngine, MvccStore};
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::raft::v1 as raftpb;
use openraft::{
    error::{Fatal, InitializeError, RPCError, RaftError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    Config, Raft,
};
use prost::Message;
use tokio::sync::broadcast;

mod log_codec;
mod log_store;
mod network;
mod region_storage;
mod transport_codec;

pub use log_codec::{decode_log_entry, encode_log_entry};
pub use log_store::{RaftEntryLog, SegmentedEntryLog};
pub use network::{MemoryRaftNetworkFactory, MemoryRaftNetworkRegistry};
pub use openraft::BasicNode;
pub use region_storage::{RegionLogStorage, RegionSnapshotBuilder, RegionStateMachine};
pub use transport_codec::{
    decode_append_entries_request, decode_append_entries_response, decode_install_snapshot_request,
    decode_install_snapshot_response, decode_vote_request, decode_vote_response,
    encode_append_entries_request, encode_append_entries_response, encode_install_snapshot_request,
    encode_install_snapshot_response, encode_vote_request, encode_vote_response,
};

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
    #[error("invalid raft transport payload: {0}")]
    InvalidTransportPayload(String),
    #[error("raft log error: {0}")]
    RaftLog(#[from] nokv_raftlog::Error),
    #[error("raft metadata io error: {0}")]
    MetadataIo(#[from] std::io::Error),
    #[error("corrupt raft metadata: {0}")]
    CorruptMetadata(&'static str),
    #[error("invalid leader transfer target {target}: {reason}")]
    InvalidLeaderTransferTarget {
        target: NodeId,
        reason: &'static str,
    },
    #[error("leader transfer to peer {target} is not supported from local peer {local}: {reason}")]
    UnsupportedLeaderTransfer {
        local: NodeId,
        target: NodeId,
        reason: &'static str,
    },
    #[error("raft command encode error: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("raft command decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("openraft error: {0}")]
    OpenRaft(String),
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
    fn execute_raft_command<'a>(
        &'a self,
        req: &'a raftpb::RaftCmdRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<raftpb::RaftCmdResponse>> + Send + 'a;
}

pub trait RegionApplyEngine: ApplyStatusProvider + ApplyWatchProvider {
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>;
}

pub trait RegionSnapshotEngine: RegionApplyEngine {
    fn export_region_snapshot(&self) -> nokv_mvcc::Result<Vec<u8>>;
    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus>;
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
    fn execute_raft_command<'a>(
        &'a self,
        req: &'a raftpb::RaftCmdRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<raftpb::RaftCmdResponse>> + Send + 'a
    {
        async move { self.execute_raft_command_inner(req) }
    }
}

impl<E, S> RaftCommandExecutor for PersistentAppliedKvEngine<E, S>
where
    E: KvEngine,
    S: ApplyStatusSink,
{
    fn execute_raft_command<'a>(
        &'a self,
        req: &'a raftpb::RaftCmdRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<raftpb::RaftCmdResponse>> + Send + 'a
    {
        async move {
            let before = self.engine.status().applied_index;
            let response = self.engine.execute_raft_command(req).await?;
            self.persist_if_advanced(before)?;
            Ok(response)
        }
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

impl<E> RegionSnapshotEngine for AppliedKvEngine<E>
where
    E: KvEngine + MvccSnapshotEngine,
{
    fn export_region_snapshot(&self) -> nokv_mvcc::Result<Vec<u8>> {
        let status = self.status();
        let mvcc_snapshot = {
            let engine =
                self.inner.engine.lock().map_err(|_| {
                    nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned())
                })?;
            engine.export_mvcc_snapshot()?
        };
        Ok(RegionSnapshotPayload {
            format_version: 1,
            region_id: status.region_id,
            term: status.term,
            applied_index: status.applied_index,
            mvcc_snapshot: nokv_mvcc::encode_mvcc_snapshot(&mvcc_snapshot),
        }
        .encode_to_vec())
    }

    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus> {
        let payload = decode_region_snapshot_payload(snapshot)?;
        if payload.format_version != 1 {
            return Err(nokv_mvcc::Error::Decode(format!(
                "unsupported region snapshot format {}",
                payload.format_version
            )));
        }
        if payload.region_id != self.inner.region_id {
            return Err(nokv_mvcc::Error::Backend(format!(
                "region snapshot {} cannot install into region {}",
                payload.region_id, self.inner.region_id
            )));
        }
        let current = self.status();
        if payload.applied_index < current.applied_index {
            return Err(nokv_mvcc::Error::Backend(format!(
                "stale region snapshot index {} is behind current applied index {}",
                payload.applied_index, current.applied_index
            )));
        }
        if payload.applied_index == current.applied_index
            && current.applied_index != 0
            && payload.term != current.term
        {
            return Err(nokv_mvcc::Error::Backend(format!(
                "region snapshot term {} conflicts with current applied term {} at index {}",
                payload.term, current.term, payload.applied_index
            )));
        }
        let mvcc_snapshot = nokv_mvcc::decode_mvcc_snapshot(&payload.mvcc_snapshot)?;
        {
            let engine =
                self.inner.engine.lock().map_err(|_| {
                    nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned())
                })?;
            engine.install_mvcc_snapshot(mvcc_snapshot)?;
        }
        self.record_applied_status(payload.term, payload.applied_index);
        Ok(self.status())
    }
}

impl<E, S> RegionSnapshotEngine for PersistentAppliedKvEngine<E, S>
where
    E: KvEngine + MvccSnapshotEngine,
    S: ApplyStatusSink,
{
    fn export_region_snapshot(&self) -> nokv_mvcc::Result<Vec<u8>> {
        self.engine.export_region_snapshot()
    }

    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus> {
        let status = self.engine.install_region_snapshot(snapshot)?;
        self.sink.save_apply_status(&status)?;
        Ok(status)
    }
}

impl<E> RegionApplyEngine for AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        self.apply_openraft_entries(entries)
    }
}

impl<E, S> RegionApplyEngine for PersistentAppliedKvEngine<E, S>
where
    E: KvEngine,
    S: ApplyStatusSink,
{
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        self.apply_openraft_entries(entries)
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

fn decode_raft_response(payload: &[u8]) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
    raftpb::RaftCmdResponse::decode(payload)
        .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))
}

fn decode_region_snapshot_payload(snapshot: &[u8]) -> nokv_mvcc::Result<RegionSnapshotPayload> {
    RegionSnapshotPayload::decode(snapshot).map_err(|err| nokv_mvcc::Error::Decode(err.to_string()))
}

pub(crate) fn decode_region_snapshot_status(snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus> {
    let payload = decode_region_snapshot_payload(snapshot)?;
    if payload.format_version != 1 {
        return Err(nokv_mvcc::Error::Decode(format!(
            "unsupported region snapshot format {}",
            payload.format_version
        )));
    }
    Ok(ApplyStatus {
        region_id: payload.region_id,
        term: payload.term,
        applied_index: payload.applied_index,
    })
}

#[derive(Clone, PartialEq, Message)]
struct RegionSnapshotPayload {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(uint64, tag = "2")]
    region_id: RegionId,
    #[prost(uint64, tag = "3")]
    term: u64,
    #[prost(uint64, tag = "4")]
    applied_index: u64,
    #[prost(bytes = "vec", tag = "5")]
    mvcc_snapshot: Vec<u8>,
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

#[derive(Clone)]
pub struct OpenRaftRegion<E = AppliedKvEngine<MvccStore>> {
    node_id: NodeId,
    raft: Raft<RaftStoreConfig>,
    apply_engine: E,
}

impl<E> OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    pub async fn open_with_network<N>(
        node_id: NodeId,
        region_id: RegionId,
        log_store: RegionLogStorage,
        state_machine: RegionStateMachine<E>,
        network: N,
    ) -> Result<OpenRaftRegion<E>, Error>
    where
        N: RaftNetworkFactory<RaftStoreConfig>,
    {
        let config = default_openraft_config(region_id)?;
        Self::open_with_network_config(
            node_id,
            region_id,
            log_store,
            state_machine,
            network,
            config,
        )
        .await
    }

    async fn open_with_network_config<N>(
        node_id: NodeId,
        _region_id: RegionId,
        mut log_store: RegionLogStorage,
        mut state_machine: RegionStateMachine<E>,
        network: N,
        config: Arc<Config>,
    ) -> Result<OpenRaftRegion<E>, Error>
    where
        N: RaftNetworkFactory<RaftStoreConfig>,
    {
        if let Some(membership) = log_store
            .latest_membership()
            .map_err(|err| Error::OpenRaft(err.to_string()))?
        {
            state_machine.restore_membership(membership);
        }
        let apply_status = state_machine.apply_engine().apply_status();
        if apply_status.applied_index != 0 {
            state_machine.restore_last_applied(
                log_store
                    .log_id_at_index(apply_status.applied_index)
                    .map_err(|err| Error::OpenRaft(err.to_string()))?,
            );
        }
        let voters = state_machine.membership().voter_ids().collect::<Vec<_>>();
        if voters.as_slice() == [node_id] {
            log_store
                .seed_single_node_vote_above_log(node_id)
                .map_err(|err| Error::OpenRaft(err.to_string()))?;
        }
        let apply_engine = state_machine.apply_engine().clone();
        let raft = Raft::new(node_id, config, network, log_store, state_machine)
            .await
            .map_err(openraft_error)?;
        Ok(OpenRaftRegion {
            node_id,
            raft,
            apply_engine,
        })
    }

    #[cfg(test)]
    async fn open_with_network_for_test<N, F>(
        node_id: NodeId,
        region_id: RegionId,
        log_store: RegionLogStorage,
        state_machine: RegionStateMachine<E>,
        network: N,
        configure: F,
    ) -> Result<OpenRaftRegion<E>, Error>
    where
        N: RaftNetworkFactory<RaftStoreConfig>,
        F: FnOnce(&mut Config),
    {
        let mut config = Config {
            cluster_name: format!("nokv-region-{region_id}"),
            ..Default::default()
        };
        configure(&mut config);
        let config = Arc::new(
            config
                .validate()
                .map_err(|err| Error::OpenRaft(err.to_string()))?,
        );
        Self::open_with_network_config(
            node_id,
            region_id,
            log_store,
            state_machine,
            network,
            config,
        )
        .await
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn bootstrap_single_node(
        node_id: NodeId,
        region_id: RegionId,
        log_store: RegionLogStorage,
        state_machine: RegionStateMachine<E>,
    ) -> Result<OpenRaftRegion<E>, Error> {
        let region = Self::open_with_network(
            node_id,
            region_id,
            log_store,
            state_machine,
            NoopNetworkFactory,
        )
        .await?;
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(format!("local-{node_id}")));
        if region.initialize_members(members).await? {
            region.wait_for_leader(node_id).await?;
        } else {
            region.elect_and_wait(node_id).await?;
        }
        region
            .raft
            .ensure_linearizable()
            .await
            .map_err(openraft_api_error)?;
        Ok(region)
    }

    pub fn raft_handle(&self) -> Raft<RaftStoreConfig> {
        self.raft.clone()
    }

    pub async fn initialize_members(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<bool, Error> {
        match self.raft.initialize(members).await {
            Ok(()) => Ok(true),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(false),
            Err(err) => return Err(openraft_api_error(err)),
        }
    }

    pub async fn elect_and_wait(&self, node_id: NodeId) -> Result<(), Error> {
        self.raft.trigger().elect().await.map_err(openraft_error)?;
        self.wait_for_leader(node_id).await
    }

    pub async fn wait_for_leader(&self, node_id: NodeId) -> Result<(), Error> {
        self.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| metrics.current_leader == Some(node_id),
                "raft leader election",
            )
            .await
            .map_err(|err| Error::OpenRaft(err.to_string()))?;
        Ok(())
    }

    pub async fn add_voter(&self, node_id: NodeId, node: BasicNode) -> Result<(), Error> {
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(openraft_api_error)?;
        self.raft
            .change_membership(
                openraft::ChangeMembers::AddVoterIds(BTreeSet::from([node_id])),
                true,
            )
            .await
            .map_err(openraft_api_error)?;
        self.wait_for_voter(node_id, true).await
    }

    pub async fn remove_voter(&self, node_id: NodeId, retain: bool) -> Result<(), Error> {
        self.raft
            .change_membership(
                openraft::ChangeMembers::RemoveVoters(BTreeSet::from([node_id])),
                retain,
            )
            .await
            .map_err(openraft_api_error)?;
        self.wait_for_voter(node_id, false).await
    }

    pub async fn transfer_leader(&self, node_id: NodeId) -> Result<(), Error> {
        if node_id == 0 {
            return Err(Error::InvalidLeaderTransferTarget {
                target: node_id,
                reason: "peer id is required",
            });
        }

        let metrics = self.raft.metrics().borrow().clone();
        let is_voter = metrics
            .membership_config
            .voter_ids()
            .any(|voter| voter == node_id);
        if !is_voter {
            return Err(Error::InvalidLeaderTransferTarget {
                target: node_id,
                reason: "target is not a voter",
            });
        }
        if metrics.current_leader == Some(node_id) {
            return Ok(());
        }
        Err(Error::UnsupportedLeaderTransfer {
            local: self.node_id,
            target: node_id,
            reason: "OpenRaft 0.9 does not expose source-initiated directed transfer",
        })
    }

    pub async fn wait_for_voter(&self, node_id: NodeId, present: bool) -> Result<(), Error> {
        self.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    let membership = &metrics.membership_config;
                    let uniform = membership.membership().get_joint_config().len() == 1;
                    let observed = membership.voter_ids().any(|voter| voter == node_id);
                    uniform && observed == present
                },
                "raft membership voter state",
            )
            .await
            .map_err(|err| Error::OpenRaft(err.to_string()))?;
        Ok(())
    }

    pub async fn propose(&self, proposal: Proposal) -> Result<AppliedProposal, Error> {
        let response = self
            .raft
            .client_write(proposal)
            .await
            .map_err(openraft_api_error)?;
        Ok(response.data)
    }

    pub async fn trigger_snapshot(&self) -> Result<(), Error> {
        self.raft.trigger().snapshot().await.map_err(openraft_error)
    }

    pub async fn trigger_log_purge(&self, upto: u64) -> Result<(), Error> {
        self.raft
            .trigger()
            .purge_log(upto)
            .await
            .map_err(openraft_error)
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| Error::OpenRaft(err.to_string()))
    }
}

impl<E> ApplyStatusProvider for OpenRaftRegion<E>
where
    E: ApplyStatusProvider,
{
    fn apply_status(&self) -> ApplyStatus {
        self.apply_engine.apply_status()
    }
}

impl<E> ApplyWatchProvider for OpenRaftRegion<E>
where
    E: ApplyWatchProvider,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.apply_engine.subscribe_apply()
    }
}

impl<E> RaftCommandExecutor for OpenRaftRegion<E>
where
    E: RegionSnapshotEngine + RaftCommandExecutor,
{
    fn execute_raft_command<'a>(
        &'a self,
        req: &'a raftpb::RaftCmdRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<raftpb::RaftCmdResponse>> + Send + 'a
    {
        async move {
            let proposal = Proposal::from_raft_command(req)
                .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
            if raft_command_is_read_only(req) {
                let applied_region_id = self.apply_engine.apply_status().region_id;
                if proposal.region_id != applied_region_id {
                    return Err(nokv_mvcc::Error::Backend(
                        Error::LogRegionMismatch {
                            record_region_id: applied_region_id,
                            proposal_region_id: proposal.region_id,
                        }
                        .to_string(),
                    ));
                }
                self.raft.ensure_linearizable().await.map_err(|err| {
                    nokv_mvcc::Error::Backend(openraft_api_error(err).to_string())
                })?;
                return self.apply_engine.execute_raft_command(req).await;
            }
            let applied = self
                .propose(proposal)
                .await
                .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
            decode_raft_response(&applied.payload)
        }
    }
}

fn raft_command_is_read_only(req: &raftpb::RaftCmdRequest) -> bool {
    !req.requests.is_empty()
        && req.requests.iter().all(|request| {
            matches!(
                raftpb::CmdType::try_from(request.cmd_type),
                Ok(raftpb::CmdType::CmdGet | raftpb::CmdType::CmdScan)
            )
        })
}

#[derive(Clone, Default)]
struct NoopNetworkFactory;

struct NoopNetwork;

#[derive(Debug, thiserror::Error)]
#[error("single-node raft network has no remote peers")]
struct NoopNetworkError;

impl RaftNetworkFactory<RaftStoreConfig> for NoopNetworkFactory {
    type Network = NoopNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        NoopNetwork
    }
}

impl RaftNetwork<RaftStoreConfig> for NoopNetwork {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        Err(remote_unreachable())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        Err(remote_unreachable())
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        Err(remote_unreachable())
    }
}

fn remote_unreachable<NID, N, E>() -> RPCError<NID, N, E>
where
    NID: openraft::NodeId,
    N: openraft::Node,
    E: StdError,
{
    RPCError::Unreachable(Unreachable::new(&NoopNetworkError))
}

fn openraft_error(err: Fatal<NodeId>) -> Error {
    Error::OpenRaft(err.to_string())
}

fn openraft_api_error<E>(err: RaftError<NodeId, E>) -> Error
where
    E: StdError,
{
    Error::OpenRaft(err.to_string())
}

fn default_openraft_config(region_id: RegionId) -> Result<Arc<Config>, Error> {
    Ok(Arc::new(
        Config {
            cluster_name: format!("nokv-region-{region_id}"),
            ..Default::default()
        }
        .validate()
        .map_err(|err| Error::OpenRaft(err.to_string()))?,
    ))
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

    #[tokio::test]
    async fn openraft_region_bootstraps_single_node_and_applies_proposal() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = RegionLogStorage::new(log);
        let state_machine = RegionStateMachine::new(AppliedKvEngine::new(7, MvccStore::new()));
        let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
            .await
            .unwrap();

        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 1,
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
        };
        let applied = raft
            .propose(Proposal::from_raft_command(&command).unwrap())
            .await
            .unwrap();
        assert_eq!(applied.region_id, 7);
        assert_eq!(applied.index, 2);
    }

    #[tokio::test]
    async fn openraft_region_serves_read_without_advancing_apply_index() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = RegionLogStorage::new(log);
        let state_machine = RegionStateMachine::new(AppliedKvEngine::new(7, MvccStore::new()));
        let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
            .await
            .unwrap();

        let write = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 1,
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
        };
        raft.execute_raft_command(&write).await.unwrap();
        let applied_after_write = raft.apply_status().applied_index;
        assert!(applied_after_write > 0);

        let read = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 2,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdGet as i32,
                cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 2,
                })),
            }],
        };
        let response = raft.execute_raft_command(&read).await.unwrap();

        assert_eq!(raft.apply_status().applied_index, applied_after_write);
        match response.responses[0].cmd.as_ref().unwrap() {
            raftpb::response::Cmd::Get(get) => assert_eq!(get.value, b"v".to_vec()),
            other => panic!("unexpected read response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn openraft_region_restart_write_returns_client_response() {
        let dir = tempfile::tempdir().unwrap();
        let status = {
            let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
            let log_store = RegionLogStorage::new(log);
            let state_machine = RegionStateMachine::new(AppliedKvEngine::new(7, MvccStore::new()));
            let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
                .await
                .unwrap();
            raft.execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(raftpb::CmdHeader {
                    region_id: 7,
                    request_id: 1,
                    ..Default::default()
                }),
                requests: vec![raftpb::Request {
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
                }],
            })
            .await
            .unwrap();
            raft.apply_status()
        };

        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = RegionLogStorage::new(log);
        let state_machine =
            RegionStateMachine::new(AppliedKvEngine::with_status(status, MvccStore::new()));
        let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
            .await
            .unwrap();

        let response = raft
            .execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(raftpb::CmdHeader {
                    region_id: 7,
                    request_id: 2,
                    ..Default::default()
                }),
                requests: vec![raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                    cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                        kvpb::TryAtomicMutateRequest {
                            mutations: vec![kvpb::Mutation {
                                key: b"k2".to_vec(),
                                value: b"v2".to_vec(),
                                op: kvpb::mutation::Op::Put as i32,
                                ..Default::default()
                            }],
                            commit_version: 4,
                            ..Default::default()
                        },
                    )),
                }],
            })
            .await
            .unwrap();

        assert_eq!(response.responses.len(), 1);
        assert!(matches!(
            response.responses[0].cmd,
            Some(raftpb::response::Cmd::TryAtomicMutate(_))
        ));
    }

    #[tokio::test]
    async fn openraft_region_replicates_proposal_to_memory_peers() {
        let registry = MemoryRaftNetworkRegistry::default();
        let mut dirs = Vec::new();
        let mut regions = Vec::new();
        let mut engines = BTreeMap::new();

        for node_id in 1..=3 {
            let dir = tempfile::tempdir().unwrap();
            let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
            let log_store = RegionLogStorage::new(log);
            let engine = AppliedKvEngine::new(7, MvccStore::new());
            let state_machine = RegionStateMachine::new(engine.clone());
            let region = OpenRaftRegion::open_with_network(
                node_id,
                7,
                log_store,
                state_machine,
                registry.factory(),
            )
            .await
            .unwrap();
            registry.register(node_id, region.raft_handle());
            dirs.push(dir);
            engines.insert(node_id, engine);
            regions.push(region);
        }

        let mut members = BTreeMap::new();
        members.insert(1, BasicNode::new("node-1"));
        members.insert(2, BasicNode::new("node-2"));
        members.insert(3, BasicNode::new("node-3"));
        regions[0].initialize_members(members).await.unwrap();
        regions[0].wait_for_leader(1).await.unwrap();

        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 1,
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
                        commit_version: 10,
                        ..Default::default()
                    },
                )),
            }],
        };

        let applied = regions[0]
            .propose(Proposal::from_raft_command(&command).unwrap())
            .await
            .unwrap();
        for region in &regions {
            region
                .raft_handle()
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(Some(applied.index), "memory peer proposal")
                .await
                .unwrap();
        }

        for node_id in 1..=3 {
            let get = engines
                .get(&node_id)
                .unwrap()
                .get(&kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 10,
                })
                .unwrap();
            assert_eq!(get.value, b"v".to_vec(), "node {node_id} did not apply");
        }
    }

    #[tokio::test]
    async fn openraft_region_adds_voter_and_replicates_to_new_peer() {
        let registry = MemoryRaftNetworkRegistry::default();
        let mut dirs = Vec::new();
        let mut regions = BTreeMap::new();
        let mut engines = BTreeMap::new();

        for node_id in 1..=2 {
            let dir = tempfile::tempdir().unwrap();
            let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
            let log_store = RegionLogStorage::new(log);
            let engine = AppliedKvEngine::new(7, MvccStore::new());
            let state_machine = RegionStateMachine::new(engine.clone());
            let region = OpenRaftRegion::open_with_network(
                node_id,
                7,
                log_store,
                state_machine,
                registry.factory(),
            )
            .await
            .unwrap();
            registry.register(node_id, region.raft_handle());
            dirs.push(dir);
            engines.insert(node_id, engine);
            regions.insert(node_id, region);
        }

        let leader = regions.get(&1).unwrap();
        leader
            .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        leader.wait_for_leader(1).await.unwrap();
        leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();

        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 2,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                    kvpb::TryAtomicMutateRequest {
                        mutations: vec![kvpb::Mutation {
                            key: b"joined".to_vec(),
                            value: b"yes".to_vec(),
                            op: kvpb::mutation::Op::Put as i32,
                            ..Default::default()
                        }],
                        commit_version: 20,
                        ..Default::default()
                    },
                )),
            }],
        };

        let applied = leader
            .propose(Proposal::from_raft_command(&command).unwrap())
            .await
            .unwrap();
        for region in regions.values() {
            region
                .raft_handle()
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(Some(applied.index), "added voter proposal")
                .await
                .unwrap();
        }

        for node_id in 1..=2 {
            let get = engines
                .get(&node_id)
                .unwrap()
                .get(&kvpb::GetRequest {
                    key: b"joined".to_vec(),
                    version: 20,
                })
                .unwrap();
            assert_eq!(
                get.value,
                b"yes".to_vec(),
                "node {node_id} did not apply after membership change"
            );
        }

        leader.remove_voter(2, false).await.unwrap();
        let voters = leader
            .raft_handle()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect::<Vec<_>>();
        assert_eq!(voters, vec![1]);
    }

    #[tokio::test]
    async fn openraft_region_restarts_after_membership_change_without_single_node_vote() {
        let registry = MemoryRaftNetworkRegistry::default();
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();

        let leader_engine = AppliedKvEngine::new(7, MvccStore::new());
        let leader = OpenRaftRegion::open_with_network(
            1,
            7,
            RegionLogStorage::new(SegmentedEntryLog::open(7, leader_dir.path()).unwrap()),
            RegionStateMachine::new(leader_engine.clone()),
            registry.factory(),
        )
        .await
        .unwrap();
        registry.register(1, leader.raft_handle());

        let follower_engine = AppliedKvEngine::new(7, MvccStore::new());
        let follower = OpenRaftRegion::open_with_network(
            2,
            7,
            RegionLogStorage::new(SegmentedEntryLog::open(7, follower_dir.path()).unwrap()),
            RegionStateMachine::new(follower_engine.clone()),
            registry.factory(),
        )
        .await
        .unwrap();
        registry.register(2, follower.raft_handle());

        leader
            .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        leader.wait_for_leader(1).await.unwrap();
        leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();
        follower.wait_for_voter(2, true).await.unwrap();

        let before_restart = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 10,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                    kvpb::TryAtomicMutateRequest {
                        mutations: vec![kvpb::Mutation {
                            key: b"before-restart".to_vec(),
                            value: b"ok".to_vec(),
                            op: kvpb::mutation::Op::Put as i32,
                            ..Default::default()
                        }],
                        commit_version: 30,
                        ..Default::default()
                    },
                )),
            }],
        };
        let applied_before_restart = leader
            .propose(Proposal::from_raft_command(&before_restart).unwrap())
            .await
            .unwrap();
        for region in [&leader, &follower] {
            region
                .raft_handle()
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(
                    Some(applied_before_restart.index),
                    "membership restart baseline proposal",
                )
                .await
                .unwrap();
        }

        let leader_status = leader.apply_status();
        let follower_status = follower.apply_status();
        leader.shutdown().await.unwrap();
        follower.shutdown().await.unwrap();
        drop(leader);
        drop(follower);
        drop(leader_engine);
        drop(follower_engine);

        let restarted_registry = MemoryRaftNetworkRegistry::default();
        let restarted_leader_engine = AppliedKvEngine::with_status(leader_status, MvccStore::new());
        let restarted_leader = OpenRaftRegion::open_with_network(
            1,
            7,
            RegionLogStorage::new(SegmentedEntryLog::open(7, leader_dir.path()).unwrap()),
            RegionStateMachine::new(restarted_leader_engine.clone()),
            restarted_registry.factory(),
        )
        .await
        .unwrap();
        restarted_registry.register(1, restarted_leader.raft_handle());

        let restarted_follower_engine =
            AppliedKvEngine::with_status(follower_status, MvccStore::new());
        let restarted_follower = OpenRaftRegion::open_with_network(
            2,
            7,
            RegionLogStorage::new(SegmentedEntryLog::open(7, follower_dir.path()).unwrap()),
            RegionStateMachine::new(restarted_follower_engine.clone()),
            restarted_registry.factory(),
        )
        .await
        .unwrap();
        restarted_registry.register(2, restarted_follower.raft_handle());

        restarted_leader.wait_for_voter(2, true).await.unwrap();
        restarted_follower.wait_for_voter(2, true).await.unwrap();
        restarted_leader.elect_and_wait(1).await.unwrap();
        restarted_follower.wait_for_leader(1).await.unwrap();

        let after_restart = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 11,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                    kvpb::TryAtomicMutateRequest {
                        mutations: vec![kvpb::Mutation {
                            key: b"after-restart".to_vec(),
                            value: b"still-quorum".to_vec(),
                            op: kvpb::mutation::Op::Put as i32,
                            ..Default::default()
                        }],
                        commit_version: 40,
                        ..Default::default()
                    },
                )),
            }],
        };
        let applied_after_restart = restarted_leader
            .propose(Proposal::from_raft_command(&after_restart).unwrap())
            .await
            .unwrap();
        for region in [&restarted_leader, &restarted_follower] {
            region
                .raft_handle()
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(
                    Some(applied_after_restart.index),
                    "post-membership-restart proposal",
                )
                .await
                .unwrap();
        }

        for (node_id, engine) in [(1, restarted_leader_engine), (2, restarted_follower_engine)] {
            let get = engine
                .get(&kvpb::GetRequest {
                    key: b"after-restart".to_vec(),
                    version: 40,
                })
                .unwrap();
            assert_eq!(
                get.value,
                b"still-quorum".to_vec(),
                "node {node_id} did not apply after multi-voter restart"
            );
        }
    }

    #[tokio::test]
    async fn openraft_region_catches_up_joining_peer_from_snapshot() {
        let registry = MemoryRaftNetworkRegistry::default();
        let leader_dir = tempfile::tempdir().unwrap();
        let leader_log = SegmentedEntryLog::open(7, leader_dir.path()).unwrap();
        let leader_log_store = RegionLogStorage::new(leader_log);
        let leader_engine = AppliedKvEngine::new(7, MvccStore::new());
        let leader = OpenRaftRegion::open_with_network_for_test(
            1,
            7,
            leader_log_store,
            RegionStateMachine::new(leader_engine.clone()),
            registry.factory(),
            |config| {
                config.snapshot_policy = openraft::SnapshotPolicy::Never;
                config.replication_lag_threshold = 1;
                config.max_in_snapshot_log_to_keep = 0;
            },
        )
        .await
        .unwrap();
        registry.register(1, leader.raft_handle());
        leader
            .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        leader.wait_for_leader(1).await.unwrap();

        let mut last_applied = None;
        for version in 1..=8 {
            let command = raftpb::RaftCmdRequest {
                header: Some(raftpb::CmdHeader {
                    region_id: 7,
                    request_id: version,
                    ..Default::default()
                }),
                requests: vec![raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                    cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                        kvpb::TryAtomicMutateRequest {
                            mutations: vec![kvpb::Mutation {
                                key: format!("k{version}").into_bytes(),
                                value: format!("v{version}").into_bytes(),
                                op: kvpb::mutation::Op::Put as i32,
                                ..Default::default()
                            }],
                            commit_version: version,
                            ..Default::default()
                        },
                    )),
                }],
            };
            last_applied = Some(
                leader
                    .propose(Proposal::from_raft_command(&command).unwrap())
                    .await
                    .unwrap(),
            );
        }
        let last_applied = last_applied.unwrap();
        leader.trigger_snapshot().await.unwrap();
        leader
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .map(|snapshot| snapshot.index >= last_applied.index)
                        .unwrap_or(false)
                },
                "leader snapshot before joining peer",
            )
            .await
            .unwrap();
        leader.trigger_log_purge(last_applied.index).await.unwrap();
        leader
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    metrics
                        .purged
                        .map(|purged| purged.index >= last_applied.index)
                        .unwrap_or(false)
                },
                "leader purges snapshot-covered log",
            )
            .await
            .unwrap();

        let joining_dir = tempfile::tempdir().unwrap();
        let joining_log = SegmentedEntryLog::open(7, joining_dir.path()).unwrap();
        let joining_log_store = RegionLogStorage::new(joining_log);
        let joining_engine = AppliedKvEngine::new(7, MvccStore::new());
        let joining = OpenRaftRegion::open_with_network_for_test(
            2,
            7,
            joining_log_store,
            RegionStateMachine::new(joining_engine.clone()),
            registry.factory(),
            |config| {
                config.snapshot_policy = openraft::SnapshotPolicy::Never;
                config.replication_lag_threshold = 1;
                config.max_in_snapshot_log_to_keep = 0;
            },
        )
        .await
        .unwrap();
        registry.register(2, joining.raft_handle());

        leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();
        joining
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(last_applied.index), "joining peer snapshot catch-up")
            .await
            .unwrap();
        assert!(
            joining.raft_handle().metrics().borrow().snapshot.is_some(),
            "joining peer should install a snapshot instead of replaying purged logs"
        );

        let current = joining_engine
            .get(&kvpb::GetRequest {
                key: b"k8".to_vec(),
                version: 8,
            })
            .unwrap();
        assert_eq!(current.value, b"v8".to_vec());
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

    #[tokio::test]
    async fn applied_kv_engine_executes_existing_raft_command_payload() {
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
            .await
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

    #[tokio::test]
    async fn raft_command_with_multiple_writes_advances_index_once() {
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
            .await
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

    #[tokio::test]
    async fn persistent_applied_engine_saves_status_after_write_command() {
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
            .await
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

    #[tokio::test]
    async fn persistent_applied_engine_does_not_save_status_after_read_command() {
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
            .await
            .unwrap();

        assert!(statuses.lock().unwrap().is_empty());
    }
}
