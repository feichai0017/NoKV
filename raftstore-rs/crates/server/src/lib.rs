//! Tonic services for the Rust raftstore data plane.
//!
//! This crate owns the external gRPC boundary. It keeps the existing NoKV
//! protobuf contract intact while the Rust state-machine and replication layers
//! are brought up behind the service.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use nokv_mvcc::{KvEngine, MvccStore};
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::raft::v1 as raftpb;
use nokv_raftnode::{
    AppliedKvEngine, ApplyStatusProvider, ApplyStatusSink, ApplyWatchProvider, BasicNode,
    PersistentAppliedKvEngine, RaftCommandExecutor, RegionApplyEngine,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

mod admission;

pub use adminpb::raft_admin_server::RaftAdminServer;
pub use admission::RegionAdmission;
pub use kvpb::store_kv_server::StoreKvServer;

#[derive(Debug, Clone, Default)]
pub struct StoreKvService<E = MvccStore> {
    engine: E,
    admission: RegionAdmission,
}

impl<E> StoreKvService<E> {
    pub fn new(engine: E) -> Self {
        Self::with_admission(engine, RegionAdmission::default())
    }

    pub fn with_admission(engine: E, admission: RegionAdmission) -> Self {
        Self { engine, admission }
    }
}

impl<E> StoreKvService<E>
where
    E: RaftCommandExecutor,
{
    async fn execute_raft_request(
        &self,
        context: Option<&kvpb::Context>,
        request: raftpb::Request,
        operation: &str,
    ) -> Result<raftpb::response::Cmd, Status> {
        let context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
        let response = self
            .engine
            .execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(header_from_context(context)),
                requests: vec![request],
            })
            .await
            .map_err(internal_error)?;
        let mut responses = response.responses.into_iter();
        let Some(response) = responses.next() else {
            return Err(raft_payload_error(operation, "missing raft response"));
        };
        if responses.next().is_some() {
            return Err(raft_payload_error(operation, "multiple raft responses"));
        }
        response
            .cmd
            .ok_or_else(|| raft_payload_error(operation, "missing raft payload"))
    }
}

#[tonic::async_trait]
impl<E> kvpb::store_kv_server::StoreKv for StoreKvService<E>
where
    E: RaftCommandExecutor + ApplyWatchProvider,
{
    async fn get(
        &self,
        request: Request<kvpb::KvGetRequest>,
    ) -> Result<Response<kvpb::KvGetResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("get request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvGetResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdGet as i32,
                    cmd: Some(raftpb::request::Cmd::Get(inner.clone())),
                },
                "get",
            )
            .await?
        {
            raftpb::response::Cmd::Get(response) => response,
            _ => return Err(raft_payload_error("get", "unexpected get payload")),
        };
        Ok(Response::new(kvpb::KvGetResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn batch_get(
        &self,
        request: Request<kvpb::KvBatchGetRequest>,
    ) -> Result<Response<kvpb::KvBatchGetResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("batch get request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            inner.requests.iter().map(|req| req.key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvBatchGetResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let context = request
            .context
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let command = raftpb::RaftCmdRequest {
            header: Some(header_from_context(context)),
            requests: inner
                .requests
                .iter()
                .map(|req| raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdGet as i32,
                    cmd: Some(raftpb::request::Cmd::Get(req.clone())),
                })
                .collect(),
        };
        let command_response = self
            .engine
            .execute_raft_command(&command)
            .await
            .map_err(internal_error)?;
        let mut responses = Vec::with_capacity(command_response.responses.len());
        for response in command_response.responses {
            match response.cmd {
                Some(raftpb::response::Cmd::Get(response)) => responses.push(response),
                _ => {
                    return Err(raft_payload_error(
                        "batch get",
                        "unexpected batch get payload",
                    ))
                }
            }
        }
        let response = kvpb::BatchGetResponse { responses };
        Ok(Response::new(kvpb::KvBatchGetResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn scan(
        &self,
        request: Request<kvpb::KvScanRequest>,
    ) -> Result<Response<kvpb::KvScanResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("scan request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.start_key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvScanResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdScan as i32,
                    cmd: Some(raftpb::request::Cmd::Scan(inner.clone())),
                },
                "scan",
            )
            .await?
        {
            raftpb::response::Cmd::Scan(response) => response,
            _ => return Err(raft_payload_error("scan", "unexpected scan payload")),
        };
        Ok(Response::new(kvpb::KvScanResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn prewrite(
        &self,
        request: Request<kvpb::KvPrewriteRequest>,
    ) -> Result<Response<kvpb::KvPrewriteResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("prewrite request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            inner
                .mutations
                .iter()
                .map(|mutation| mutation.key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvPrewriteResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdPrewrite as i32,
                    cmd: Some(raftpb::request::Cmd::Prewrite(inner.clone())),
                },
                "prewrite",
            )
            .await?
        {
            raftpb::response::Cmd::Prewrite(response) => response,
            _ => {
                return Err(raft_payload_error(
                    "prewrite",
                    "unexpected prewrite payload",
                ))
            }
        };
        Ok(Response::new(kvpb::KvPrewriteResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn commit(
        &self,
        request: Request<kvpb::KvCommitRequest>,
    ) -> Result<Response<kvpb::KvCommitResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("commit request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            inner.keys.iter().map(|key| key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvCommitResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdCommit as i32,
                    cmd: Some(raftpb::request::Cmd::Commit(inner.clone())),
                },
                "commit",
            )
            .await?
        {
            raftpb::response::Cmd::Commit(response) => response,
            _ => return Err(raft_payload_error("commit", "unexpected commit payload")),
        };
        Ok(Response::new(kvpb::KvCommitResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn batch_rollback(
        &self,
        request: Request<kvpb::KvBatchRollbackRequest>,
    ) -> Result<Response<kvpb::KvBatchRollbackResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("batch rollback request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            inner.keys.iter().map(|key| key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvBatchRollbackResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdBatchRollback as i32,
                    cmd: Some(raftpb::request::Cmd::BatchRollback(inner.clone())),
                },
                "batch rollback",
            )
            .await?
        {
            raftpb::response::Cmd::BatchRollback(response) => response,
            _ => {
                return Err(raft_payload_error(
                    "batch rollback",
                    "unexpected batch rollback payload",
                ))
            }
        };
        Ok(Response::new(kvpb::KvBatchRollbackResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn resolve_lock(
        &self,
        request: Request<kvpb::KvResolveLockRequest>,
    ) -> Result<Response<kvpb::KvResolveLockResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("resolve lock request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            inner.keys.iter().map(|key| key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvResolveLockResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdResolveLock as i32,
                    cmd: Some(raftpb::request::Cmd::ResolveLock(inner.clone())),
                },
                "resolve lock",
            )
            .await?
        {
            raftpb::response::Cmd::ResolveLock(response) => response,
            _ => {
                return Err(raft_payload_error(
                    "resolve lock",
                    "unexpected resolve lock payload",
                ))
            }
        };
        Ok(Response::new(kvpb::KvResolveLockResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn check_txn_status(
        &self,
        request: Request<kvpb::KvCheckTxnStatusRequest>,
    ) -> Result<Response<kvpb::KvCheckTxnStatusResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("check txn status request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.primary_key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvCheckTxnStatusResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdCheckTxnStatus as i32,
                    cmd: Some(raftpb::request::Cmd::CheckTxnStatus(inner.clone())),
                },
                "check txn status",
            )
            .await?
        {
            raftpb::response::Cmd::CheckTxnStatus(response) => response,
            _ => {
                return Err(raft_payload_error(
                    "check txn status",
                    "unexpected check txn status payload",
                ))
            }
        };
        Ok(Response::new(kvpb::KvCheckTxnStatusResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn txn_heart_beat(
        &self,
        request: Request<kvpb::KvTxnHeartBeatRequest>,
    ) -> Result<Response<kvpb::KvTxnHeartBeatResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("txn heart beat request missing payload"))?;
        if let Some(region_error) = self.admission.admit_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.primary_key.as_slice()),
        )? {
            return Ok(Response::new(kvpb::KvTxnHeartBeatResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdTxnHeartBeat as i32,
                    cmd: Some(raftpb::request::Cmd::TxnHeartBeat(inner.clone())),
                },
                "txn heart beat",
            )
            .await?
        {
            raftpb::response::Cmd::TxnHeartBeat(response) => response,
            _ => {
                return Err(raft_payload_error(
                    "txn heart beat",
                    "unexpected txn heart beat payload",
                ))
            }
        };
        Ok(Response::new(kvpb::KvTxnHeartBeatResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn try_atomic_mutate(
        &self,
        request: Request<kvpb::KvTryAtomicMutateRequest>,
    ) -> Result<Response<kvpb::KvTryAtomicMutateResponse>, Status> {
        let request = request.into_inner();
        let inner = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("atomic mutate request missing payload"))?;
        let keys = inner
            .predicates
            .iter()
            .map(|predicate| predicate.key.as_slice())
            .chain(
                inner
                    .mutations
                    .iter()
                    .map(|mutation| mutation.key.as_slice()),
            );
        if let Some(region_error) = self
            .admission
            .admit_optional_keys(request.context.as_ref(), keys)?
        {
            return Ok(Response::new(kvpb::KvTryAtomicMutateResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                    cmd: Some(raftpb::request::Cmd::TryAtomicMutate(inner.clone())),
                },
                "atomic mutate",
            )
            .await?
        {
            raftpb::response::Cmd::TryAtomicMutate(response) => response,
            _ => {
                return Err(raft_payload_error(
                    "atomic mutate",
                    "unexpected atomic mutate payload",
                ))
            }
        };
        Ok(Response::new(kvpb::KvTryAtomicMutateResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn install_prepared_mvcc_entries(
        &self,
        request: Request<kvpb::KvInstallPreparedMvccEntriesRequest>,
    ) -> Result<Response<kvpb::KvInstallPreparedMvccEntriesResponse>, Status> {
        let request = request.into_inner();
        let inner = request.request.as_ref().ok_or_else(|| {
            Status::invalid_argument("install prepared mvcc request missing payload")
        })?;
        let keys = std::iter::once(inner.routing_key.as_slice())
            .chain(inner.dependency_keys.iter().map(|key| key.as_slice()))
            .chain(inner.entries.iter().map(|entry| entry.key.as_slice()));
        if let Some(region_error) = self
            .admission
            .admit_required_keys(request.context.as_ref(), keys)?
        {
            return Ok(Response::new(kvpb::KvInstallPreparedMvccEntriesResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let response = match self
            .execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdInstallPreparedMvcc as i32,
                    cmd: Some(raftpb::request::Cmd::InstallPreparedMvcc(inner.clone())),
                },
                "install prepared mvcc",
            )
            .await?
        {
            raftpb::response::Cmd::InstallPreparedMvcc(response) => response,
            _ => {
                return Err(raft_payload_error(
                    "install prepared mvcc",
                    "unexpected install prepared mvcc payload",
                ))
            }
        };
        Ok(Response::new(kvpb::KvInstallPreparedMvccEntriesResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    type WatchApplyStream = ReceiverStream<Result<kvpb::ApplyWatchResponse, Status>>;

    async fn watch_apply(
        &self,
        request: Request<kvpb::ApplyWatchRequest>,
    ) -> Result<Response<Self::WatchApplyStream>, Status> {
        let request = request.into_inner();
        let prefix = request.key_prefix;
        let buffer = request.buffer.max(1) as usize;
        let mut watch = self.engine.subscribe_apply();
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        tokio::spawn(async move {
            loop {
                match watch.recv().await {
                    Ok(event) => {
                        if !event.keys.iter().any(|key| key.starts_with(&prefix)) {
                            continue;
                        }
                        let response = kvpb::ApplyWatchResponse {
                            event: Some(event),
                            dropped_events: 0,
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                        let response = kvpb::ApplyWatchResponse {
                            event: None,
                            dropped_events: dropped,
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Debug, Clone, Default)]
pub struct RaftAdminService<S = EmptyApplyStatus> {
    status: S,
    topology: Arc<Mutex<AdminTopology>>,
}

impl<S> RaftAdminService<S> {
    pub fn new(status: S) -> Self {
        Self::with_admission(status, RegionAdmission::default())
    }

    pub fn with_admission(status: S, admission: RegionAdmission) -> Self {
        Self {
            status,
            topology: Arc::new(Mutex::new(AdminTopology::from_admission(&admission))),
        }
    }
}

#[derive(Debug, Clone)]
struct AdminTopology {
    region_id: u64,
    epoch_version: u64,
    conf_version: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    peers: BTreeMap<u64, u64>,
}

impl Default for AdminTopology {
    fn default() -> Self {
        Self::from_admission(&RegionAdmission::default())
    }
}

impl AdminTopology {
    fn from_admission(admission: &RegionAdmission) -> Self {
        Self {
            region_id: admission.region_id,
            epoch_version: admission.epoch_version,
            conf_version: admission.epoch_conf_version,
            start_key: admission.start_key.clone(),
            end_key: admission.end_key.clone(),
            peers: BTreeMap::from([(admission.peer_id, admission.store_id)]),
        }
    }

    fn add_peer(&mut self, peer_id: u64, store_id: u64) {
        if self.peers.insert(peer_id, store_id).is_none() {
            self.conf_version += 1;
        }
    }

    fn remove_peer(&mut self, peer_id: u64) {
        if self.peers.remove(&peer_id).is_some() {
            self.conf_version += 1;
        }
    }

    fn validate_region(&self, region_id: u64) -> Result<(), Status> {
        if region_id == 0 {
            return Err(Status::invalid_argument("region_id is required"));
        }
        if region_id != self.region_id {
            return Err(Status::failed_precondition(format!(
                "region {region_id} is not hosted by this raft admin"
            )));
        }
        Ok(())
    }

    fn descriptor(&self) -> metapb::RegionDescriptor {
        metapb::RegionDescriptor {
            region_id: self.region_id,
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
            epoch: Some(metapb::RegionEpoch {
                version: self.epoch_version,
                conf_version: self.conf_version,
            }),
            peers: self
                .peers
                .iter()
                .map(|(peer_id, store_id)| metapb::RegionPeer {
                    store_id: *store_id,
                    peer_id: *peer_id,
                })
                .collect(),
            ..Default::default()
        }
    }
}

fn membership_unimplemented() -> Status {
    Status::unimplemented("rust raft membership requires an OpenRaftRegion")
}

fn admin_topology_poisoned() -> Status {
    Status::internal("admin topology mutex poisoned")
}

fn admin_region_descriptor(
    topology: &Arc<Mutex<AdminTopology>>,
) -> Result<metapb::RegionDescriptor, Status> {
    topology
        .lock()
        .map_err(|_| admin_topology_poisoned())
        .map(|topology| topology.descriptor())
}

fn validate_admin_region(
    topology: &Arc<Mutex<AdminTopology>>,
    region_id: u64,
) -> Result<(), Status> {
    topology
        .lock()
        .map_err(|_| admin_topology_poisoned())?
        .validate_region(region_id)
}

#[tonic::async_trait]
pub trait RaftMembershipAdmin: Clone + Send + Sync + 'static {
    async fn add_voter(&self, peer_id: u64, node: BasicNode) -> Result<(), Status>;
    async fn remove_voter(&self, peer_id: u64) -> Result<(), Status>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RaftRuntimeStatus {
    pub local_peer_id: u64,
    pub leader_peer_id: u64,
    pub leader: bool,
}

pub trait RaftRuntimeStatusProvider: ApplyStatusProvider {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus;
}

#[tonic::async_trait]
impl<E> RaftMembershipAdmin for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionApplyEngine,
{
    async fn add_voter(&self, peer_id: u64, node: BasicNode) -> Result<(), Status> {
        self.add_voter(peer_id, node)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }

    async fn remove_voter(&self, peer_id: u64) -> Result<(), Status> {
        self.remove_voter(peer_id, false)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }
}

impl<E> RaftRuntimeStatusProvider for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionApplyEngine,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let local_peer_id = self.node_id();
        let leader_peer_id = self
            .raft_handle()
            .metrics()
            .borrow()
            .current_leader
            .unwrap_or_default();
        RaftRuntimeStatus {
            local_peer_id,
            leader_peer_id,
            leader: leader_peer_id == local_peer_id && leader_peer_id != 0,
        }
    }
}

#[tonic::async_trait]
impl<E> RaftMembershipAdmin for AppliedKvEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl<E> RaftRuntimeStatusProvider for AppliedKvEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let known = self.apply_status().region_id != 0;
        RaftRuntimeStatus {
            local_peer_id: u64::from(known),
            leader_peer_id: u64::from(known),
            leader: known,
        }
    }
}

#[tonic::async_trait]
impl<E, S> RaftMembershipAdmin for PersistentAppliedKvEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: ApplyStatusSink,
{
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl<E, S> RaftRuntimeStatusProvider for PersistentAppliedKvEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: ApplyStatusSink,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let known = self.apply_status().region_id != 0;
        RaftRuntimeStatus {
            local_peer_id: u64::from(known),
            leader_peer_id: u64::from(known),
            leader: known,
        }
    }
}

#[tonic::async_trait]
impl RaftMembershipAdmin for EmptyApplyStatus {
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl RaftRuntimeStatusProvider for EmptyApplyStatus {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        RaftRuntimeStatus::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct EmptyApplyStatus;

impl ApplyStatusProvider for EmptyApplyStatus {
    fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
        nokv_raftnode::ApplyStatus {
            region_id: 0,
            term: 0,
            applied_index: 0,
        }
    }
}

#[tonic::async_trait]
impl<S> adminpb::raft_admin_server::RaftAdmin for RaftAdminService<S>
where
    S: RaftRuntimeStatusProvider + RaftMembershipAdmin,
{
    async fn add_peer(
        &self,
        request: Request<adminpb::AddPeerRequest>,
    ) -> Result<Response<adminpb::AddPeerResponse>, Status> {
        let request = request.into_inner();
        if request.store_id == 0 || request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id, store_id, and peer_id are required",
            ));
        }
        validate_admin_region(&self.topology, request.region_id)?;
        self.status
            .add_voter(
                request.peer_id,
                BasicNode::new(format!(
                    "store-{}-peer-{}",
                    request.store_id, request.peer_id
                )),
            )
            .await?;
        let region = {
            let mut topology = self
                .topology
                .lock()
                .map_err(|_| admin_topology_poisoned())?;
            topology.add_peer(request.peer_id, request.store_id);
            topology.descriptor()
        };
        Ok(Response::new(adminpb::AddPeerResponse {
            region: Some(region),
        }))
    }

    async fn remove_peer(
        &self,
        request: Request<adminpb::RemovePeerRequest>,
    ) -> Result<Response<adminpb::RemovePeerResponse>, Status> {
        let request = request.into_inner();
        if request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id and peer_id are required",
            ));
        }
        validate_admin_region(&self.topology, request.region_id)?;
        self.status.remove_voter(request.peer_id).await?;
        let region = {
            let mut topology = self
                .topology
                .lock()
                .map_err(|_| admin_topology_poisoned())?;
            topology.remove_peer(request.peer_id);
            topology.descriptor()
        };
        Ok(Response::new(adminpb::RemovePeerResponse {
            region: Some(region),
        }))
    }

    async fn transfer_leader(
        &self,
        _request: Request<adminpb::TransferLeaderRequest>,
    ) -> Result<Response<adminpb::TransferLeaderResponse>, Status> {
        Err(Status::unimplemented(
            "rust raft leadership is not wired yet",
        ))
    }

    async fn region_runtime_status(
        &self,
        request: Request<adminpb::RegionRuntimeStatusRequest>,
    ) -> Result<Response<adminpb::RegionRuntimeStatusResponse>, Status> {
        let request = request.into_inner();
        let status = self.status.apply_status();
        if request.region_id != 0 && request.region_id != status.region_id {
            return Ok(Response::new(
                adminpb::RegionRuntimeStatusResponse::default(),
            ));
        }
        let region = if status.region_id == 0 {
            None
        } else {
            Some(admin_region_descriptor(&self.topology)?)
        };
        let runtime = self.status.raft_runtime_status();
        Ok(Response::new(adminpb::RegionRuntimeStatusResponse {
            known: status.region_id != 0,
            hosted: status.region_id != 0,
            local_peer_id: runtime.local_peer_id,
            leader_peer_id: runtime.leader_peer_id,
            leader: runtime.leader,
            region,
            applied_index: status.applied_index,
            applied_term: status.term,
        }))
    }

    async fn execution_status(
        &self,
        _request: Request<adminpb::ExecutionStatusRequest>,
    ) -> Result<Response<adminpb::ExecutionStatusResponse>, Status> {
        let status = self.status.apply_status();
        Ok(Response::new(adminpb::ExecutionStatusResponse {
            restart: Some(adminpb::ExecutionRestartStatus {
                state: if status.region_id == 0 {
                    adminpb::ExecutionRestartState::Degraded as i32
                } else {
                    adminpb::ExecutionRestartState::Ready as i32
                },
                region_count: if status.region_id == 0 { 0 } else { 1 },
                raft_group_count: if status.region_id == 0 { 0 } else { 1 },
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

pub async fn serve(addr: SocketAddr, mvcc: MvccStore) -> Result<(), tonic::transport::Error> {
    serve_with_engine(addr, mvcc).await
}

pub async fn serve_with_engine<E>(
    addr: SocketAddr,
    engine: E,
) -> Result<(), tonic::transport::Error>
where
    E: KvEngine,
{
    serve_with_region_engine(addr, nokv_raftnode::AppliedKvEngine::new(1, engine)).await
}

pub async fn serve_with_region_engine<E>(
    addr: SocketAddr,
    engine: E,
) -> Result<(), tonic::transport::Error>
where
    E: RaftCommandExecutor
        + ApplyStatusProvider
        + ApplyWatchProvider
        + RaftMembershipAdmin
        + RaftRuntimeStatusProvider,
{
    serve_with_region_engine_and_admission(addr, engine, RegionAdmission::default()).await
}

pub async fn serve_with_region_engine_and_admission<E>(
    addr: SocketAddr,
    engine: E,
    admission: RegionAdmission,
) -> Result<(), tonic::transport::Error>
where
    E: RaftCommandExecutor
        + ApplyStatusProvider
        + ApplyWatchProvider
        + RaftMembershipAdmin
        + RaftRuntimeStatusProvider,
{
    tonic::transport::Server::builder()
        .add_service(StoreKvServer::new(StoreKvService::with_admission(
            engine.clone(),
            admission.clone(),
        )))
        .add_service(RaftAdminServer::new(RaftAdminService::with_admission(
            engine, admission,
        )))
        .serve(addr)
        .await
}

fn internal_error(err: nokv_mvcc::Error) -> Status {
    Status::internal(err.to_string())
}

fn header_from_context(context: &kvpb::Context) -> raftpb::CmdHeader {
    let peer = context.peer.as_ref();
    raftpb::CmdHeader {
        region_id: context.region_id,
        region_epoch: context.region_epoch.clone(),
        peer_id: peer.map(|peer| peer.peer_id).unwrap_or_default(),
        read_consistency: context.read_consistency,
        read_preference: context.read_preference,
        max_stale_read_index: context.max_stale_read_index,
        max_stale_read_ms: context.max_stale_read_ms,
        store_id: peer.map(|peer| peer.store_id).unwrap_or_default(),
        ..Default::default()
    }
}

fn raft_payload_error(operation: &str, detail: &str) -> Status {
    Status::internal(format!("{operation} raft payload error: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use adminpb::raft_admin_server::RaftAdmin;
    use kvpb::store_kv_server::StoreKv;
    use nokv_proto::nokv::meta::v1 as metapb;
    use tokio_stream::StreamExt;

    fn context(admission: &RegionAdmission) -> kvpb::Context {
        kvpb::Context {
            region_id: admission.region_id,
            region_epoch: Some(metapb::RegionEpoch {
                version: admission.epoch_version,
                conf_version: admission.epoch_conf_version,
            }),
            peer: Some(metapb::RegionPeer {
                store_id: admission.store_id,
                peer_id: admission.peer_id,
            }),
            ..Default::default()
        }
    }

    fn default_context() -> kvpb::Context {
        context(&RegionAdmission::default())
    }

    #[tokio::test]
    async fn get_returns_not_found_from_empty_store() {
        let service = StoreKvService::new(nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()));
        let response = service
            .get(Request::new(kvpb::KvGetRequest {
                context: Some(default_context()),
                request: Some(kvpb::GetRequest {
                    key: b"missing".to_vec(),
                    version: 1,
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.response.unwrap().not_found);
    }

    #[tokio::test]
    async fn service_can_run_against_holt_mvcc_engine() {
        let engine = nokv_raftnode::AppliedKvEngine::new(
            1,
            nokv_holtstore::HoltMvccStore::open_memory().unwrap(),
        );
        let service = StoreKvService::new(engine.clone());
        let response = service
            .try_atomic_mutate(Request::new(kvpb::KvTryAtomicMutateRequest {
                context: Some(default_context()),
                request: Some(kvpb::TryAtomicMutateRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    commit_version: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.response.unwrap().applied_keys, 1);
        assert_eq!(engine.status().applied_index, 1);
    }

    #[tokio::test]
    async fn service_can_run_against_openraft_region() {
        let dir = tempfile::tempdir().unwrap();
        let log = nokv_raftnode::SegmentedEntryLog::open(1, dir.path()).unwrap();
        let state_machine = nokv_raftnode::RegionStateMachine::new(
            nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()),
        );
        let region = nokv_raftnode::OpenRaftRegion::bootstrap_single_node(
            1,
            1,
            nokv_raftnode::RegionLogStorage::new(log),
            state_machine,
        )
        .await
        .unwrap();
        let service = StoreKvService::new(region.clone());

        let response = service
            .try_atomic_mutate(Request::new(kvpb::KvTryAtomicMutateRequest {
                context: Some(default_context()),
                request: Some(kvpb::TryAtomicMutateRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    commit_version: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.response.unwrap().applied_keys, 1);
        assert_eq!(region.apply_status().applied_index, 2);
    }

    #[tokio::test]
    async fn transaction_rpcs_round_trip_through_service() {
        let service = StoreKvService::new(nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()));
        let context = default_context();

        let prewrite = service
            .prewrite(Request::new(kvpb::KvPrewriteRequest {
                context: Some(context.clone()),
                request: Some(kvpb::PrewriteRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"txn/a".to_vec(),
                        value: b"va".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    primary_lock: b"txn/a".to_vec(),
                    start_version: 10,
                    lock_ttl: 10,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert!(prewrite.errors.is_empty());

        let heartbeat = service
            .txn_heart_beat(Request::new(kvpb::KvTxnHeartBeatRequest {
                context: Some(context.clone()),
                request: Some(kvpb::TxnHeartBeatRequest {
                    primary_key: b"txn/a".to_vec(),
                    start_version: 10,
                    ttl_extension: 100,
                    current_time: 1,
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert!(heartbeat.error.is_none());
        assert!(heartbeat.lock_ttl >= 100);

        let status = service
            .check_txn_status(Request::new(kvpb::KvCheckTxnStatusRequest {
                context: Some(context.clone()),
                request: Some(kvpb::CheckTxnStatusRequest {
                    primary_key: b"txn/a".to_vec(),
                    lock_ts: 10,
                    current_ts: 11,
                    caller_start_ts: 11,
                    current_time: 1,
                    rollback_if_not_exist: true,
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert!(status.error.is_none());
        assert_eq!(
            status.action,
            kvpb::CheckTxnStatusAction::CheckTxnStatusMinCommitTsPushed as i32
        );

        let commit = service
            .commit(Request::new(kvpb::KvCommitRequest {
                context: Some(context.clone()),
                request: Some(kvpb::CommitRequest {
                    keys: vec![b"txn/a".to_vec()],
                    start_version: 10,
                    commit_version: 20,
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert!(commit.error.is_none());

        let batch_get = service
            .batch_get(Request::new(kvpb::KvBatchGetRequest {
                context: Some(context.clone()),
                request: Some(kvpb::BatchGetRequest {
                    requests: vec![
                        kvpb::GetRequest {
                            key: b"txn/a".to_vec(),
                            version: 20,
                        },
                        kvpb::GetRequest {
                            key: b"txn/missing".to_vec(),
                            version: 20,
                        },
                    ],
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert_eq!(batch_get.responses[0].value, b"va".to_vec());
        assert!(batch_get.responses[1].not_found);

        let scan = service
            .scan(Request::new(kvpb::KvScanRequest {
                context: Some(context.clone()),
                request: Some(kvpb::ScanRequest {
                    start_key: b"txn/".to_vec(),
                    limit: 10,
                    version: 20,
                    include_start: true,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert_eq!(scan.kvs.len(), 1);
        assert_eq!(scan.kvs[0].key, b"txn/a".to_vec());

        service
            .prewrite(Request::new(kvpb::KvPrewriteRequest {
                context: Some(context.clone()),
                request: Some(kvpb::PrewriteRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"txn/rollback".to_vec(),
                        value: b"discard".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    primary_lock: b"txn/rollback".to_vec(),
                    start_version: 30,
                    lock_ttl: 10,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        let rollback = service
            .batch_rollback(Request::new(kvpb::KvBatchRollbackRequest {
                context: Some(context.clone()),
                request: Some(kvpb::BatchRollbackRequest {
                    keys: vec![b"txn/rollback".to_vec()],
                    start_version: 30,
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert!(rollback.error.is_none());

        service
            .prewrite(Request::new(kvpb::KvPrewriteRequest {
                context: Some(context.clone()),
                request: Some(kvpb::PrewriteRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"txn/resolve".to_vec(),
                        value: b"resolved".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    primary_lock: b"txn/resolve".to_vec(),
                    start_version: 40,
                    lock_ttl: 10,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        let resolved = service
            .resolve_lock(Request::new(kvpb::KvResolveLockRequest {
                context: Some(context.clone()),
                request: Some(kvpb::ResolveLockRequest {
                    start_version: 40,
                    commit_version: 50,
                    keys: vec![b"txn/resolve".to_vec()],
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert_eq!(resolved.resolved_locks, 1);

        let install = service
            .install_prepared_mvcc_entries(Request::new(
                kvpb::KvInstallPreparedMvccEntriesRequest {
                    context: Some(context.clone()),
                    request: Some(kvpb::InstallPreparedMvccEntriesRequest {
                        routing_key: b"txn/prepared".to_vec(),
                        commit_version: 60,
                        entries: vec![kvpb::PreparedMvccEntry {
                            column_family: kvpb::prepared_mvcc_entry::ColumnFamily::Default as i32,
                            key: b"txn/prepared".to_vec(),
                            version: 60,
                            value: b"prepared".to_vec(),
                            has_value: true,
                            ..Default::default()
                        }],
                        watch_keys: vec![b"txn/prepared".to_vec()],
                        ..Default::default()
                    }),
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert_eq!(install.applied_entries, 1);
        assert_eq!(install.commit_version, 60);

        let prepared = service
            .get(Request::new(kvpb::KvGetRequest {
                context: Some(context),
                request: Some(kvpb::GetRequest {
                    key: b"txn/prepared".to_vec(),
                    version: 60,
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();
        assert_eq!(prepared.value, b"prepared".to_vec());
    }

    #[tokio::test]
    async fn watch_apply_streams_matching_apply_events() {
        let engine = nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new());
        let service = StoreKvService::new(engine.clone());
        let mut stream = service
            .watch_apply(Request::new(kvpb::ApplyWatchRequest {
                key_prefix: b"prefix/".to_vec(),
                buffer: 4,
            }))
            .await
            .unwrap()
            .into_inner();

        service
            .try_atomic_mutate(Request::new(kvpb::KvTryAtomicMutateRequest {
                context: Some(default_context()),
                request: Some(kvpb::TryAtomicMutateRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"prefix/k".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    commit_version: 9,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap();

        let response = stream.next().await.unwrap().unwrap();
        let event = response.event.unwrap();
        assert_eq!(event.commit_version, 9);
        assert_eq!(event.keys, vec![b"prefix/k".to_vec()]);
    }

    #[tokio::test]
    async fn get_requires_context() {
        let service = StoreKvService::new(nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()));
        let err = service
            .get(Request::new(kvpb::KvGetRequest {
                request: Some(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 1,
                }),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_rejects_region_not_found() {
        let service = StoreKvService::new(nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()));
        let mut ctx = default_context();
        ctx.region_id = 99;
        let response = service
            .get(Request::new(kvpb::KvGetRequest {
                context: Some(ctx),
                request: Some(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 1,
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        let region_error = response.region_error.unwrap();
        assert!(region_error.region_not_found.is_some());
        assert!(response.response.is_none());
    }

    #[tokio::test]
    async fn get_rejects_store_not_match() {
        let service = StoreKvService::new(nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()));
        let mut ctx = default_context();
        ctx.peer.as_mut().unwrap().store_id = 999;
        let response = service
            .get(Request::new(kvpb::KvGetRequest {
                context: Some(ctx),
                request: Some(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 1,
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        let mismatch = response.region_error.unwrap().store_not_match.unwrap();
        assert_eq!(mismatch.request_store_id, 999);
        assert_eq!(mismatch.actual_store_id, 1);
        assert!(response.response.is_none());
    }

    #[tokio::test]
    async fn get_rejects_epoch_not_match() {
        let service = StoreKvService::new(nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()));
        let mut ctx = default_context();
        ctx.region_epoch.as_mut().unwrap().version = 99;
        let response = service
            .get(Request::new(kvpb::KvGetRequest {
                context: Some(ctx),
                request: Some(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 1,
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        let mismatch = response.region_error.unwrap().epoch_not_match.unwrap();
        assert_eq!(mismatch.current_epoch.unwrap().version, 1);
        assert_eq!(mismatch.regions.len(), 1);
        assert!(response.response.is_none());
    }

    #[tokio::test]
    async fn get_rejects_key_not_in_region() {
        let admission = RegionAdmission {
            region_id: 10,
            store_id: 7,
            peer_id: 77,
            epoch_version: 3,
            epoch_conf_version: 2,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            leader: true,
        };
        let service = StoreKvService::with_admission(
            nokv_raftnode::AppliedKvEngine::new(10, MvccStore::new()),
            admission.clone(),
        );
        let response = service
            .get(Request::new(kvpb::KvGetRequest {
                context: Some(context(&admission)),
                request: Some(kvpb::GetRequest {
                    key: b"z".to_vec(),
                    version: 1,
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        let out = response.region_error.unwrap().key_not_in_region.unwrap();
        assert_eq!(out.key, b"z".to_vec());
        assert_eq!(out.region_id, 10);
        assert_eq!(out.start_key, b"a".to_vec());
        assert_eq!(out.end_key, b"m".to_vec());
        assert!(response.response.is_none());
    }

    #[tokio::test]
    async fn scan_rejects_not_leader() {
        let admission = RegionAdmission {
            leader: false,
            ..Default::default()
        };
        let service = StoreKvService::with_admission(
            nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()),
            admission.clone(),
        );
        let response = service
            .scan(Request::new(kvpb::KvScanRequest {
                context: Some(context(&admission)),
                request: Some(kvpb::ScanRequest {
                    start_key: b"k".to_vec(),
                    limit: 1,
                    version: 1,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.region_error.unwrap().not_leader.is_some());
        assert!(response.response.is_none());
    }

    #[tokio::test]
    async fn admin_membership_is_explicitly_not_wired() {
        let service = RaftAdminService::new(EmptyApplyStatus);
        let err = service
            .add_peer(Request::new(adminpb::AddPeerRequest {
                region_id: 1,
                store_id: 1,
                peer_id: 1,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn admin_adds_and_removes_openraft_voter() {
        let registry = nokv_raftnode::MemoryRaftNetworkRegistry::default();
        let mut dirs = Vec::new();
        let mut regions = BTreeMap::new();

        for node_id in 1..=2 {
            let dir = tempfile::tempdir().unwrap();
            let log = nokv_raftnode::SegmentedEntryLog::open(7, dir.path()).unwrap();
            let log_store = nokv_raftnode::RegionLogStorage::new(log);
            let state_machine = nokv_raftnode::RegionStateMachine::new(
                nokv_raftnode::AppliedKvEngine::new(7, MvccStore::new()),
            );
            let region = nokv_raftnode::OpenRaftRegion::open_with_network(
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
            regions.insert(node_id, region);
        }

        let leader = regions.get(&1).unwrap();
        leader
            .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        leader.wait_for_leader(1).await.unwrap();

        let admission = RegionAdmission {
            region_id: 7,
            store_id: 1,
            peer_id: 1,
            epoch_conf_version: 1,
            ..Default::default()
        };
        let service = RaftAdminService::with_admission(leader.clone(), admission);

        let add = service
            .add_peer(Request::new(adminpb::AddPeerRequest {
                region_id: 7,
                store_id: 2,
                peer_id: 2,
            }))
            .await
            .unwrap()
            .into_inner()
            .region
            .unwrap();
        assert_eq!(add.region_id, 7);
        assert_eq!(add.epoch.unwrap().conf_version, 2);
        assert_eq!(
            add.peers,
            vec![
                metapb::RegionPeer {
                    store_id: 1,
                    peer_id: 1
                },
                metapb::RegionPeer {
                    store_id: 2,
                    peer_id: 2
                },
            ]
        );

        let leader_status = service
            .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
                region_id: 7,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(leader_status.local_peer_id, 1);
        assert_eq!(leader_status.leader_peer_id, 1);
        assert!(leader_status.leader);

        let follower_service = RaftAdminService::with_admission(
            regions.get(&2).unwrap().clone(),
            RegionAdmission {
                region_id: 7,
                store_id: 2,
                peer_id: 2,
                epoch_conf_version: 2,
                ..Default::default()
            },
        );
        let follower_status = follower_service
            .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
                region_id: 7,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(follower_status.local_peer_id, 2);
        assert_eq!(follower_status.leader_peer_id, 1);
        assert!(!follower_status.leader);

        let remove = service
            .remove_peer(Request::new(adminpb::RemovePeerRequest {
                region_id: 7,
                peer_id: 2,
            }))
            .await
            .unwrap()
            .into_inner()
            .region
            .unwrap();
        assert_eq!(remove.epoch.unwrap().conf_version, 3);
        assert_eq!(
            remove.peers,
            vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 1
            }]
        );
    }

    #[tokio::test]
    async fn admin_runtime_status_reports_apply_index() {
        let engine = nokv_raftnode::AppliedKvEngine::new(11, MvccStore::new());
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
        let service = RaftAdminService::new(engine);
        let response = service
            .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
                region_id: 11,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.known);
        assert_eq!(response.applied_index, 1);
        assert_eq!(response.applied_term, 1);
    }
}
