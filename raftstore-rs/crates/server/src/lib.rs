//! Tonic services for the Rust raftstore data plane.
//!
//! This crate owns the external gRPC boundary. It keeps the existing NoKV
//! protobuf contract intact while the Rust state-machine and replication layers
//! are brought up behind the service.

use std::net::SocketAddr;

use nokv_mvcc::{KvEngine, MvccStore};
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::raft::v1 as raftpb;
use nokv_raftnode::{ApplyStatusProvider, ApplyWatchProvider, RaftCommandExecutor};
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
}

impl<S> RaftAdminService<S> {
    pub fn new(status: S) -> Self {
        Self { status }
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
    S: ApplyStatusProvider,
{
    async fn add_peer(
        &self,
        _request: Request<adminpb::AddPeerRequest>,
    ) -> Result<Response<adminpb::AddPeerResponse>, Status> {
        Err(Status::unimplemented(
            "rust raft membership is not wired yet",
        ))
    }

    async fn remove_peer(
        &self,
        _request: Request<adminpb::RemovePeerRequest>,
    ) -> Result<Response<adminpb::RemovePeerResponse>, Status> {
        Err(Status::unimplemented(
            "rust raft membership is not wired yet",
        ))
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
        Ok(Response::new(adminpb::RegionRuntimeStatusResponse {
            known: status.region_id != 0,
            hosted: status.region_id != 0,
            local_peer_id: 1,
            leader_peer_id: 1,
            leader: status.region_id != 0,
            region: None,
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
    E: RaftCommandExecutor + ApplyStatusProvider + ApplyWatchProvider,
{
    serve_with_region_engine_and_admission(addr, engine, RegionAdmission::default()).await
}

pub async fn serve_with_region_engine_and_admission<E>(
    addr: SocketAddr,
    engine: E,
    admission: RegionAdmission,
) -> Result<(), tonic::transport::Error>
where
    E: RaftCommandExecutor + ApplyStatusProvider + ApplyWatchProvider,
{
    tonic::transport::Server::builder()
        .add_service(StoreKvServer::new(StoreKvService::with_admission(
            engine.clone(),
            admission,
        )))
        .add_service(RaftAdminServer::new(RaftAdminService::new(engine)))
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
