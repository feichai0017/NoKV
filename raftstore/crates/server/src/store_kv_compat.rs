use std::time::{SystemTime, UNIX_EPOCH};

use nokv_mvcc::MvccStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::raft::v1 as raftpb;
use nokv_raftnode::{ApplyWatchProvider, RaftCommandExecutor};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::admission::RegionAdmission;
use crate::admission_state::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
use crate::wire_helpers::{
    chunk_apply_watch_keys, header_from_context, matching_apply_watch_keys, raft_payload_error,
    trim_scan_response_to_region,
};
use crate::{
    internal_error, AppliedRegionDescriptorProvider, RaftRuntimeStatusProvider,
    DEFAULT_APPLY_WATCH_BUFFER,
};

fn service_physical_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn check_txn_status_request_with_service_time(
    req: &kvpb::CheckTxnStatusRequest,
) -> kvpb::CheckTxnStatusRequest {
    let mut out = req.clone();
    if out.current_time == 0 {
        out.current_time = service_physical_time_millis();
    }
    out
}

fn txn_heart_beat_request_with_service_time(
    req: &kvpb::TxnHeartBeatRequest,
) -> kvpb::TxnHeartBeatRequest {
    let mut out = req.clone();
    if out.current_time == 0 {
        out.current_time = service_physical_time_millis();
    }
    out
}

#[derive(Clone)]
pub struct StoreKvService<E = MvccStore> {
    engine: E,
    admission: RegionAdmissionState,
    execution: ExecutionRuntime,
}

impl<E> StoreKvService<E> {
    pub fn new(engine: E) -> Self {
        Self::with_admission(engine, RegionAdmission::default())
    }

    pub fn with_admission(engine: E, admission: RegionAdmission) -> Self {
        Self::with_admission_and_execution(engine, admission, ExecutionRuntime::default())
    }

    pub(crate) fn with_admission_and_execution(
        engine: E,
        admission: RegionAdmission,
        execution: ExecutionRuntime,
    ) -> Self {
        Self::with_admission_state_and_execution(
            engine,
            RegionAdmissionState::new(admission),
            execution,
        )
    }

    pub(crate) fn with_admission_state_and_execution(
        engine: E,
        admission: RegionAdmissionState,
        execution: ExecutionRuntime,
    ) -> Self {
        Self {
            engine,
            admission,
            execution,
        }
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
    ) -> Result<Result<raftpb::response::Cmd, errorpb::RegionError>, Status> {
        let context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
        let response = self
            .engine
            .execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(header_from_context(context)),
                requests: vec![request],
            })
            .await
            .map_err(internal_error)?;
        if let Some(region_error) = response.region_error {
            return Ok(Err(region_error));
        }
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
            .map(Ok)
    }
}

macro_rules! raft_cmd_or_region_error {
    ($outcome:expr, $response_type:ident) => {
        match $outcome {
            Ok(cmd) => cmd,
            Err(region_error) => {
                return Ok(Response::new(kvpb::$response_type {
                    response: None,
                    region_error: Some(region_error),
                }))
            }
        }
    };
}

impl<E> StoreKvService<E>
where
    E: AppliedRegionDescriptorProvider + RaftRuntimeStatusProvider,
{
    fn admission_snapshot(&self) -> Result<RegionAdmission, Status> {
        let runtime = self.engine.raft_runtime_status();
        self.admission.with_applied_descriptor_and_runtime_status(
            self.engine.applied_region_descriptor()?,
            runtime,
        )
    }

    fn record_admission(
        &self,
        class: adminpb::ExecutionAdmissionClass,
        context: Option<&kvpb::Context>,
        region_error: Option<&errorpb::RegionError>,
    ) {
        self.execution
            .record_admission(class, context, region_error);
    }
}

#[tonic::async_trait]
impl<E> kvpb::store_kv_server::StoreKv for StoreKvService<E>
where
    E: AppliedRegionDescriptorProvider
        + ApplyWatchProvider
        + RaftCommandExecutor
        + RaftRuntimeStatusProvider,
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
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_read_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Read,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvGetResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdGet as i32,
                    cmd: Some(raftpb::request::Cmd::Get(inner.clone())),
                },
                "get",
            )
            .await?,
            KvGetResponse
        );
        let response = match cmd {
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
        if inner.requests.is_empty() {
            let context = request
                .context
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("context is required"))?;
            if context.region_id == 0 {
                return Err(Status::invalid_argument("region id is required"));
            }
            return Ok(Response::new(kvpb::KvBatchGetResponse {
                response: Some(kvpb::BatchGetResponse {
                    responses: Vec::new(),
                }),
                region_error: None,
            }));
        }
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_read_optional_keys(
            request.context.as_ref(),
            inner.requests.iter().map(|req| req.key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Read,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
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
        if let Some(region_error) = command_response.region_error {
            return Ok(Response::new(kvpb::KvBatchGetResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
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
        if inner.reverse {
            self.execution.record_invalid(
                adminpb::ExecutionAdmissionClass::Read,
                request.context.as_ref(),
                "reverse scan unsupported",
            );
            return Err(Status::unimplemented(
                "StoreKV Scan reverse scans are not supported yet",
            ));
        }
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_read_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.start_key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Read,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvScanResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdScan as i32,
                    cmd: Some(raftpb::request::Cmd::Scan(inner.clone())),
                },
                "scan",
            )
            .await?,
            KvScanResponse
        );
        let mut response = match cmd {
            raftpb::response::Cmd::Scan(response) => response,
            _ => return Err(raft_payload_error("scan", "unexpected scan payload")),
        };
        trim_scan_response_to_region(&admission, &mut response);
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
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_optional_keys(
            request.context.as_ref(),
            inner
                .mutations
                .iter()
                .map(|mutation| mutation.key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvPrewriteResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdPrewrite as i32,
                    cmd: Some(raftpb::request::Cmd::Prewrite(inner.clone())),
                },
                "prewrite",
            )
            .await?,
            KvPrewriteResponse
        );
        let response = match cmd {
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
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_optional_keys(
            request.context.as_ref(),
            inner.keys.iter().map(|key| key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvCommitResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdCommit as i32,
                    cmd: Some(raftpb::request::Cmd::Commit(inner.clone())),
                },
                "commit",
            )
            .await?,
            KvCommitResponse
        );
        let response = match cmd {
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
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_optional_keys(
            request.context.as_ref(),
            inner.keys.iter().map(|key| key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvBatchRollbackResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdBatchRollback as i32,
                    cmd: Some(raftpb::request::Cmd::BatchRollback(inner.clone())),
                },
                "batch rollback",
            )
            .await?,
            KvBatchRollbackResponse
        );
        let response = match cmd {
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
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_optional_keys(
            request.context.as_ref(),
            inner.keys.iter().map(|key| key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvResolveLockResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdResolveLock as i32,
                    cmd: Some(raftpb::request::Cmd::ResolveLock(inner.clone())),
                },
                "resolve lock",
            )
            .await?,
            KvResolveLockResponse
        );
        let response = match cmd {
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
        let inner = check_txn_status_request_with_service_time(inner);
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.primary_key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvCheckTxnStatusResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdCheckTxnStatus as i32,
                    cmd: Some(raftpb::request::Cmd::CheckTxnStatus(inner)),
                },
                "check txn status",
            )
            .await?,
            KvCheckTxnStatusResponse
        );
        let response = match cmd {
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
        let inner = txn_heart_beat_request_with_service_time(inner);
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_optional_keys(
            request.context.as_ref(),
            std::iter::once(inner.primary_key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvTxnHeartBeatResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdTxnHeartBeat as i32,
                    cmd: Some(raftpb::request::Cmd::TxnHeartBeat(inner)),
                },
                "txn heart beat",
            )
            .await?,
            KvTxnHeartBeatResponse
        );
        let response = match cmd {
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
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_optional_keys(request.context.as_ref(), keys)?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvTryAtomicMutateResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                    cmd: Some(raftpb::request::Cmd::TryAtomicMutate(inner.clone())),
                },
                "atomic mutate",
            )
            .await?,
            KvTryAtomicMutateResponse
        );
        let response = match cmd {
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
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_required_keys(request.context.as_ref(), keys)?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(kvpb::KvInstallPreparedMvccEntriesResponse {
                response: None,
                region_error: Some(region_error),
            }));
        }
        let cmd = raft_cmd_or_region_error!(
            self.execute_raft_request(
                request.context.as_ref(),
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdInstallPreparedMvcc as i32,
                    cmd: Some(raftpb::request::Cmd::InstallPreparedMvcc(inner.clone())),
                },
                "install prepared mvcc",
            )
            .await?,
            KvInstallPreparedMvccEntriesResponse
        );
        let response = match cmd {
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
        let buffer = if request.buffer == 0 {
            DEFAULT_APPLY_WATCH_BUFFER
        } else {
            request.buffer as usize
        };
        let mut watch = self.engine.subscribe_apply();
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        tokio::spawn(async move {
            let mut dropped_events = 0;
            loop {
                match watch.recv().await {
                    Ok(event) => {
                        let keys = matching_apply_watch_keys(&event.keys, &prefix);
                        if keys.is_empty() {
                            continue;
                        }
                        for chunk in chunk_apply_watch_keys(keys) {
                            let mut out = event.clone();
                            out.keys = chunk;
                            let response = kvpb::ApplyWatchResponse {
                                event: Some(out),
                                dropped_events,
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                        dropped_events += dropped;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
