use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_proto::nokv::raft::v1 as raftpb;
use nokv_raftnode::{ApplyWatchProvider, MetadataCommandExecutor, RaftCommandExecutor};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::execution::ExecutionRuntime;
use crate::service::{
    chunk_apply_watch_keys, header_from_context, matching_apply_watch_keys, raft_payload_error,
    trim_scan_response_to_region, RegionAdmissionState,
};
use crate::{
    internal_error, AppliedRegionDescriptorProvider, RaftRuntimeStatusProvider,
    DEFAULT_APPLY_WATCH_BUFFER,
};

#[derive(Clone)]
pub struct MetadataPlaneService<E> {
    engine: E,
    admission: RegionAdmissionState,
    execution: ExecutionRuntime,
}

impl<E> MetadataPlaneService<E> {
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

impl<E> MetadataPlaneService<E>
where
    E: AppliedRegionDescriptorProvider + RaftRuntimeStatusProvider,
{
    fn admission_snapshot(&self) -> Result<crate::RegionAdmission, Status> {
        let runtime = self.engine.raft_runtime_status();
        self.admission.with_applied_descriptor_and_runtime_status(
            self.engine.applied_region_descriptor()?,
            runtime,
        )
    }

    fn record_admission(
        &self,
        class: adminpb::ExecutionAdmissionClass,
        context: Option<&metadatapb::MetadataContext>,
        region_error: Option<&errorpb::RegionError>,
    ) {
        let kv_context = context.map(kv_context_from_metadata);
        self.execution
            .record_admission(class, kv_context.as_ref(), region_error);
    }
}

#[tonic::async_trait]
impl<E> metadatapb::metadata_plane_server::MetadataPlane for MetadataPlaneService<E>
where
    E: AppliedRegionDescriptorProvider
        + ApplyWatchProvider
        + MetadataCommandExecutor
        + RaftCommandExecutor
        + RaftRuntimeStatusProvider,
{
    async fn get(
        &self,
        request: Request<metadatapb::MetadataGetRequest>,
    ) -> Result<Response<metadatapb::MetadataGetResponse>, Status> {
        let request = request.into_inner();
        let kv_context = kv_context_from_required_metadata(request.context.as_ref())?;
        let admission = self.admission_snapshot()?;
        let region_error = admission
            .admit_read_optional_keys(Some(&kv_context), std::iter::once(request.key.as_slice()))?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Read,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(metadatapb::MetadataGetResponse {
                region_error: Some(region_error),
                ..Default::default()
            }));
        }
        let cmd = self
            .execute_raft_request(
                &kv_context,
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdGet as i32,
                    cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                        key: request.key,
                        version: request.version,
                    })),
                },
                "metadata get",
            )
            .await?;
        let raftpb::response::Cmd::Get(response) = cmd else {
            return Err(raft_payload_error("metadata get", "unexpected get payload"));
        };
        Ok(Response::new(metadata_get_response_from_kv(response)))
    }

    async fn batch_get(
        &self,
        request: Request<metadatapb::MetadataBatchGetRequest>,
    ) -> Result<Response<metadatapb::MetadataBatchGetResponse>, Status> {
        let request = request.into_inner();
        let kv_context = kv_context_from_required_metadata(request.context.as_ref())?;
        if request.requests.is_empty() {
            return Ok(Response::new(
                metadatapb::MetadataBatchGetResponse::default(),
            ));
        }
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_read_optional_keys(
            Some(&kv_context),
            request.requests.iter().map(|req| req.key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Read,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(metadatapb::MetadataBatchGetResponse {
                region_error: Some(region_error),
                ..Default::default()
            }));
        }
        let command = raftpb::RaftCmdRequest {
            header: Some(header_from_context(&kv_context)),
            requests: request
                .requests
                .iter()
                .map(|req| raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdGet as i32,
                    cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                        key: req.key.clone(),
                        version: req.version,
                    })),
                })
                .collect(),
        };
        let command_response = self
            .engine
            .execute_raft_command(&command)
            .await
            .map_err(internal_error)?;
        if let Some(region_error) = command_response.region_error {
            return Ok(Response::new(metadatapb::MetadataBatchGetResponse {
                region_error: Some(region_error),
                ..Default::default()
            }));
        }
        let mut responses = Vec::with_capacity(command_response.responses.len());
        for response in command_response.responses {
            match response.cmd {
                Some(raftpb::response::Cmd::Get(response)) => {
                    responses.push(metadata_get_response_from_kv(response))
                }
                _ => {
                    return Err(raft_payload_error(
                        "metadata batch get",
                        "unexpected get payload",
                    ))
                }
            }
        }
        Ok(Response::new(metadatapb::MetadataBatchGetResponse {
            responses,
            region_error: None,
        }))
    }

    async fn scan(
        &self,
        request: Request<metadatapb::MetadataScanRequest>,
    ) -> Result<Response<metadatapb::MetadataScanResponse>, Status> {
        let request = request.into_inner();
        if request.reverse {
            return Err(Status::unimplemented(
                "MetadataPlane Scan reverse scans are not supported yet",
            ));
        }
        let kv_context = kv_context_from_required_metadata(request.context.as_ref())?;
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_read_optional_keys(
            Some(&kv_context),
            std::iter::once(request.start_key.as_slice()),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Read,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(metadatapb::MetadataScanResponse {
                region_error: Some(region_error),
                ..Default::default()
            }));
        }
        let cmd = self
            .execute_raft_request(
                &kv_context,
                raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdScan as i32,
                    cmd: Some(raftpb::request::Cmd::Scan(kvpb::ScanRequest {
                        start_key: request.start_key,
                        limit: request.limit,
                        version: request.version,
                        include_start: request.include_start,
                        reverse: false,
                    })),
                },
                "metadata scan",
            )
            .await?;
        let raftpb::response::Cmd::Scan(mut response) = cmd else {
            return Err(raft_payload_error(
                "metadata scan",
                "unexpected scan payload",
            ));
        };
        trim_scan_response_to_region(&admission, &mut response);
        Ok(Response::new(metadata_scan_response_from_kv(response)))
    }

    async fn commit_metadata(
        &self,
        request: Request<metadatapb::MetadataCommitRequest>,
    ) -> Result<Response<metadatapb::MetadataCommitResponse>, Status> {
        let request = request.into_inner();
        let kv_context = kv_context_from_required_metadata(request.context.as_ref())?;
        let command = request
            .command
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("metadata command is required"))?;
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_leader_required_keys(
            Some(&kv_context),
            metadata_command_admission_keys(command),
        )?;
        self.record_admission(
            adminpb::ExecutionAdmissionClass::Write,
            request.context.as_ref(),
            region_error.as_ref(),
        );
        if let Some(region_error) = region_error {
            return Ok(Response::new(metadatapb::MetadataCommitResponse {
                region_error: Some(region_error),
                ..Default::default()
            }));
        }
        let response = self
            .engine
            .execute_metadata_command(&request)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(response))
    }

    type WatchApplyStream = ReceiverStream<Result<metadatapb::MetadataWatchApplyResponse, Status>>;

    async fn watch_apply(
        &self,
        request: Request<metadatapb::MetadataWatchApplyRequest>,
    ) -> Result<Response<Self::WatchApplyStream>, Status> {
        let request = request.into_inner();
        let mut source = self.engine.subscribe_apply();
        let (tx, rx) = tokio::sync::mpsc::channel(
            request
                .buffer
                .try_into()
                .ok()
                .filter(|buffer| *buffer > 0)
                .unwrap_or(DEFAULT_APPLY_WATCH_BUFFER),
        );
        tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Ok(event) => {
                        let keys = matching_apply_watch_keys(&event.keys, &request.key_prefix);
                        for chunk in chunk_apply_watch_keys(keys) {
                            let response = metadatapb::MetadataWatchApplyResponse {
                                event: Some(metadatapb::MetadataApplyWatchEvent {
                                    region_id: event.region_id,
                                    term: event.term,
                                    index: event.index,
                                    source: metadatapb::MetadataApplyWatchEventSource::Commit
                                        as i32,
                                    commit_version: event.commit_version,
                                    keys: chunk,
                                }),
                                dropped_events: 0,
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                        if tx
                            .send(Ok(metadatapb::MetadataWatchApplyResponse {
                                event: None,
                                dropped_events: dropped,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl<E> MetadataPlaneService<E>
where
    E: RaftCommandExecutor,
{
    async fn execute_raft_request(
        &self,
        context: &kvpb::Context,
        request: raftpb::Request,
        operation: &str,
    ) -> Result<raftpb::response::Cmd, Status> {
        let response = self
            .engine
            .execute_raft_command(&raftpb::RaftCmdRequest {
                header: Some(header_from_context(context)),
                requests: vec![request],
            })
            .await
            .map_err(internal_error)?;
        if response.region_error.is_some() {
            return Err(raft_payload_error(operation, "unexpected region error"));
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
    }
}

fn kv_context_from_required_metadata(
    context: Option<&metadatapb::MetadataContext>,
) -> Result<kvpb::Context, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
    if context.region_id == 0 {
        return Err(Status::invalid_argument("region id is required"));
    }
    Ok(kv_context_from_metadata(context))
}

fn kv_context_from_metadata(context: &metadatapb::MetadataContext) -> kvpb::Context {
    kvpb::Context {
        region_id: context.region_id,
        region_epoch: context.region_epoch.clone(),
        peer: context.peer.clone(),
        read_consistency: match metadatapb::ReadConsistency::try_from(context.read_consistency)
            .unwrap_or(metadatapb::ReadConsistency::Strong)
        {
            metadatapb::ReadConsistency::Strong => kvpb::ReadConsistency::Strong,
            metadatapb::ReadConsistency::BoundedStale => kvpb::ReadConsistency::BoundedStale,
        } as i32,
        read_preference: match metadatapb::ReadPreference::try_from(context.read_preference)
            .unwrap_or(metadatapb::ReadPreference::LeaderOnly)
        {
            metadatapb::ReadPreference::LeaderOnly => kvpb::ReadPreference::LeaderOnly,
            metadatapb::ReadPreference::FollowerPrefer => kvpb::ReadPreference::FollowerPrefer,
        } as i32,
        ..Default::default()
    }
}

fn metadata_get_response_from_kv(response: kvpb::GetResponse) -> metadatapb::MetadataGetResponse {
    metadatapb::MetadataGetResponse {
        kv: (!response.not_found && response.error.is_none()).then(|| metadatapb::MetadataKv {
            value: response.value,
            expires_at: response.expires_at,
            ..Default::default()
        }),
        not_found: response.not_found,
        error: response.error.map(metadata_key_error_from_kv),
        region_error: None,
    }
}

fn metadata_scan_response_from_kv(
    response: kvpb::ScanResponse,
) -> metadatapb::MetadataScanResponse {
    metadatapb::MetadataScanResponse {
        kvs: response
            .kvs
            .into_iter()
            .map(|kv| metadatapb::MetadataKv {
                key: kv.key,
                value: kv.value,
                version: kv.version,
                expires_at: kv.expires_at,
            })
            .collect(),
        error: response.error.map(metadata_key_error_from_kv),
        region_error: None,
    }
}

fn metadata_key_error_from_kv(error: kvpb::KeyError) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        locked: error.locked.map(|locked| metadatapb::MetadataLocked {
            primary_lock: locked.primary_lock,
            key: locked.key,
            lock_version: locked.lock_version,
            lock_ttl: locked.lock_ttl,
        }),
        write_conflict: error
            .write_conflict
            .map(|conflict| metadatapb::MetadataWriteConflict {
                key: conflict.key,
                primary: conflict.primary,
                conflict_ts: conflict.conflict_ts,
                commit_ts: conflict.commit_ts,
                start_ts: conflict.start_ts,
            }),
        already_exists: error
            .already_exists
            .map(|exists| metadatapb::MetadataKeyAlreadyExists { key: exists.key }),
        retryable: error.retryable,
        abort: error.abort,
    }
}

fn metadata_command_admission_keys(
    command: &metadatapb::MetadataCommand,
) -> impl Iterator<Item = &[u8]> {
    command
        .mutations
        .iter()
        .map(|mutation| mutation.key.as_slice())
        .chain(
            command
                .predicates
                .iter()
                .map(|predicate| predicate.key.as_slice()),
        )
}
