use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::{
    ApplyWatchProvider, ApplyWatchReplayRequest, MetadataCommandExecutor, MetadataReadExecutor,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::admission_state::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
use crate::wire_helpers::{chunk_apply_watch_keys, matching_apply_watch_keys};
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
        self.execution
            .record_admission(class, context, region_error);
    }
}

#[tonic::async_trait]
impl<E> metadatapb::metadata_plane_server::MetadataPlane for MetadataPlaneService<E>
where
    E: AppliedRegionDescriptorProvider
        + ApplyWatchProvider
        + MetadataCommandExecutor
        + MetadataReadExecutor
        + RaftRuntimeStatusProvider,
{
    async fn get(
        &self,
        request: Request<metadatapb::MetadataGetRequest>,
    ) -> Result<Response<metadatapb::MetadataGetResponse>, Status> {
        let request = request.into_inner();
        let context = required_metadata_context(request.context.as_ref())?;
        let admission = self.admission_snapshot()?;
        let region_error = admission
            .admit_read_optional_keys(Some(context), std::iter::once(request.key.as_slice()))?;
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
        let response = self
            .engine
            .execute_metadata_get(&request)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(response))
    }

    async fn batch_get(
        &self,
        request: Request<metadatapb::MetadataBatchGetRequest>,
    ) -> Result<Response<metadatapb::MetadataBatchGetResponse>, Status> {
        let request = request.into_inner();
        let context = required_metadata_context(request.context.as_ref())?;
        if request.requests.is_empty() {
            return Ok(Response::new(
                metadatapb::MetadataBatchGetResponse::default(),
            ));
        }
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_read_optional_keys(
            Some(context),
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
        let response = self
            .engine
            .execute_metadata_batch_get(&request)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(response))
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
        let context = required_metadata_context(request.context.as_ref())?;
        let admission = self.admission_snapshot()?;
        let region_error = admission.admit_read_optional_keys(
            Some(context),
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
        let mut response = self
            .engine
            .execute_metadata_scan(&request)
            .await
            .map_err(internal_error)?;
        if response.region_error.is_none() && response.error.is_none() {
            response
                .kvs
                .retain(|kv| admission.key_in_range(kv.key.as_slice()));
        }
        Ok(Response::new(response))
    }

    async fn commit_metadata(
        &self,
        request: Request<metadatapb::MetadataCommitRequest>,
    ) -> Result<Response<metadatapb::MetadataCommitResponse>, Status> {
        let request = request.into_inner();
        let context = required_metadata_context(request.context.as_ref())?;
        let command = request
            .command
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("metadata command is required"))?;
        let admission = self.admission_snapshot()?;
        let region_error = admission
            .admit_leader_required_keys(Some(context), metadata_command_admission_keys(command))?;
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
        let replay = self
            .engine
            .replay_apply(ApplyWatchReplayRequest {
                region_id: request.resume_region_id,
                term: request.resume_term,
                index: request.resume_index,
                key_prefix: request.key_prefix.clone(),
            })
            .map_err(internal_error)?;
        tokio::spawn(async move {
            if replay.expired {
                let _ = tx
                    .send(Ok(metadatapb::MetadataWatchApplyResponse {
                        event: None,
                        dropped_events: 1,
                    }))
                    .await;
                return;
            }
            let mut last_replayed = replay.events.iter().fold(None, advance_apply_watch_cursor);
            for event in replay.events {
                if send_metadata_apply_event(&tx, &request, &event)
                    .await
                    .is_err()
                {
                    return;
                }
            }
            loop {
                match source.recv().await {
                    Ok(event) => {
                        if apply_watch_event_at_or_before(&event, last_replayed) {
                            continue;
                        }
                        if send_metadata_apply_event(&tx, &request, &event)
                            .await
                            .is_err()
                        {
                            return;
                        }
                        last_replayed = advance_apply_watch_cursor(last_replayed, &event);
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

async fn send_metadata_apply_event(
    tx: &tokio::sync::mpsc::Sender<Result<metadatapb::MetadataWatchApplyResponse, Status>>,
    request: &metadatapb::MetadataWatchApplyRequest,
    event: &metadatapb::MetadataApplyWatchEvent,
) -> Result<(), ()> {
    let keys = matching_apply_watch_keys(&event.keys, &request.key_prefix);
    for chunk in chunk_apply_watch_keys(keys) {
        let response = metadatapb::MetadataWatchApplyResponse {
            event: Some(metadatapb::MetadataApplyWatchEvent {
                region_id: event.region_id,
                term: event.term,
                index: event.index,
                source: event.source,
                commit_version: event.commit_version,
                keys: chunk,
            }),
            dropped_events: 0,
        };
        tx.send(Ok(response)).await.map_err(|_| ())?;
    }
    Ok(())
}

fn advance_apply_watch_cursor(
    current: Option<(u64, u64)>,
    event: &metadatapb::MetadataApplyWatchEvent,
) -> Option<(u64, u64)> {
    let event_cursor = (event.term, event.index);
    match current {
        Some(cursor) if !apply_watch_cursor_after(event_cursor, cursor) => Some(cursor),
        _ => Some(event_cursor),
    }
}

fn apply_watch_event_at_or_before(
    event: &metadatapb::MetadataApplyWatchEvent,
    cursor: Option<(u64, u64)>,
) -> bool {
    let Some(cursor) = cursor else {
        return false;
    };
    !apply_watch_cursor_after((event.term, event.index), cursor)
}

fn apply_watch_cursor_after(candidate: (u64, u64), cursor: (u64, u64)) -> bool {
    if cursor.0 == 0 {
        return candidate.1 > cursor.1;
    }
    candidate.0 > cursor.0 || (candidate.0 == cursor.0 && candidate.1 > cursor.1)
}

fn required_metadata_context(
    context: Option<&metadatapb::MetadataContext>,
) -> Result<&metadatapb::MetadataContext, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
    if context.region_id == 0 {
        return Err(Status::invalid_argument("region id is required"));
    }
    Ok(context)
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
