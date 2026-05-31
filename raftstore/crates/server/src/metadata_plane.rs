use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::{ApplyWatchProvider, MetadataCommandExecutor, MetadataReadExecutor};
use tonic::{Request, Response, Status};

use crate::admission_state::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
use crate::metadata_watch::{metadata_watch_apply_stream, MetadataWatchApplyStream};
use crate::{internal_error, AppliedRegionDescriptorProvider, RaftRuntimeStatusProvider};

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

    type WatchApplyStream = MetadataWatchApplyStream;

    async fn watch_apply(
        &self,
        request: Request<metadatapb::MetadataWatchApplyRequest>,
    ) -> Result<Response<Self::WatchApplyStream>, Status> {
        let stream = metadata_watch_apply_stream(&self.engine, request.into_inner())?;
        Ok(Response::new(stream))
    }
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
