//! Multi-region RaftAdmin routing and process-level diagnostics.

use std::sync::Arc;

use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_raftnode::MetadataRetentionExecutor;
use tonic::{Request, Response, Status};

use crate::region_registry::{admin_region_lookup, RegionServiceRegistry};
use crate::{
    push_missing_topology_status, AppliedRegionDescriptorProvider, EmptyRegionDescriptorSink,
    EmptyRestartDiagnostics, RaftAdminService, RaftMembershipAdmin, RaftRuntimeStatusProvider,
    RegionDescriptorSink, RestartDiagnosticsProvider,
};

/// RaftAdmin service router for a process that hosts more than one region.
///
/// Region-scoped admin requests are routed by the request `region_id`.
/// Process-scoped diagnostics are aggregated across hosted regions so the
/// existing `ExecutionStatus` response can describe multi-region readiness
/// without changing the protobuf contract.
#[derive(Clone)]
pub struct MultiRegionRaftAdminService<S, D = EmptyRegionDescriptorSink> {
    regions: RegionServiceRegistry<RaftAdminService<S, D>>,
    restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
}

impl<S, D> MultiRegionRaftAdminService<S, D> {
    pub fn new(
        regions: impl IntoIterator<Item = (u64, RaftAdminService<S, D>)>,
    ) -> Result<Self, Status> {
        Ok(Self {
            regions: RegionServiceRegistry::new(regions)?,
            restart_diagnostics: Arc::new(EmptyRestartDiagnostics),
        })
    }

    pub fn insert_region(
        &self,
        region_id: u64,
        service: RaftAdminService<S, D>,
    ) -> Result<(), Status> {
        self.regions.insert_region(region_id, service)
    }

    pub fn remove_region(&self, region_id: u64) -> Result<Option<RaftAdminService<S, D>>, Status> {
        self.regions.remove_region(region_id)
    }

    pub fn with_restart_diagnostics(
        mut self,
        restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
    ) -> Self {
        self.restart_diagnostics = restart_diagnostics;
        self
    }

    fn service_for_region(&self, region_id: u64) -> Result<RaftAdminService<S, D>, Status>
    where
        RaftAdminService<S, D>: Clone,
    {
        admin_region_lookup(&self.regions, region_id)
    }
}

#[tonic::async_trait]
impl<S, D> adminpb::raft_admin_server::RaftAdmin for MultiRegionRaftAdminService<S, D>
where
    S: AppliedRegionDescriptorProvider
        + MetadataRetentionExecutor
        + RaftMembershipAdmin
        + RaftRuntimeStatusProvider,
    D: RegionDescriptorSink,
{
    async fn add_peer(
        &self,
        request: Request<adminpb::AddPeerRequest>,
    ) -> Result<Response<adminpb::AddPeerResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 || request.store_id == 0 || request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id, store_id, and peer_id are required",
            ));
        }
        self.service_for_region(request.region_id)?
            .add_peer(Request::new(request))
            .await
    }

    async fn remove_peer(
        &self,
        request: Request<adminpb::RemovePeerRequest>,
    ) -> Result<Response<adminpb::RemovePeerResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 || request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id and peer_id are required",
            ));
        }
        self.service_for_region(request.region_id)?
            .remove_peer(Request::new(request))
            .await
    }

    async fn transfer_leader(
        &self,
        request: Request<adminpb::TransferLeaderRequest>,
    ) -> Result<Response<adminpb::TransferLeaderResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 || request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id and peer_id are required",
            ));
        }
        self.service_for_region(request.region_id)?
            .transfer_leader(Request::new(request))
            .await
    }

    async fn region_runtime_status(
        &self,
        request: Request<adminpb::RegionRuntimeStatusRequest>,
    ) -> Result<Response<adminpb::RegionRuntimeStatusResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 {
            return Err(Status::invalid_argument("region_id is required"));
        }
        let Some(service) = self.regions.get_region(request.region_id)? else {
            return Ok(Response::new(
                adminpb::RegionRuntimeStatusResponse::default(),
            ));
        };
        service.region_runtime_status(Request::new(request)).await
    }

    async fn execution_status(
        &self,
        _request: Request<adminpb::ExecutionStatusRequest>,
    ) -> Result<Response<adminpb::ExecutionStatusResponse>, Status> {
        let mut last_admission = None;
        let mut restart = adminpb::ExecutionRestartStatus {
            state: adminpb::ExecutionRestartState::Ready as i32,
            ..Default::default()
        };
        let mut topology = Vec::new();

        for service in self.regions.values()? {
            let status = service.status.apply_status();
            let runtime = service.status.raft_runtime_status();
            let hosted = status.region_id != 0 && runtime.hosted;
            if hosted {
                restart.region_count += 1;
                restart.raft_group_count += 1;
            } else {
                restart.state = adminpb::ExecutionRestartState::Degraded as i32;
            }
            merge_last_admission(&mut last_admission, Some(service.execution.snapshot()?));
            for status in service.execution.topology_snapshot()? {
                push_missing_topology_status(&mut topology, status);
            }
        }
        if self.regions.is_empty()? {
            restart.state = adminpb::ExecutionRestartState::Degraded as i32;
        }
        restart.pending_root_event_count = self.restart_diagnostics.pending_root_event_count()?;
        restart.blocked_root_event_count = self.restart_diagnostics.blocked_root_event_count()?;
        restart.pending_scheduler_operation_count = self
            .restart_diagnostics
            .pending_scheduler_operation_count()?;
        for blocked in self.restart_diagnostics.blocked_topology_statuses()? {
            push_missing_topology_status(&mut topology, blocked);
        }
        for pending in self.restart_diagnostics.pending_topology_statuses()? {
            push_missing_topology_status(&mut topology, pending);
        }
        Ok(Response::new(adminpb::ExecutionStatusResponse {
            last_admission,
            restart: Some(restart),
            topology,
        }))
    }

    async fn prune_metadata_versions(
        &self,
        request: Request<adminpb::PruneMetadataVersionsRequest>,
    ) -> Result<Response<adminpb::PruneMetadataVersionsResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 {
            return Err(Status::invalid_argument("region_id is required"));
        }
        self.service_for_region(request.region_id)?
            .prune_metadata_versions(Request::new(request))
            .await
    }
}

fn merge_last_admission(
    current: &mut Option<adminpb::ExecutionAdmissionStatus>,
    next: Option<adminpb::ExecutionAdmissionStatus>,
) {
    let Some(next) = next else {
        return;
    };
    if !next.observed {
        return;
    }
    let replace = current
        .as_ref()
        .map(|current| !current.observed || current.at_unix_nano <= next.at_unix_nano)
        .unwrap_or(true);
    if replace {
        *current = Some(next);
    }
}
