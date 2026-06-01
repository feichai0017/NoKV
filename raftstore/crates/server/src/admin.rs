use std::sync::Arc;

use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_raftnode::MetadataRetentionExecutor;
use tonic::{Request, Response, Status};

use crate::admission::RegionAdmission;
use crate::admission_state::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
use crate::peer_endpoint_catalog::PeerEndpointCatalog;
use crate::region_runtime::{
    AppliedRegionDescriptorProvider, EmptyApplyStatus, RaftMembershipAdmin,
    RaftRuntimeStatusProvider,
};
use crate::topology::peer_change_transition_id;
use crate::{
    internal_error, EmptyRegionDescriptorSink, EmptyRestartDiagnostics, EmptyTopologyPublisher,
    RegionDescriptorSink, RestartDiagnosticsProvider, TopologyPublisher,
};

#[derive(Clone)]
pub struct RaftAdminService<S = EmptyApplyStatus, D = EmptyRegionDescriptorSink> {
    pub(crate) status: S,
    pub(crate) admission: RegionAdmissionState,
    pub(crate) peer_endpoints: PeerEndpointCatalog,
    pub(crate) execution: ExecutionRuntime,
    pub(crate) descriptor_sink: D,
    pub(crate) topology_publisher: Arc<dyn TopologyPublisher>,
    pub(crate) restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
}

impl<S> RaftAdminService<S, EmptyRegionDescriptorSink> {
    pub fn new(status: S) -> Self {
        RaftAdminService::with_admission_state_execution_peer_endpoints_and_descriptor_sink(
            status,
            RegionAdmissionState::new(RegionAdmission::default()),
            ExecutionRuntime::default(),
            PeerEndpointCatalog::default(),
            EmptyRegionDescriptorSink,
        )
    }

    pub fn with_admission(status: S, admission: RegionAdmission) -> Self {
        RaftAdminService::with_admission_state_execution_peer_endpoints_and_descriptor_sink(
            status,
            RegionAdmissionState::new(admission),
            ExecutionRuntime::default(),
            PeerEndpointCatalog::default(),
            EmptyRegionDescriptorSink,
        )
    }
}

impl<S, D> RaftAdminService<S, D>
where
    D: RegionDescriptorSink,
{
    pub(crate) fn with_admission_state_execution_peer_endpoints_and_descriptor_sink(
        status: S,
        admission: RegionAdmissionState,
        execution: ExecutionRuntime,
        peer_endpoints: PeerEndpointCatalog,
        descriptor_sink: D,
    ) -> Self {
        Self {
            status,
            admission,
            peer_endpoints,
            execution,
            descriptor_sink,
            topology_publisher: Arc::new(EmptyTopologyPublisher),
            restart_diagnostics: Arc::new(EmptyRestartDiagnostics),
        }
    }

    pub(crate) fn with_topology_publisher(
        mut self,
        topology_publisher: Arc<dyn TopologyPublisher>,
    ) -> Self {
        self.topology_publisher = topology_publisher;
        self
    }

    pub(crate) fn with_restart_diagnostics(
        mut self,
        restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
    ) -> Self {
        self.restart_diagnostics = restart_diagnostics;
        self
    }
}

impl<S, D> RaftAdminService<S, D>
where
    S: AppliedRegionDescriptorProvider + RaftRuntimeStatusProvider,
{
    fn admission_snapshot(&self) -> Result<RegionAdmission, Status> {
        let runtime = self.status.raft_runtime_status();
        self.admission.with_applied_descriptor_and_runtime_status(
            self.status.applied_region_descriptor()?,
            runtime,
        )
    }
}

#[tonic::async_trait]
impl<S, D> adminpb::raft_admin_server::RaftAdmin for RaftAdminService<S, D>
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
        self.admission.validate_region(request.region_id)?;
        self.status
            .add_voter(
                request.peer_id,
                self.peer_endpoints
                    .node_for_peer(request.store_id, request.peer_id)?,
            )
            .await?;
        let region = self.admission.add_peer(request.peer_id, request.store_id)?;
        self.status.propose_region_descriptor(&region).await?;
        self.descriptor_sink.save_region_descriptor(&region)?;
        let publish = self
            .topology_publisher
            .publish_peer_added(
                request.region_id,
                request.store_id,
                request.peer_id,
                &region,
            )
            .await;
        self.execution.record_topology_applied(
            request.region_id,
            request.peer_id,
            peer_change_transition_id("add", request.region_id, request.store_id, request.peer_id),
            "peer change",
            publish.publish,
            publish.last_error,
        );
        Ok(Response::new(adminpb::AddPeerResponse {
            region: Some(region),
        }))
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
        self.admission.validate_region(request.region_id)?;
        let removed_store_id = self
            .admission
            .descriptor()?
            .peers
            .iter()
            .find(|peer| peer.peer_id == request.peer_id)
            .map(|peer| peer.store_id)
            .unwrap_or(request.peer_id);
        self.status.remove_voter(request.peer_id).await?;
        let region = self.admission.remove_peer(request.peer_id)?;
        self.status.propose_region_descriptor(&region).await?;
        self.descriptor_sink.save_region_descriptor(&region)?;
        let publish = self
            .topology_publisher
            .publish_peer_removed(
                request.region_id,
                removed_store_id,
                request.peer_id,
                &region,
            )
            .await;
        self.execution.record_topology_applied(
            request.region_id,
            request.peer_id,
            peer_change_transition_id(
                "remove",
                request.region_id,
                removed_store_id,
                request.peer_id,
            ),
            "peer change",
            publish.publish,
            publish.last_error,
        );
        Ok(Response::new(adminpb::RemovePeerResponse {
            region: Some(region),
        }))
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
        self.admission.validate_region(request.region_id)?;
        let runtime = self.status.raft_runtime_status();
        if runtime.leader && request.peer_id != runtime.local_peer_id {
            if let Some(endpoint) = self.peer_endpoints.endpoint_for_peer(request.peer_id)? {
                transfer_leader_via_peer(&endpoint, request.region_id, request.peer_id).await?;
                return Ok(Response::new(adminpb::TransferLeaderResponse {
                    region: Some(self.admission_snapshot()?.descriptor()),
                }));
            }
        }
        self.status.transfer_leader(request.peer_id).await?;
        Ok(Response::new(adminpb::TransferLeaderResponse {
            region: Some(self.admission_snapshot()?.descriptor()),
        }))
    }

    async fn region_runtime_status(
        &self,
        request: Request<adminpb::RegionRuntimeStatusRequest>,
    ) -> Result<Response<adminpb::RegionRuntimeStatusResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 {
            return Err(Status::invalid_argument("region_id is required"));
        }
        let status = self.status.apply_status();
        let runtime = self.status.raft_runtime_status();
        let hosted = status.region_id != 0 && runtime.hosted;
        if request.region_id != status.region_id {
            return Ok(Response::new(
                adminpb::RegionRuntimeStatusResponse::default(),
            ));
        }
        let region = if !hosted {
            None
        } else {
            Some(self.admission_snapshot()?.descriptor())
        };
        Ok(Response::new(adminpb::RegionRuntimeStatusResponse {
            known: hosted,
            hosted,
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
        let runtime = self.status.raft_runtime_status();
        let hosted = status.region_id != 0 && runtime.hosted;
        let mut topology = self.execution.topology_snapshot()?;
        for blocked in self.restart_diagnostics.blocked_topology_statuses()? {
            push_missing_topology_status(&mut topology, blocked);
        }
        for pending in self.restart_diagnostics.pending_topology_statuses()? {
            push_missing_topology_status(&mut topology, pending);
        }
        Ok(Response::new(adminpb::ExecutionStatusResponse {
            last_admission: Some(self.execution.snapshot()?),
            restart: Some(adminpb::ExecutionRestartStatus {
                state: if hosted {
                    adminpb::ExecutionRestartState::Ready as i32
                } else {
                    adminpb::ExecutionRestartState::Degraded as i32
                },
                region_count: u64::from(hosted),
                raft_group_count: u64::from(hosted),
                pending_root_event_count: self.restart_diagnostics.pending_root_event_count()?,
                blocked_root_event_count: self.restart_diagnostics.blocked_root_event_count()?,
                pending_scheduler_operation_count: self
                    .restart_diagnostics
                    .pending_scheduler_operation_count()?,
                ..Default::default()
            }),
            topology,
            ..Default::default()
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
        self.admission.validate_region(request.region_id)?;
        let status = self.status.apply_status();
        let runtime = self.status.raft_runtime_status();
        if status.region_id != request.region_id || !runtime.hosted {
            return Err(Status::failed_precondition("region is not hosted"));
        }
        let result = self
            .status
            .prune_metadata_versions(request.retention_floor)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(adminpb::PruneMetadataVersionsResponse {
            retention_floor: result.retention_floor,
            pruned_versions: result.pruned_versions,
            retained_anchor_versions: result.retained_anchor_versions,
        }))
    }
}

pub(crate) fn push_missing_topology_status(
    topology: &mut Vec<adminpb::ExecutionTopologyStatus>,
    status: adminpb::ExecutionTopologyStatus,
) {
    if topology
        .iter()
        .any(|existing| existing.transition_id == status.transition_id)
    {
        return;
    }
    topology.push(status);
}

async fn transfer_leader_via_peer(
    endpoint: &str,
    region_id: u64,
    peer_id: u64,
) -> Result<(), Status> {
    let mut client =
        adminpb::raft_admin_client::RaftAdminClient::connect(normalize_admin_endpoint(endpoint))
            .await
            .map_err(|err| {
                Status::failed_precondition(format!("dial target peer {peer_id}: {err}"))
            })?;
    client
        .transfer_leader(adminpb::TransferLeaderRequest { region_id, peer_id })
        .await
        .map(|_| ())
        .map_err(|err| {
            Status::failed_precondition(format!("target peer {peer_id} transfer failed: {err}"))
        })
}

fn normalize_admin_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    }
}
