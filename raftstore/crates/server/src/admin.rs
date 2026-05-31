use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{
    AppliedMetadataEngine, ApplyStatusProvider, BasicNode, MetadataRetentionExecutor,
    PersistentAppliedMetadataEngine, RegionMetadataSink, RegionSnapshotEngine,
};
use tonic::{Request, Response, Status};

use crate::admission::RegionAdmission;
use crate::admission_state::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
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
        Self::with_admission(status, RegionAdmission::default())
    }

    pub fn with_admission(status: S, admission: RegionAdmission) -> Self {
        Self::with_admission_and_execution(status, admission, ExecutionRuntime::default())
    }

    pub(crate) fn with_admission_and_execution(
        status: S,
        admission: RegionAdmission,
        execution: ExecutionRuntime,
    ) -> Self {
        Self::with_admission_state_and_execution(
            status,
            RegionAdmissionState::new(admission),
            execution,
        )
    }

    pub(crate) fn with_admission_state_and_execution(
        status: S,
        admission: RegionAdmissionState,
        execution: ExecutionRuntime,
    ) -> Self {
        Self::with_admission_state_execution_and_peer_endpoints(
            status,
            admission,
            execution,
            PeerEndpointCatalog::default(),
        )
    }
}

impl<S, D> RaftAdminService<S, D>
where
    D: RegionDescriptorSink,
{
    pub(crate) fn with_admission_state_execution_and_peer_endpoints(
        status: S,
        admission: RegionAdmissionState,
        execution: ExecutionRuntime,
        peer_endpoints: PeerEndpointCatalog,
    ) -> RaftAdminService<S, EmptyRegionDescriptorSink> {
        RaftAdminService::with_admission_state_execution_peer_endpoints_and_descriptor_sink(
            status,
            admission,
            execution,
            peer_endpoints,
            EmptyRegionDescriptorSink,
        )
    }

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

#[derive(Debug, Clone, Default)]
pub struct PeerEndpointCatalog {
    endpoints: Arc<Mutex<BTreeMap<u64, String>>>,
    require_configured: bool,
}

impl PeerEndpointCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn require_configured() -> Self {
        Self {
            endpoints: Arc::new(Mutex::new(BTreeMap::new())),
            require_configured: true,
        }
    }

    pub fn insert_peer(&self, peer_id: u64, endpoint: impl Into<String>) -> Result<(), Status> {
        if peer_id == 0 {
            return Err(Status::invalid_argument("peer_id is required"));
        }
        let endpoint = endpoint.into();
        if endpoint.is_empty() {
            return Err(Status::invalid_argument("peer endpoint is required"));
        }
        self.endpoints
            .lock()
            .map_err(|_| peer_endpoint_catalog_poisoned())?
            .insert(peer_id, endpoint);
        Ok(())
    }

    pub fn node_for_peer(&self, store_id: u64, peer_id: u64) -> Result<BasicNode, Status> {
        let endpoints = self
            .endpoints
            .lock()
            .map_err(|_| peer_endpoint_catalog_poisoned())?;
        if let Some(endpoint) = endpoints.get(&peer_id) {
            return Ok(BasicNode::new(endpoint.clone()));
        }
        if self.require_configured {
            return Err(Status::failed_precondition(format!(
                "endpoint for store {store_id} peer {peer_id} is not configured"
            )));
        }
        Ok(BasicNode::new(format!("store-{store_id}-peer-{peer_id}")))
    }

    fn endpoint_for_peer(&self, peer_id: u64) -> Result<Option<String>, Status> {
        self.endpoints
            .lock()
            .map_err(|_| peer_endpoint_catalog_poisoned())
            .map(|endpoints| endpoints.get(&peer_id).cloned())
    }
}

fn peer_endpoint_catalog_poisoned() -> Status {
    Status::internal("peer endpoint catalog mutex poisoned")
}

fn membership_unimplemented() -> Status {
    Status::unimplemented("rust raft membership requires an OpenRaftRegion")
}

#[tonic::async_trait]
pub trait RaftMembershipAdmin: Clone + Send + Sync + 'static {
    async fn add_voter(&self, peer_id: u64, node: BasicNode) -> Result<(), Status>;
    async fn remove_voter(&self, peer_id: u64) -> Result<(), Status>;
    async fn transfer_leader(&self, peer_id: u64) -> Result<(), Status>;
    async fn propose_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RaftRuntimeStatus {
    pub local_peer_id: u64,
    pub leader_peer_id: u64,
    pub leader: bool,
    pub hosted: bool,
}

pub trait RaftRuntimeStatusProvider: ApplyStatusProvider {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus;
}

pub trait AppliedRegionDescriptorProvider: ApplyStatusProvider {
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        Ok(None)
    }
}

#[tonic::async_trait]
impl<E> RaftMembershipAdmin for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    async fn add_voter(&self, peer_id: u64, node: BasicNode) -> Result<(), Status> {
        self.add_voter(peer_id, node)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }

    async fn remove_voter(&self, peer_id: u64) -> Result<(), Status> {
        nokv_raftnode::OpenRaftRegion::remove_voter(self, peer_id, false)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }

    async fn transfer_leader(&self, peer_id: u64) -> Result<(), Status> {
        nokv_raftnode::OpenRaftRegion::transfer_leader(self, peer_id)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }

    async fn propose_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        self.propose_region_descriptor(descriptor)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }
}

impl<E> RaftRuntimeStatusProvider for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let local_peer_id = self.node_id();
        let metrics = self.raft_handle().metrics();
        let metrics = metrics.borrow();
        let leader_peer_id = metrics.current_leader.unwrap_or_default();
        let hosted = metrics
            .membership_config
            .voter_ids()
            .any(|peer_id| peer_id == local_peer_id);
        RaftRuntimeStatus {
            local_peer_id,
            leader_peer_id,
            leader: leader_peer_id == local_peer_id && leader_peer_id != 0,
            hosted,
        }
    }
}

impl<E> AppliedRegionDescriptorProvider for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        nokv_raftnode::OpenRaftRegion::region_descriptor(self).map_err(internal_error)
    }
}

#[tonic::async_trait]
impl<E> RaftMembershipAdmin for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn propose_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl<E> RaftRuntimeStatusProvider for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let known = self.apply_status().region_id != 0;
        RaftRuntimeStatus {
            local_peer_id: u64::from(known),
            leader_peer_id: u64::from(known),
            leader: known,
            hosted: known,
        }
    }
}

impl<E> AppliedRegionDescriptorProvider for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        AppliedMetadataEngine::region_descriptor(self).map_err(internal_error)
    }
}

#[tonic::async_trait]
impl<E, S> RaftMembershipAdmin for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn propose_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl<E, S> RaftRuntimeStatusProvider for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let known = self.apply_status().region_id != 0;
        RaftRuntimeStatus {
            local_peer_id: u64::from(known),
            leader_peer_id: u64::from(known),
            leader: known,
            hosted: known,
        }
    }
}

impl<E, S> AppliedRegionDescriptorProvider for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        self.inner().region_descriptor().map_err(internal_error)
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

    async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn propose_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl RaftRuntimeStatusProvider for EmptyApplyStatus {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        RaftRuntimeStatus::default()
    }
}

impl AppliedRegionDescriptorProvider for EmptyApplyStatus {}

impl MetadataRetentionExecutor for EmptyApplyStatus {
    fn prune_metadata_versions<'a>(
        &'a self,
        _retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metastore::Result<nokv_metastore::MetadataRetentionResult>,
    > + Send
           + 'a {
        async move {
            Err(nokv_metastore::Error::Backend(
                "metadata retention requires a hosted region".to_owned(),
            ))
        }
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
