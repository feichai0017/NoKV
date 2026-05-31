#[cfg(test)]
use std::net::SocketAddr;
use std::sync::Arc;

use nokv_raftnode::{
    MetadataCommandExecutor, MetadataReadExecutor, MetadataRetentionExecutor, RegionSnapshotEngine,
};

use crate::admission_state::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
#[cfg(test)]
use crate::{
    EmptyRegionDescriptorSink, EmptyRestartDiagnostics, EmptyTopologyPublisher,
    MetadataPlaneServer, RaftAdminServer,
};
use crate::{
    MetadataPlaneService, PeerEndpointCatalog, RaftAdminService, RegionAdmission,
    RegionDescriptorSink, RestartDiagnosticsProvider, TopologyPublisher,
};

#[cfg(test)]
pub async fn serve_with_openraft_metadata_region_admission_and_peer_endpoints<E>(
    addr: SocketAddr,
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
) -> Result<(), tonic::transport::Error>
where
    E: RegionSnapshotEngine
        + MetadataCommandExecutor
        + MetadataReadExecutor
        + MetadataRetentionExecutor,
{
    let (metadata, admin) = openraft_metadata_service_pair_with_execution(
        region.clone(),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
        peer_endpoints,
        EmptyRegionDescriptorSink,
        Arc::new(EmptyTopologyPublisher),
        Arc::new(EmptyRestartDiagnostics),
    );
    let transport = nokv_raftnode::TonicRaftTransportRegistry::default();
    transport.register(admission.region_id, region.raft_handle());
    tonic::transport::Server::builder()
        .add_service(MetadataPlaneServer::new(metadata))
        .add_service(RaftAdminServer::new(admin))
        .add_service(nokv_raftnode::RaftTransportServer::new(transport.service()))
        .serve(addr)
        .await
}

pub fn openraft_metadata_service_pair<E, D>(
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
    descriptor_sink: D,
    topology_publisher: Arc<dyn TopologyPublisher>,
    restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
) -> (
    MetadataPlaneService<nokv_raftnode::OpenRaftRegion<E>>,
    RaftAdminService<nokv_raftnode::OpenRaftRegion<E>, D>,
)
where
    E: RegionSnapshotEngine
        + MetadataCommandExecutor
        + MetadataReadExecutor
        + MetadataRetentionExecutor,
    D: RegionDescriptorSink,
{
    let execution = ExecutionRuntime::default();
    let admission = RegionAdmissionState::new(admission);
    openraft_metadata_service_pair_with_execution(
        region,
        admission,
        execution,
        peer_endpoints,
        descriptor_sink,
        topology_publisher,
        restart_diagnostics,
    )
}

fn openraft_metadata_service_pair_with_execution<E, D>(
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmissionState,
    execution: ExecutionRuntime,
    peer_endpoints: PeerEndpointCatalog,
    descriptor_sink: D,
    topology_publisher: Arc<dyn TopologyPublisher>,
    restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
) -> (
    MetadataPlaneService<nokv_raftnode::OpenRaftRegion<E>>,
    RaftAdminService<nokv_raftnode::OpenRaftRegion<E>, D>,
)
where
    E: RegionSnapshotEngine
        + MetadataCommandExecutor
        + MetadataReadExecutor
        + MetadataRetentionExecutor,
    D: RegionDescriptorSink,
{
    let metadata = MetadataPlaneService::with_admission_state_and_execution(
        region.clone(),
        admission.clone(),
        execution.clone(),
    );
    let admin =
        RaftAdminService::with_admission_state_execution_peer_endpoints_and_descriptor_sink(
            region,
            admission,
            execution,
            peer_endpoints,
            descriptor_sink,
        )
        .with_topology_publisher(topology_publisher)
        .with_restart_diagnostics(restart_diagnostics);
    (metadata, admin)
}
