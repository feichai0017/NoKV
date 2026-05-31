use std::net::SocketAddr;
use std::sync::Arc;

use nokv_raftnode::{MetadataCommandExecutor, RaftCommandExecutor, RegionSnapshotEngine};

use crate::execution::ExecutionRuntime;
use crate::service::RegionAdmissionState;
use crate::{
    EmptyRegionDescriptorSink, EmptyRestartDiagnostics, EmptyTopologyPublisher,
    MetadataPlaneServer, MetadataPlaneService, PeerEndpointCatalog, RaftAdminServer,
    RaftAdminService, RegionAdmission, RegionDescriptorSink, RestartDiagnosticsProvider,
    StoreKvServer, StoreKvService, TopologyPublisher,
};

pub async fn serve_with_openraft_region_admission_and_peer_endpoints<E>(
    addr: SocketAddr,
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
) -> Result<(), tonic::transport::Error>
where
    E: RegionSnapshotEngine + MetadataCommandExecutor + RaftCommandExecutor,
{
    serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_topology_publisher_and_restart_diagnostics(
        addr,
        region,
        admission,
        peer_endpoints,
        EmptyRegionDescriptorSink,
        Arc::new(EmptyTopologyPublisher),
        Arc::new(EmptyRestartDiagnostics),
    )
    .await
}

pub fn openraft_region_service_pair<E, D>(
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
    descriptor_sink: D,
    topology_publisher: Arc<dyn TopologyPublisher>,
    restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
) -> (
    StoreKvService<nokv_raftnode::OpenRaftRegion<E>>,
    MetadataPlaneService<nokv_raftnode::OpenRaftRegion<E>>,
    RaftAdminService<nokv_raftnode::OpenRaftRegion<E>, D>,
)
where
    E: RegionSnapshotEngine + MetadataCommandExecutor + RaftCommandExecutor,
    D: RegionDescriptorSink,
{
    let execution = ExecutionRuntime::default();
    let admission = RegionAdmissionState::new(admission);
    let store = StoreKvService::with_admission_state_and_execution(
        region.clone(),
        admission.clone(),
        execution.clone(),
    );
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
    (store, metadata, admin)
}

pub async fn serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_topology_publisher_and_restart_diagnostics<
    E,
    D,
>(
    addr: SocketAddr,
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
    descriptor_sink: D,
    topology_publisher: Arc<dyn TopologyPublisher>,
    restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
) -> Result<(), tonic::transport::Error>
where
    E: RegionSnapshotEngine + MetadataCommandExecutor + RaftCommandExecutor,
    D: RegionDescriptorSink,
{
    let transport = nokv_raftnode::TonicRaftTransportRegistry::default();
    transport.register(admission.region_id, region.raft_handle());
    let (store, metadata, admin) = openraft_region_service_pair(
        region,
        admission,
        peer_endpoints,
        descriptor_sink,
        topology_publisher,
        restart_diagnostics,
    );
    tonic::transport::Server::builder()
        .add_service(StoreKvServer::new(store))
        .add_service(MetadataPlaneServer::new(metadata))
        .add_service(RaftAdminServer::new(admin))
        .add_service(nokv_raftnode::RaftTransportServer::new(transport.service()))
        .serve(addr)
        .await
}
