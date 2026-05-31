use std::net::SocketAddr;
use std::sync::Arc;

use nokv_mvcc::{KvEngine, MvccStore};
use nokv_raftnode::{
    ApplyStatusProvider, ApplyWatchProvider, RaftCommandExecutor, RegionSnapshotEngine,
};

use crate::execution::ExecutionRuntime;
use crate::service::RegionAdmissionState;
use crate::{
    AppliedRegionDescriptorProvider, EmptyRegionDescriptorSink, EmptyRestartDiagnostics,
    EmptyTopologyPublisher, PeerEndpointCatalog, RaftAdminServer, RaftAdminService,
    RaftMembershipAdmin, RaftRuntimeStatusProvider, RegionAdmission, RegionDescriptorSink,
    RestartDiagnosticsProvider, StoreKvServer, StoreKvService, TopologyPublisher,
};

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
    E: AppliedRegionDescriptorProvider
        + RaftCommandExecutor
        + ApplyStatusProvider
        + ApplyWatchProvider
        + RaftMembershipAdmin
        + RaftRuntimeStatusProvider,
{
    serve_with_region_engine_and_admission(addr, engine, RegionAdmission::default()).await
}

pub async fn serve_with_region_engine_and_admission<E>(
    addr: SocketAddr,
    engine: E,
    admission: RegionAdmission,
) -> Result<(), tonic::transport::Error>
where
    E: AppliedRegionDescriptorProvider
        + RaftCommandExecutor
        + ApplyStatusProvider
        + ApplyWatchProvider
        + RaftMembershipAdmin
        + RaftRuntimeStatusProvider,
{
    let execution = ExecutionRuntime::default();
    let admission = RegionAdmissionState::new(admission);
    tonic::transport::Server::builder()
        .add_service(StoreKvServer::new(
            StoreKvService::with_admission_state_and_execution(
                engine.clone(),
                admission.clone(),
                execution.clone(),
            ),
        ))
        .add_service(RaftAdminServer::new(
            RaftAdminService::with_admission_state_and_execution(engine, admission, execution),
        ))
        .serve(addr)
        .await
}

pub async fn serve_with_openraft_region_and_admission<E>(
    addr: SocketAddr,
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
) -> Result<(), tonic::transport::Error>
where
    E: RegionSnapshotEngine + RaftCommandExecutor,
{
    serve_with_openraft_region_admission_and_peer_endpoints(
        addr,
        region,
        admission,
        PeerEndpointCatalog::default(),
    )
    .await
}

pub async fn serve_with_openraft_region_admission_and_peer_endpoints<E>(
    addr: SocketAddr,
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
) -> Result<(), tonic::transport::Error>
where
    E: RegionSnapshotEngine + RaftCommandExecutor,
{
    serve_with_openraft_region_admission_peer_endpoints_and_descriptor_sink(
        addr,
        region,
        admission,
        peer_endpoints,
        EmptyRegionDescriptorSink,
    )
    .await
}

pub async fn serve_with_openraft_region_admission_peer_endpoints_and_descriptor_sink<E, D>(
    addr: SocketAddr,
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
    descriptor_sink: D,
) -> Result<(), tonic::transport::Error>
where
    E: RegionSnapshotEngine + RaftCommandExecutor,
    D: RegionDescriptorSink,
{
    serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_and_topology_publisher(
        addr,
        region,
        admission,
        peer_endpoints,
        descriptor_sink,
        Arc::new(EmptyTopologyPublisher),
    )
    .await
}

pub async fn serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_and_topology_publisher<
    E,
    D,
>(
    addr: SocketAddr,
    region: nokv_raftnode::OpenRaftRegion<E>,
    admission: RegionAdmission,
    peer_endpoints: PeerEndpointCatalog,
    descriptor_sink: D,
    topology_publisher: Arc<dyn TopologyPublisher>,
) -> Result<(), tonic::transport::Error>
where
    E: RegionSnapshotEngine + RaftCommandExecutor,
    D: RegionDescriptorSink,
{
    serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_topology_publisher_and_restart_diagnostics(
        addr,
        region,
        admission,
        peer_endpoints,
        descriptor_sink,
        topology_publisher,
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
    RaftAdminService<nokv_raftnode::OpenRaftRegion<E>, D>,
)
where
    E: RegionSnapshotEngine + RaftCommandExecutor,
    D: RegionDescriptorSink,
{
    let execution = ExecutionRuntime::default();
    let admission = RegionAdmissionState::new(admission);
    let store = StoreKvService::with_admission_state_and_execution(
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
    (store, admin)
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
    E: RegionSnapshotEngine + RaftCommandExecutor,
    D: RegionDescriptorSink,
{
    let transport = nokv_raftnode::TonicRaftTransportRegistry::default();
    transport.register(admission.region_id, region.raft_handle());
    let (store, admin) = openraft_region_service_pair(
        region,
        admission,
        peer_endpoints,
        descriptor_sink,
        topology_publisher,
        restart_diagnostics,
    );
    tonic::transport::Server::builder()
        .add_service(StoreKvServer::new(store))
        .add_service(RaftAdminServer::new(admin))
        .add_service(nokv_raftnode::RaftTransportServer::new(transport.service()))
        .serve(addr)
        .await
}
