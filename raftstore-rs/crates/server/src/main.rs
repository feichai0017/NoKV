//! Standalone Rust raftstore server entrypoint for local compatibility tests.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nokv_holtstore::HoltMvccStore;
use nokv_mvcc::MvccStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{
    AppliedKvEngine, ApplyStatusProvider, OpenRaftRegion, PersistentAppliedKvEngine,
    RegionLogStorage, RegionSnapshotEngine, RegionStateMachine, SegmentedEntryLog,
    TonicRaftNetworkFactory,
};
use nokv_raftstore_server::{
    apply_status_from_holt, EmptyRegionDescriptorSink, EmptyTopologyPublisher,
    HoltRegionMetadataSink, PeerEndpointCatalog, RegionAdmission, TopologyPublishOutcome,
    TopologyPublisher,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("NOKV_RUST_RAFTSTORE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:23880".to_owned())
        .parse::<SocketAddr>()?;
    let identity = ServerIdentity::from_env()?;
    let coordinator = coordinator_heartbeat_config_from_env()?;
    let peer_endpoints = peer_endpoint_catalog_from_env()?;
    let mut temp_log_dir = None;
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_HOLT_DIR") {
        tracing::info!(%addr, %path, "starting rust raftstore server with Holt MVCC");
        let log_dir = raft_log_dir(Some(PathBuf::from(&path)), &mut temp_log_dir)?;
        let mvcc = HoltMvccStore::open_file(path)?;
        let descriptor =
            mvcc.load_or_bootstrap_region_descriptor(&default_region_descriptor(identity))?;
        let admission = RegionAdmission::from_descriptor(&descriptor, identity.bootstrap)?;
        let apply_status = mvcc
            .get_region_apply_state(descriptor.region_id)?
            .map(apply_status_from_holt)
            .unwrap_or(nokv_raftnode::ApplyStatus {
                region_id: descriptor.region_id,
                term: 1,
                applied_index: 0,
            });
        let engine = AppliedKvEngine::with_status(apply_status, mvcc.clone());
        engine.set_region_descriptor(descriptor.clone())?;
        let engine =
            PersistentAppliedKvEngine::new(engine, HoltRegionMetadataSink::new(mvcc.clone()));
        let region = open_openraft_region(identity, addr, log_dir, engine).await?;
        spawn_coordinator_heartbeat(coordinator.clone(), identity, addr, region.clone());
        nokv_raftstore_server::serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_and_topology_publisher(
            addr,
            region,
            admission,
            peer_endpoints,
            HoltRegionMetadataSink::new(mvcc),
            coordinator_topology_publisher(coordinator),
        )
        .await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore server with in-memory MVCC");
        let log_dir = raft_log_dir(None, &mut temp_log_dir)?;
        let engine = AppliedKvEngine::new(identity.region_id, MvccStore::new());
        engine.set_region_descriptor(default_region_descriptor(identity))?;
        let region = open_openraft_region(identity, addr, log_dir, engine).await?;
        spawn_coordinator_heartbeat(coordinator.clone(), identity, addr, region.clone());
        nokv_raftstore_server::serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_and_topology_publisher(
            addr,
            region,
            RegionAdmission::from_descriptor(
                &default_region_descriptor(identity),
                identity.bootstrap,
            )?,
            peer_endpoints,
            EmptyRegionDescriptorSink,
            coordinator_topology_publisher(coordinator),
        )
        .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServerIdentity {
    region_id: u64,
    store_id: u64,
    peer_id: u64,
    bootstrap: bool,
}

impl Default for ServerIdentity {
    fn default() -> Self {
        Self {
            region_id: 1,
            store_id: 1,
            peer_id: 1,
            bootstrap: true,
        }
    }
}

impl ServerIdentity {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Self::from_values(
            std::env::var("NOKV_RUST_RAFTSTORE_REGION_ID").ok(),
            std::env::var("NOKV_RUST_RAFTSTORE_STORE_ID").ok(),
            std::env::var("NOKV_RUST_RAFTSTORE_PEER_ID").ok(),
            std::env::var("NOKV_RUST_RAFTSTORE_BOOTSTRAP").ok(),
        )
    }

    fn from_values(
        region_id: Option<String>,
        store_id: Option<String>,
        peer_id: Option<String>,
        bootstrap: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let default = Self::default();
        Ok(Self {
            region_id: parse_required_nonzero_u64(
                "NOKV_RUST_RAFTSTORE_REGION_ID",
                region_id,
                default.region_id,
            )?,
            store_id: parse_required_nonzero_u64(
                "NOKV_RUST_RAFTSTORE_STORE_ID",
                store_id,
                default.store_id,
            )?,
            peer_id: parse_required_nonzero_u64(
                "NOKV_RUST_RAFTSTORE_PEER_ID",
                peer_id,
                default.peer_id,
            )?,
            bootstrap: parse_bootstrap_flag(bootstrap, default.bootstrap)?,
        })
    }
}

fn parse_required_nonzero_u64(
    name: &str,
    value: Option<String>,
    default: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        return Err(format!("{name} must be non-zero").into());
    }
    Ok(parsed)
}

fn parse_bootstrap_flag(
    value: Option<String>,
    default: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err("NOKV_RUST_RAFTSTORE_BOOTSTRAP must be true or false".into()),
    }
}

fn peer_endpoint_catalog_from_env() -> Result<PeerEndpointCatalog, Box<dyn std::error::Error>> {
    let catalog = PeerEndpointCatalog::require_configured();
    let Ok(raw) = std::env::var("NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS") else {
        return Ok(catalog);
    };
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (peer_id, endpoint) = item.split_once('=').ok_or_else(|| {
            format!("invalid NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS entry {item:?}: expected peer_id=endpoint")
        })?;
        catalog.insert_peer(peer_id.parse()?, endpoint.to_owned())?;
    }
    Ok(catalog)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoordinatorHeartbeatConfig {
    endpoint: String,
    interval: Duration,
}

fn coordinator_heartbeat_config_from_env(
) -> Result<Option<CoordinatorHeartbeatConfig>, Box<dyn std::error::Error>> {
    let Ok(raw_addr) = std::env::var("NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR") else {
        return Ok(None);
    };
    let addr = raw_addr.trim();
    if addr.is_empty() {
        return Ok(None);
    }
    let interval_ms = parse_required_nonzero_u64(
        "NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS",
        std::env::var("NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS").ok(),
        1_000,
    )?;
    Ok(Some(CoordinatorHeartbeatConfig {
        endpoint: coordinator_endpoint(addr),
        interval: Duration::from_millis(interval_ms),
    }))
}

fn coordinator_endpoint(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_owned()
    } else {
        format!("http://{addr}")
    }
}

#[derive(Debug, Clone)]
struct CoordinatorTopologyPublisher {
    endpoint: String,
}

fn coordinator_topology_publisher(
    config: Option<CoordinatorHeartbeatConfig>,
) -> Arc<dyn TopologyPublisher> {
    config
        .map(|config| {
            Arc::new(CoordinatorTopologyPublisher {
                endpoint: config.endpoint,
            }) as Arc<dyn TopologyPublisher>
        })
        .unwrap_or_else(|| Arc::new(EmptyTopologyPublisher))
}

#[tonic::async_trait]
impl TopologyPublisher for CoordinatorTopologyPublisher {
    async fn publish_peer_added(
        &self,
        region_id: u64,
        store_id: u64,
        peer_id: u64,
        region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        self.publish_peer_change(
            metapb::RootEventKind::PeerAdded,
            region_id,
            store_id,
            peer_id,
            region,
        )
        .await
    }

    async fn publish_peer_removed(
        &self,
        region_id: u64,
        store_id: u64,
        peer_id: u64,
        region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        self.publish_peer_change(
            metapb::RootEventKind::PeerRemoved,
            region_id,
            store_id,
            peer_id,
            region,
        )
        .await
    }
}

impl CoordinatorTopologyPublisher {
    async fn publish_peer_change(
        &self,
        kind: metapb::RootEventKind,
        region_id: u64,
        store_id: u64,
        peer_id: u64,
        region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        let mut client =
            match coordpb::coordinator_client::CoordinatorClient::connect(self.endpoint.clone())
                .await
            {
                Ok(client) => client,
                Err(err) => return TopologyPublishOutcome::terminal_failed(err.to_string()),
            };
        match client
            .publish_root_event(coordpb::PublishRootEventRequest {
                event: Some(metapb::RootEvent {
                    kind: kind as i32,
                    payload: Some(metapb::root_event::Payload::PeerChange(
                        metapb::RootPeerChange {
                            region_id,
                            store_id,
                            peer_id,
                            target: Some(region.clone()),
                            ..Default::default()
                        },
                    )),
                }),
                ..Default::default()
            })
            .await
        {
            Ok(_) => TopologyPublishOutcome::terminal_published(),
            Err(status) => TopologyPublishOutcome::terminal_failed(status.to_string()),
        }
    }
}

fn spawn_coordinator_heartbeat<E>(
    config: Option<CoordinatorHeartbeatConfig>,
    identity: ServerIdentity,
    addr: SocketAddr,
    region: OpenRaftRegion<E>,
) where
    E: RegionSnapshotEngine + Send + Sync + 'static,
{
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        run_coordinator_heartbeat(config, identity, addr, region).await;
    });
}

async fn run_coordinator_heartbeat<E>(
    config: CoordinatorHeartbeatConfig,
    identity: ServerIdentity,
    addr: SocketAddr,
    region: OpenRaftRegion<E>,
) where
    E: RegionSnapshotEngine + Send + Sync + 'static,
{
    let mut ticker = tokio::time::interval(config.interval);
    let admin_endpoint = local_admin_endpoint(addr);
    loop {
        ticker.tick().await;
        let request = coordinator_heartbeat_request(identity, addr, &region);
        match coordpb::coordinator_client::CoordinatorClient::connect(config.endpoint.clone()).await
        {
            Ok(mut client) => match client.store_heartbeat(request).await {
                Ok(response) => {
                    let operations = response.into_inner().operations;
                    for operation in operations {
                        match execute_scheduler_operation(&admin_endpoint, operation).await {
                            Ok(true) => {
                                tracing::debug!("rust raftstore applied coordinator operation");
                            }
                            Ok(false) => {
                                tracing::debug!(
                                    "rust raftstore ignored unsupported coordinator operation"
                                );
                            }
                            Err(err) => {
                                tracing::debug!(
                                    error = %err,
                                    "rust raftstore coordinator operation failed"
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, "rust raftstore coordinator heartbeat failed");
                }
            },
            Err(err) => {
                tracing::debug!(error = %err, "rust raftstore coordinator connect failed");
            }
        }
    }
}

async fn execute_scheduler_operation(
    admin_endpoint: &str,
    operation: coordpb::SchedulerOperation,
) -> Result<bool, tonic::Status> {
    let kind = coordpb::SchedulerOperationType::try_from(operation.r#type)
        .unwrap_or(coordpb::SchedulerOperationType::None);
    match kind {
        coordpb::SchedulerOperationType::LeaderTransfer => {
            if operation.region_id == 0
                || operation.source_peer_id == 0
                || operation.target_peer_id == 0
            {
                return Ok(false);
            }
            let mut client =
                adminpb::raft_admin_client::RaftAdminClient::connect(admin_endpoint.to_owned())
                    .await
                    .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
            client
                .transfer_leader(adminpb::TransferLeaderRequest {
                    region_id: operation.region_id,
                    peer_id: operation.target_peer_id,
                })
                .await?;
            Ok(true)
        }
        coordpb::SchedulerOperationType::SplitRegion
        | coordpb::SchedulerOperationType::MergeRegion
        | coordpb::SchedulerOperationType::None => Ok(false),
    }
}

fn local_admin_endpoint(addr: SocketAddr) -> String {
    if addr.ip().is_unspecified() {
        format!("http://127.0.0.1:{}", addr.port())
    } else {
        format!("http://{addr}")
    }
}

fn coordinator_heartbeat_request<E>(
    identity: ServerIdentity,
    addr: SocketAddr,
    region: &OpenRaftRegion<E>,
) -> coordpb::StoreHeartbeatRequest
where
    E: RegionSnapshotEngine,
{
    let status = region.apply_status();
    let leader_peer_id = region
        .raft_handle()
        .metrics()
        .borrow()
        .current_leader
        .unwrap_or_default();
    let known = status.region_id != 0;
    let leader = known && leader_peer_id == identity.peer_id;
    coordpb::StoreHeartbeatRequest {
        store_id: identity.store_id,
        region_num: u64::from(known),
        leader_num: u64::from(leader),
        leader_region_ids: if leader {
            vec![status.region_id]
        } else {
            Vec::new()
        },
        client_addr: addr.to_string(),
        raft_addr: addr.to_string(),
        region_stats: known
            .then(|| coordpb::RegionRuntimeStats {
                region_id: status.region_id,
                leader_store_id: if leader { identity.store_id } else { 0 },
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

async fn open_openraft_region<E>(
    identity: ServerIdentity,
    addr: SocketAddr,
    log_dir: PathBuf,
    engine: E,
) -> Result<OpenRaftRegion<E>, Box<dyn std::error::Error>>
where
    E: RegionSnapshotEngine,
{
    let log = SegmentedEntryLog::open(identity.region_id, log_dir)?;
    let state_machine = RegionStateMachine::new(engine);
    if identity.bootstrap {
        return Ok(OpenRaftRegion::bootstrap_single_node_with_network(
            identity.peer_id,
            identity.region_id,
            RegionLogStorage::new(log),
            state_machine,
            TonicRaftNetworkFactory::new(identity.region_id),
            addr.to_string(),
        )
        .await?);
    }
    Ok(OpenRaftRegion::open_with_network(
        identity.peer_id,
        identity.region_id,
        RegionLogStorage::new(log),
        state_machine,
        TonicRaftNetworkFactory::new(identity.region_id),
    )
    .await?)
}

fn raft_log_dir(
    persistent_root: Option<PathBuf>,
    temp_log_dir: &mut Option<tempfile::TempDir>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_LOG_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(root) = persistent_root {
        return Ok(root.join("raftlog"));
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_path_buf();
    *temp_log_dir = Some(dir);
    Ok(path)
}

fn default_region_descriptor(identity: ServerIdentity) -> metapb::RegionDescriptor {
    metapb::RegionDescriptor {
        region_id: identity.region_id,
        epoch: Some(metapb::RegionEpoch {
            version: 1,
            conf_version: 1,
        }),
        peers: vec![metapb::RegionPeer {
            store_id: identity.store_id,
            peer_id: identity.peer_id,
        }],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tonic::{Request, Response, Status};

    #[derive(Clone, Default)]
    struct CaptureRaftAdmin {
        transfers: Arc<Mutex<Vec<adminpb::TransferLeaderRequest>>>,
    }

    #[tonic::async_trait]
    impl adminpb::raft_admin_server::RaftAdmin for CaptureRaftAdmin {
        async fn add_peer(
            &self,
            _request: Request<adminpb::AddPeerRequest>,
        ) -> Result<Response<adminpb::AddPeerResponse>, Status> {
            Err(Status::unimplemented("add peer is not used by this test"))
        }

        async fn remove_peer(
            &self,
            _request: Request<adminpb::RemovePeerRequest>,
        ) -> Result<Response<adminpb::RemovePeerResponse>, Status> {
            Err(Status::unimplemented(
                "remove peer is not used by this test",
            ))
        }

        async fn transfer_leader(
            &self,
            request: Request<adminpb::TransferLeaderRequest>,
        ) -> Result<Response<adminpb::TransferLeaderResponse>, Status> {
            let request = request.into_inner();
            self.transfers.lock().unwrap().push(request.clone());
            Ok(Response::new(adminpb::TransferLeaderResponse {
                region: Some(metapb::RegionDescriptor {
                    region_id: request.region_id,
                    ..Default::default()
                }),
            }))
        }

        async fn region_runtime_status(
            &self,
            _request: Request<adminpb::RegionRuntimeStatusRequest>,
        ) -> Result<Response<adminpb::RegionRuntimeStatusResponse>, Status> {
            Err(Status::unimplemented(
                "region runtime status is not used by this test",
            ))
        }

        async fn execution_status(
            &self,
            _request: Request<adminpb::ExecutionStatusRequest>,
        ) -> Result<Response<adminpb::ExecutionStatusResponse>, Status> {
            Err(Status::unimplemented(
                "execution status is not used by this test",
            ))
        }
    }

    #[test]
    fn server_identity_defaults_to_single_node_bootstrap() {
        assert_eq!(
            ServerIdentity::from_values(None, None, None, None).unwrap(),
            ServerIdentity::default()
        );
    }

    #[test]
    fn server_identity_parses_joining_peer() {
        let identity = ServerIdentity::from_values(
            Some("9".to_owned()),
            Some("12".to_owned()),
            Some("34".to_owned()),
            Some("false".to_owned()),
        )
        .unwrap();
        assert_eq!(
            identity,
            ServerIdentity {
                region_id: 9,
                store_id: 12,
                peer_id: 34,
                bootstrap: false,
            }
        );
    }

    #[test]
    fn server_identity_rejects_zero_peer() {
        let err = ServerIdentity::from_values(None, None, Some("0".to_owned()), None).unwrap_err();
        assert!(err.to_string().contains("NOKV_RUST_RAFTSTORE_PEER_ID"));
    }

    #[test]
    fn server_identity_rejects_invalid_bootstrap_flag() {
        let err =
            ServerIdentity::from_values(None, None, None, Some("maybe".to_owned())).unwrap_err();
        assert!(err.to_string().contains("NOKV_RUST_RAFTSTORE_BOOTSTRAP"));
    }

    #[test]
    fn coordinator_endpoint_adds_http_scheme_for_host_port() {
        assert_eq!(
            coordinator_endpoint("127.0.0.1:23790"),
            "http://127.0.0.1:23790"
        );
        assert_eq!(
            coordinator_endpoint("http://127.0.0.1:23790"),
            "http://127.0.0.1:23790"
        );
    }

    #[test]
    fn local_admin_endpoint_uses_loopback_for_unspecified_bind_addr() {
        let addr: SocketAddr = "0.0.0.0:23880".parse().unwrap();
        assert_eq!(local_admin_endpoint(addr), "http://127.0.0.1:23880");
    }

    #[tokio::test]
    async fn scheduler_operation_executes_leader_transfer_via_admin_rpc() {
        let addr = reserve_loopback_addr();
        let admin = CaptureRaftAdmin::default();
        let transfers = admin.transfers.clone();
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(adminpb::raft_admin_server::RaftAdminServer::new(admin))
                .serve(addr)
                .await
                .unwrap();
        });
        wait_for_server(addr).await;

        let applied = execute_scheduler_operation(
            &local_admin_endpoint(addr),
            coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
                region_id: 7,
                source_peer_id: 101,
                target_peer_id: 202,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(applied);
        let captured = transfers.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].region_id, 7);
        assert_eq!(captured[0].peer_id, 202);
        handle.abort();
    }

    #[tokio::test]
    async fn scheduler_operation_ignores_unsupported_split_without_dialing_admin() {
        let applied = execute_scheduler_operation(
            "http://127.0.0.1:1",
            coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
                region_id: 7,
                split_key: b"k".to_vec(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(!applied);
    }

    #[tokio::test]
    async fn non_bootstrap_start_opens_joining_peer_without_initializing_membership() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
            bootstrap: false,
        };
        let region = open_openraft_region(
            identity,
            "127.0.0.1:0".parse().unwrap(),
            dir.path().to_path_buf(),
            AppliedKvEngine::new(identity.region_id, MvccStore::new()),
        )
        .await
        .unwrap();
        let metrics = region.raft_handle().metrics().borrow().clone();
        assert!(metrics.current_leader.is_none());
        assert!(metrics.membership_config.voter_ids().next().is_none());
    }

    #[tokio::test]
    async fn coordinator_heartbeat_reports_local_leader_region() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 11,
            peer_id: 101,
            bootstrap: true,
        };
        let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
        let region = open_openraft_region(
            identity,
            addr,
            dir.path().to_path_buf(),
            AppliedKvEngine::new(identity.region_id, MvccStore::new()),
        )
        .await
        .unwrap();

        let req = coordinator_heartbeat_request(identity, addr, &region);

        assert_eq!(req.store_id, 11);
        assert_eq!(req.region_num, 1);
        assert_eq!(req.leader_num, 1);
        assert_eq!(req.leader_region_ids, vec![7]);
        assert_eq!(req.client_addr, "127.0.0.1:23880");
        assert_eq!(req.raft_addr, "127.0.0.1:23880");
        assert_eq!(req.region_stats.len(), 1);
        assert_eq!(req.region_stats[0].region_id, 7);
        assert_eq!(req.region_stats[0].leader_store_id, 11);
    }

    fn reserve_loopback_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    async fn wait_for_server(addr: SocketAddr) {
        let endpoint = local_admin_endpoint(addr);
        for _ in 0..50 {
            if tonic::transport::Endpoint::from_shared(endpoint.clone())
                .unwrap()
                .connect()
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("rust raftstore test server at {addr} did not become ready");
    }
}
