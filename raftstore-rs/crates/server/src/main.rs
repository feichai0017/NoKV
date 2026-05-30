//! Standalone Rust raftstore server entrypoint for local compatibility tests.

use std::collections::HashMap;
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
    apply_status_from_holt, root_event_transition_id, EmptyRegionDescriptorSink,
    EmptyRestartDiagnostics, EmptyTopologyPublisher, HoltRegionMetadataSink, PeerEndpointCatalog,
    RaftRuntimeStatusProvider, RegionAdmission, TopologyPublishOutcome, TopologyPublisher,
};
use prost::Message;
use prost_types::Any;

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
        let descriptor = startup_region_descriptor(&mvcc, identity)?;
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
        spawn_startup_root_publication(
            coordinator.clone(),
            identity,
            descriptor.clone(),
            Some(mvcc.clone()),
        );
        spawn_coordinator_heartbeat(
            coordinator.clone(),
            identity,
            addr,
            region.clone(),
            Some(mvcc.clone()),
        );
        spawn_pending_topology_retries(coordinator.clone(), mvcc.clone());
        let topology_publisher = coordinator_topology_publisher(coordinator, Some(mvcc.clone()));
        nokv_raftstore_server::serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_topology_publisher_and_restart_diagnostics(
            addr,
            region,
            admission,
            peer_endpoints,
            HoltRegionMetadataSink::new(mvcc.clone()),
            topology_publisher,
            Arc::new(mvcc.clone()),
        )
        .await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore server with in-memory MVCC");
        let log_dir = raft_log_dir(None, &mut temp_log_dir)?;
        let engine = AppliedKvEngine::new(identity.region_id, MvccStore::new());
        let descriptor = default_region_descriptor(identity);
        engine.set_region_descriptor(descriptor.clone())?;
        let region = open_openraft_region(identity, addr, log_dir, engine).await?;
        spawn_startup_root_publication(coordinator.clone(), identity, descriptor.clone(), None);
        spawn_coordinator_heartbeat(coordinator.clone(), identity, addr, region.clone(), None);
        nokv_raftstore_server::serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_topology_publisher_and_restart_diagnostics(
            addr,
            region,
            RegionAdmission::from_descriptor(&descriptor, identity.bootstrap)?,
            peer_endpoints,
            EmptyRegionDescriptorSink,
            coordinator_topology_publisher(coordinator, None),
            Arc::new(EmptyRestartDiagnostics),
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
    endpoints: Vec<String>,
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
    let endpoints = coordinator_endpoints(addr);
    if endpoints.is_empty() {
        return Ok(None);
    }
    let interval_ms = parse_required_nonzero_u64(
        "NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS",
        std::env::var("NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS").ok(),
        1_000,
    )?;
    Ok(Some(CoordinatorHeartbeatConfig {
        endpoints,
        interval: Duration::from_millis(interval_ms),
    }))
}

fn coordinator_endpoints(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(coordinator_endpoint)
        .collect()
}

fn coordinator_endpoint(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_owned()
    } else {
        format!("http://{addr}")
    }
}

#[derive(Clone)]
struct CoordinatorTopologyPublisher {
    endpoints: Vec<String>,
    pending_store: Option<HoltMvccStore>,
}

fn coordinator_topology_publisher(
    config: Option<CoordinatorHeartbeatConfig>,
    pending_store: Option<HoltMvccStore>,
) -> Arc<dyn TopologyPublisher> {
    config
        .map(|config| {
            Arc::new(CoordinatorTopologyPublisher {
                endpoints: config.endpoints,
                pending_store,
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
        let event = metapb::RootEvent {
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
        };
        publish_root_event_with_pending(&self.endpoints, self.pending_store.as_ref(), event).await
    }
}

async fn publish_root_event_with_pending(
    endpoints: &[String],
    pending_store: Option<&HoltMvccStore>,
    event: metapb::RootEvent,
) -> TopologyPublishOutcome {
    let sequence = match pending_store {
        Some(store) => match store.enqueue_pending_root_event(&event) {
            Ok(sequence) => Some(sequence),
            Err(err) => {
                return TopologyPublishOutcome::terminal_failed(format!(
                    "persist pending root event: {err}"
                ))
            }
        },
        None => None,
    };
    match publish_root_event_to_any(endpoints, event.clone()).await {
        Ok(()) => {
            if let (Some(store), Some(sequence)) = (pending_store, sequence) {
                if let Err(err) = store.delete_pending_root_event(sequence) {
                    return TopologyPublishOutcome::terminal_failed(format!(
                        "delete pending root event {sequence}: {err}"
                    ));
                }
            }
            TopologyPublishOutcome::terminal_published()
        }
        Err(RootEventPublishError::Transient(message)) => {
            TopologyPublishOutcome::terminal_pending(message)
        }
        Err(RootEventPublishError::Permanent(message)) => {
            if let (Some(store), Some(sequence)) = (pending_store, sequence) {
                let transition_id = root_event_transition_id(&event);
                if let Err(block_err) =
                    store.block_pending_root_event(sequence, &event, &transition_id, &message)
                {
                    return TopologyPublishOutcome::terminal_failed(format!(
                        "block pending root event {sequence}: {block_err}"
                    ));
                }
            }
            TopologyPublishOutcome::terminal_blocked(message)
        }
    }
}

async fn publish_root_event_to_any(
    endpoints: &[String],
    event: metapb::RootEvent,
) -> Result<(), RootEventPublishError> {
    let mut last_transient = None;
    for endpoint in endpoints {
        match publish_root_event(endpoint, event.clone()).await {
            Ok(()) => return Ok(()),
            Err(RootEventPublishError::Permanent(message)) => {
                return Err(RootEventPublishError::Permanent(message));
            }
            Err(RootEventPublishError::Transient(message)) => {
                last_transient = Some(message);
            }
        }
    }
    Err(RootEventPublishError::Transient(
        last_transient.unwrap_or_else(|| "coordinator endpoints unavailable".to_owned()),
    ))
}

async fn publish_root_event(
    endpoint: &str,
    event: metapb::RootEvent,
) -> Result<(), RootEventPublishError> {
    let mut client = coordpb::coordinator_client::CoordinatorClient::connect(endpoint.to_owned())
        .await
        .map_err(|err| RootEventPublishError::Transient(err.to_string()))?;
    let response = client
        .publish_root_event(coordpb::PublishRootEventRequest {
            event: Some(event),
            ..Default::default()
        })
        .await
        .map_err(classify_root_event_publish_status)?
        .into_inner();
    if !response.accepted {
        return Err(RootEventPublishError::Permanent(
            "coordinator rejected root event".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RootEventPublishError {
    Transient(String),
    Permanent(String),
}

fn classify_root_event_publish_status(status: tonic::Status) -> RootEventPublishError {
    let message = status.to_string();
    match status.code() {
        tonic::Code::InvalidArgument => RootEventPublishError::Permanent(message),
        tonic::Code::FailedPrecondition => {
            let reason = root_event_status_metadata(&status)
                .and_then(|metadata| metadata.get(COORDINATOR_REASON_METADATA).cloned());
            match reason.as_deref() {
                Some(
                    "catalog_invalid"
                    | "catalog_precondition"
                    | "cluster_era_mismatch"
                    | "invalid_request",
                ) => RootEventPublishError::Permanent(message),
                Some(
                    "not_leader"
                    | "grant_not_held"
                    | "root_unavailable"
                    | "root_lag_exceeded"
                    | "required_rooted_token"
                    | "required_descriptor"
                    | "range_change_pending"
                    | "bootstrap_required"
                    | "root_storage_unavailable",
                ) => RootEventPublishError::Transient(message),
                _ => RootEventPublishError::Transient(message),
            }
        }
        tonic::Code::AlreadyExists => RootEventPublishError::Permanent(message),
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Aborted
        | tonic::Code::ResourceExhausted
        | tonic::Code::Internal
        | tonic::Code::Unknown => RootEventPublishError::Transient(message),
        _ => RootEventPublishError::Transient(message),
    }
}

const COORDINATOR_REASON_METADATA: &str = "coordinator_reason";
const NOKV_ERROR_INFO_DOMAIN: &str = "nokv";
const NOKV_ERROR_INFO_REASON: &str = "nokv_error";
const GOOGLE_RPC_ERROR_INFO_TYPE: &str = "type.googleapis.com/google.rpc.ErrorInfo";

#[derive(Clone, PartialEq, Message)]
struct RpcStatusDetails {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

#[derive(Clone, PartialEq, Message)]
struct RpcErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: HashMap<String, String>,
}

fn root_event_status_metadata(status: &tonic::Status) -> Option<HashMap<String, String>> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    let details = RpcStatusDetails::decode(details).ok()?;
    for detail in details.details {
        if detail.type_url != GOOGLE_RPC_ERROR_INFO_TYPE {
            continue;
        }
        let info = RpcErrorInfo::decode(detail.value.as_slice()).ok()?;
        if info.domain == NOKV_ERROR_INFO_DOMAIN && info.reason == NOKV_ERROR_INFO_REASON {
            return Some(info.metadata);
        }
    }
    None
}

fn spawn_pending_topology_retries(
    config: Option<CoordinatorHeartbeatConfig>,
    pending_store: HoltMvccStore,
) {
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        run_pending_topology_retries(config, pending_store).await;
    });
}

fn spawn_startup_root_publication(
    config: Option<CoordinatorHeartbeatConfig>,
    identity: ServerIdentity,
    descriptor: metapb::RegionDescriptor,
    pending_store: Option<HoltMvccStore>,
) {
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        for event in startup_root_events(identity, descriptor) {
            let outcome =
                publish_root_event_with_pending(&config.endpoints, pending_store.as_ref(), event)
                    .await;
            if outcome.publish_state() == adminpb::ExecutionPublishState::TerminalPublished {
                continue;
            }
            tracing::debug!(
                publish = ?outcome.publish_state(),
                error = %outcome.last_error(),
                "rust raftstore startup root publication deferred"
            );
        }
    });
}

fn startup_root_events(
    identity: ServerIdentity,
    descriptor: metapb::RegionDescriptor,
) -> Vec<metapb::RootEvent> {
    let mut events = vec![metapb::RootEvent {
        kind: metapb::RootEventKind::StoreJoined as i32,
        payload: Some(metapb::root_event::Payload::StoreMembership(
            metapb::RootStoreMembership {
                store_id: identity.store_id,
            },
        )),
    }];
    if identity.bootstrap {
        events.push(metapb::RootEvent {
            kind: metapb::RootEventKind::RegionBootstrap as i32,
            payload: Some(metapb::root_event::Payload::RegionDescriptor(
                metapb::RootRegionDescriptor {
                    descriptor: Some(descriptor),
                },
            )),
        });
    }
    events
}

async fn run_pending_topology_retries(
    config: CoordinatorHeartbeatConfig,
    pending_store: HoltMvccStore,
) {
    let mut ticker = tokio::time::interval(config.interval);
    loop {
        ticker.tick().await;
        retry_pending_topology_events(&config.endpoints, &pending_store).await;
    }
}

async fn retry_pending_topology_events(endpoints: &[String], pending_store: &HoltMvccStore) {
    let pending = match pending_store.pending_root_events() {
        Ok(pending) => pending,
        Err(err) => {
            tracing::debug!(error = %err, "rust raftstore pending topology load failed");
            return;
        }
    };
    for item in pending {
        match publish_root_event_to_any(endpoints, item.event.clone()).await {
            Ok(()) => {
                if let Err(err) = pending_store.delete_pending_root_event(item.sequence) {
                    tracing::debug!(
                        error = %err,
                        sequence = item.sequence,
                        "rust raftstore pending topology delete failed"
                    );
                    return;
                }
            }
            Err(RootEventPublishError::Permanent(err)) => {
                let transition_id = root_event_transition_id(&item.event);
                if let Err(block_err) = pending_store.block_pending_root_event(
                    item.sequence,
                    &item.event,
                    &transition_id,
                    &err,
                ) {
                    tracing::debug!(
                        error = %block_err,
                        sequence = item.sequence,
                        "rust raftstore pending topology block failed"
                    );
                    return;
                }
                tracing::debug!(
                    error = %err,
                    sequence = item.sequence,
                    "rust raftstore pending topology blocked"
                );
            }
            Err(RootEventPublishError::Transient(err)) => {
                tracing::debug!(
                    error = %err,
                    sequence = item.sequence,
                    "rust raftstore pending topology publish failed"
                );
                return;
            }
        }
    }
}

fn spawn_coordinator_heartbeat<E>(
    config: Option<CoordinatorHeartbeatConfig>,
    identity: ServerIdentity,
    addr: SocketAddr,
    region: OpenRaftRegion<E>,
    root_events: Option<HoltMvccStore>,
) where
    E: RegionSnapshotEngine + Send + Sync + 'static,
{
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        run_coordinator_heartbeat(config, identity, addr, region, root_events).await;
    });
}

async fn run_coordinator_heartbeat<E>(
    config: CoordinatorHeartbeatConfig,
    identity: ServerIdentity,
    addr: SocketAddr,
    region: OpenRaftRegion<E>,
    root_events: Option<HoltMvccStore>,
) where
    E: RegionSnapshotEngine + Send + Sync + 'static,
{
    let mut ticker = tokio::time::interval(config.interval);
    let admin_endpoint = local_admin_endpoint(addr);
    loop {
        ticker.tick().await;
        let request = coordinator_heartbeat_request(identity, addr, &region, root_events.as_ref());
        match send_store_heartbeat(&config.endpoints, request).await {
            Ok(operations) => {
                for operation in operations {
                    match execute_scheduler_operation(&admin_endpoint, &operation).await {
                        Ok(SchedulerOperationOutcome::Applied) => {
                            tracing::debug!("rust raftstore applied coordinator operation");
                        }
                        Ok(SchedulerOperationOutcome::Invalid { reason }) => {
                            tracing::debug!(
                                %reason,
                                "rust raftstore ignored invalid coordinator operation"
                            );
                        }
                        Ok(SchedulerOperationOutcome::Unsupported { kind, reason }) => {
                            record_pending_scheduler_operation(root_events.as_ref(), &operation);
                            tracing::warn!(
                                ?kind,
                                %reason,
                                region_id = operation.region_id,
                                source_peer_id = operation.source_peer_id,
                                target_peer_id = operation.target_peer_id,
                                source_region_id = operation.source_region_id,
                                split_key_len = operation.split_key.len(),
                                "rust raftstore received unsupported coordinator operation"
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
        }
    }
}

async fn send_store_heartbeat(
    endpoints: &[String],
    request: coordpb::StoreHeartbeatRequest,
) -> Result<Vec<coordpb::SchedulerOperation>, String> {
    send_store_heartbeat_with(endpoints, request, |endpoint, request| async move {
        match coordpb::coordinator_client::CoordinatorClient::connect(endpoint.clone()).await {
            Ok(mut client) => client
                .store_heartbeat(request)
                .await
                .map(|response| response.into_inner())
                .map_err(|err| err.to_string()),
            Err(err) => Err(err.to_string()),
        }
    })
    .await
}

async fn send_store_heartbeat_with<F, Fut>(
    endpoints: &[String],
    request: coordpb::StoreHeartbeatRequest,
    mut send: F,
) -> Result<Vec<coordpb::SchedulerOperation>, String>
where
    F: FnMut(String, coordpb::StoreHeartbeatRequest) -> Fut,
    Fut: std::future::Future<Output = Result<coordpb::StoreHeartbeatResponse, String>>,
{
    let mut first_success = None;
    let mut first_operational_success = None;
    let mut last_error = None;
    for endpoint in endpoints {
        match send(endpoint.clone(), request.clone()).await {
            Ok(response) => {
                let operations = response.operations;
                if first_success.is_none() {
                    first_success = Some(operations.clone());
                }
                if first_operational_success.is_none() && !operations.is_empty() {
                    first_operational_success = Some(operations);
                }
            }
            Err(err) => {
                last_error = Some(err);
            }
        }
    }
    if let Some(operations) = first_operational_success {
        return Ok(operations);
    }
    if let Some(operations) = first_success {
        return Ok(operations);
    }
    Err(last_error.unwrap_or_else(|| "coordinator endpoints unavailable".to_owned()))
}

fn record_pending_scheduler_operation(
    store: Option<&HoltMvccStore>,
    operation: &coordpb::SchedulerOperation,
) {
    let Some(store) = store else {
        return;
    };
    if let Err(err) = store.record_pending_scheduler_operation(operation) {
        tracing::warn!(
            error = %err,
            region_id = operation.region_id,
            operation_type = operation.r#type,
            "rust raftstore failed to persist pending scheduler operation"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SchedulerOperationOutcome {
    Applied,
    Invalid {
        reason: &'static str,
    },
    Unsupported {
        kind: coordpb::SchedulerOperationType,
        reason: &'static str,
    },
}

async fn execute_scheduler_operation(
    admin_endpoint: &str,
    operation: &coordpb::SchedulerOperation,
) -> Result<SchedulerOperationOutcome, tonic::Status> {
    let kind = coordpb::SchedulerOperationType::try_from(operation.r#type)
        .unwrap_or(coordpb::SchedulerOperationType::None);
    match kind {
        coordpb::SchedulerOperationType::LeaderTransfer => {
            if operation.region_id == 0
                || operation.source_peer_id == 0
                || operation.target_peer_id == 0
            {
                return Ok(SchedulerOperationOutcome::Invalid {
                    reason: "leader transfer requires region, source peer, and target peer",
                });
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
            Ok(SchedulerOperationOutcome::Applied)
        }
        coordpb::SchedulerOperationType::SplitRegion => {
            if operation.region_id == 0
                || operation.split_key.is_empty()
                || operation
                    .split_child
                    .as_ref()
                    .is_none_or(|child| child.region_id == 0)
            {
                return Ok(SchedulerOperationOutcome::Invalid {
                    reason: "split requires region, split key, and child descriptor",
                });
            }
            Ok(SchedulerOperationOutcome::Unsupported {
                kind,
                reason: "split execution is not implemented in raftstore-rs yet",
            })
        }
        coordpb::SchedulerOperationType::MergeRegion => {
            if operation.region_id == 0 || operation.source_region_id == 0 {
                return Ok(SchedulerOperationOutcome::Invalid {
                    reason: "merge requires target region and source region",
                });
            }
            Ok(SchedulerOperationOutcome::Unsupported {
                kind,
                reason: "merge execution is not implemented in raftstore-rs yet",
            })
        }
        coordpb::SchedulerOperationType::None => Ok(SchedulerOperationOutcome::Invalid {
            reason: "scheduler operation type is none",
        }),
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
    root_events: Option<&HoltMvccStore>,
) -> coordpb::StoreHeartbeatRequest
where
    E: RegionSnapshotEngine,
{
    let status = region.apply_status();
    let runtime = region.raft_runtime_status();
    let known = status.region_id != 0 && runtime.hosted;
    let leader = known && runtime.leader;
    let pending_admin = root_events
        .map(topology_catalog_has_pending_admin_work)
        .unwrap_or(false);
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
                pending_admin,
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

fn topology_catalog_has_pending_admin_work(store: &HoltMvccStore) -> bool {
    let pending = store
        .pending_root_events()
        .map(|events| !events.is_empty())
        .unwrap_or(true);
    let blocked = store
        .blocked_root_events()
        .map(|events| !events.is_empty())
        .unwrap_or(true);
    let scheduler = store
        .pending_scheduler_operations()
        .map(|ops| !ops.is_empty())
        .unwrap_or(true);
    pending || blocked || scheduler
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

fn startup_region_descriptor(
    store: &HoltMvccStore,
    identity: ServerIdentity,
) -> nokv_holtstore::Result<metapb::RegionDescriptor> {
    let default = default_region_descriptor(identity);
    if identity.bootstrap {
        return store.load_or_bootstrap_region_descriptor(&default);
    }
    Ok(store
        .get_region_descriptor(identity.region_id)?
        .unwrap_or(default))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
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
    fn coordinator_endpoints_split_comma_separated_addresses() {
        assert_eq!(
            coordinator_endpoints("127.0.0.1:23790, http://127.0.0.1:23791 ,,127.0.0.1:23792"),
            vec![
                "http://127.0.0.1:23790".to_owned(),
                "http://127.0.0.1:23791".to_owned(),
                "http://127.0.0.1:23792".to_owned(),
            ]
        );
    }

    #[test]
    fn root_event_publish_status_classifies_coordinator_error_info() {
        assert!(matches!(
            classify_root_event_publish_status(status_with_coordinator_reason("not_leader")),
            RootEventPublishError::Transient(_)
        ));
        assert!(matches!(
            classify_root_event_publish_status(status_with_coordinator_reason(
                "catalog_precondition"
            )),
            RootEventPublishError::Permanent(_)
        ));
        assert!(matches!(
            classify_root_event_publish_status(Status::invalid_argument("bad root event")),
            RootEventPublishError::Permanent(_)
        ));
    }

    #[test]
    fn root_event_transition_id_matches_go_peer_shape() {
        let event = metapb::RootEvent {
            kind: metapb::RootEventKind::PeerAdditionPlanned as i32,
            payload: Some(metapb::root_event::Payload::PeerChange(
                metapb::RootPeerChange {
                    region_id: 11,
                    store_id: 2,
                    peer_id: 201,
                    ..Default::default()
                },
            )),
        };
        assert_eq!(root_event_transition_id(&event), "peer:11:add:2:201");

        let event = metapb::RootEvent {
            kind: metapb::RootEventKind::PeerRemovalCancelled as i32,
            payload: Some(metapb::root_event::Payload::PeerChange(
                metapb::RootPeerChange {
                    region_id: 11,
                    store_id: 2,
                    peer_id: 201,
                    ..Default::default()
                },
            )),
        };
        assert_eq!(root_event_transition_id(&event), "peer:11:remove:2:201");
    }

    #[test]
    fn root_event_transition_id_matches_go_range_split_shape() {
        let event = metapb::RootEvent {
            kind: metapb::RootEventKind::RegionSplitPlanned as i32,
            payload: Some(metapb::root_event::Payload::RangeSplit(
                metapb::RootRangeSplit {
                    parent_region_id: 7,
                    split_key: vec![0x00, 0x0a, 0xff],
                    ..Default::default()
                },
            )),
        };
        assert_eq!(root_event_transition_id(&event), "split:7:000aff");
    }

    #[test]
    fn root_event_transition_id_matches_go_range_merge_shape() {
        let event = metapb::RootEvent {
            kind: metapb::RootEventKind::RegionMergePlanned as i32,
            payload: Some(metapb::root_event::Payload::RangeMerge(
                metapb::RootRangeMerge {
                    left_region_id: 7,
                    right_region_id: 8,
                    ..Default::default()
                },
            )),
        };
        assert_eq!(root_event_transition_id(&event), "merge:7:8");
    }

    fn status_with_coordinator_reason(reason: &str) -> Status {
        let mut metadata = HashMap::new();
        metadata.insert(COORDINATOR_REASON_METADATA.to_owned(), reason.to_owned());
        let info = RpcErrorInfo {
            reason: NOKV_ERROR_INFO_REASON.to_owned(),
            domain: NOKV_ERROR_INFO_DOMAIN.to_owned(),
            metadata,
        };
        let details = RpcStatusDetails {
            code: tonic::Code::FailedPrecondition as i32,
            message: reason.to_owned(),
            details: vec![Any {
                type_url: GOOGLE_RPC_ERROR_INFO_TYPE.to_owned(),
                value: info.encode_to_vec(),
            }],
        };
        Status::with_details(
            tonic::Code::FailedPrecondition,
            reason.to_owned(),
            details.encode_to_vec().into(),
        )
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

        let outcome = execute_scheduler_operation(
            &local_admin_endpoint(addr),
            &coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
                region_id: 7,
                source_peer_id: 101,
                target_peer_id: 202,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(outcome, SchedulerOperationOutcome::Applied);
        let captured = transfers.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].region_id, 7);
        assert_eq!(captured[0].peer_id, 202);
        handle.abort();
    }

    #[tokio::test]
    async fn store_heartbeat_queries_all_endpoints_and_prefers_operations() {
        let endpoints = vec![
            "http://standby".to_owned(),
            "http://holder".to_owned(),
            "http://down".to_owned(),
        ];
        let operation = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
            region_id: 9,
            source_peer_id: 101,
            target_peer_id: 201,
            ..Default::default()
        };
        let responses = Arc::new(Mutex::new(BTreeMap::from([
            (
                "http://standby".to_owned(),
                Ok(coordpb::StoreHeartbeatResponse {
                    accepted: true,
                    ..Default::default()
                }),
            ),
            (
                "http://holder".to_owned(),
                Ok(coordpb::StoreHeartbeatResponse {
                    accepted: true,
                    operations: vec![operation.clone()],
                }),
            ),
            ("http://down".to_owned(), Err("unavailable".to_owned())),
        ])));
        let calls = Arc::new(Mutex::new(Vec::new()));

        let operations = send_store_heartbeat_with(
            &endpoints,
            coordpb::StoreHeartbeatRequest {
                store_id: 2,
                region_num: 1,
                leader_num: 1,
                leader_region_ids: vec![9],
                ..Default::default()
            },
            |endpoint, _request| {
                let responses = responses.clone();
                let calls = calls.clone();
                async move {
                    calls.lock().unwrap().push(endpoint.clone());
                    responses.lock().unwrap().get(&endpoint).unwrap().clone()
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(operations, vec![operation]);
        assert_eq!(calls.lock().unwrap().as_slice(), endpoints.as_slice());
    }

    #[tokio::test]
    async fn scheduler_operation_reports_unsupported_split_without_dialing_admin() {
        let outcome = execute_scheduler_operation(
            "http://127.0.0.1:1",
            &coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
                region_id: 7,
                split_key: b"k".to_vec(),
                split_child: Some(metapb::RegionDescriptor {
                    region_id: 8,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            SchedulerOperationOutcome::Unsupported {
                kind: coordpb::SchedulerOperationType::SplitRegion,
                reason: "split execution is not implemented in raftstore-rs yet",
            }
        );
    }

    #[tokio::test]
    async fn scheduler_operation_reports_invalid_split_before_admin_rpc() {
        let outcome = execute_scheduler_operation(
            "http://127.0.0.1:1",
            &coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
                region_id: 7,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            SchedulerOperationOutcome::Invalid {
                reason: "split requires region, split key, and child descriptor",
            }
        );
    }

    #[tokio::test]
    async fn scheduler_operation_reports_unsupported_merge_without_dialing_admin() {
        let outcome = execute_scheduler_operation(
            "http://127.0.0.1:1",
            &coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::MergeRegion as i32,
                region_id: 7,
                source_region_id: 8,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            SchedulerOperationOutcome::Unsupported {
                kind: coordpb::SchedulerOperationType::MergeRegion,
                reason: "merge execution is not implemented in raftstore-rs yet",
            }
        );
    }

    #[test]
    fn unsupported_scheduler_operation_records_pending_holt_diagnostic() {
        let store = HoltMvccStore::open_memory().unwrap();
        let operation = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: 7,
            split_key: b"k".to_vec(),
            split_child: Some(metapb::RegionDescriptor {
                region_id: 8,
                ..Default::default()
            }),
            ..Default::default()
        };

        record_pending_scheduler_operation(Some(&store), &operation);

        let pending = store.pending_scheduler_operations().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].operation, operation);
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

        let heartbeat = coordinator_heartbeat_request(
            identity,
            "127.0.0.1:23880".parse().unwrap(),
            &region,
            None,
        );
        assert_eq!(heartbeat.region_num, 0);
        assert_eq!(heartbeat.leader_num, 0);
        assert!(heartbeat.leader_region_ids.is_empty());
        assert!(heartbeat.region_stats.is_empty());
    }

    #[test]
    fn non_bootstrap_holt_start_does_not_persist_default_descriptor() {
        let store = HoltMvccStore::open_memory().unwrap();
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
            bootstrap: false,
        };

        let descriptor = startup_region_descriptor(&store, identity).unwrap();

        assert_eq!(descriptor, default_region_descriptor(identity));
        assert!(store.get_region_descriptor(7).unwrap().is_none());
    }

    #[test]
    fn bootstrap_holt_start_persists_default_descriptor() {
        let store = HoltMvccStore::open_memory().unwrap();
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 1,
            peer_id: 1,
            bootstrap: true,
        };

        let descriptor = startup_region_descriptor(&store, identity).unwrap();

        assert_eq!(descriptor, default_region_descriptor(identity));
        assert_eq!(store.get_region_descriptor(7).unwrap().unwrap(), descriptor);
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

        let req = coordinator_heartbeat_request(identity, addr, &region, None);

        assert_eq!(req.store_id, 11);
        assert_eq!(req.region_num, 1);
        assert_eq!(req.leader_num, 1);
        assert_eq!(req.leader_region_ids, vec![7]);
        assert_eq!(req.client_addr, "127.0.0.1:23880");
        assert_eq!(req.raft_addr, "127.0.0.1:23880");
        assert_eq!(req.region_stats.len(), 1);
        assert_eq!(req.region_stats[0].region_id, 7);
        assert_eq!(req.region_stats[0].leader_store_id, 11);
        assert!(!req.region_stats[0].pending_admin);
    }

    #[tokio::test]
    async fn coordinator_heartbeat_marks_pending_admin_for_unpublished_root_events() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 11,
            peer_id: 101,
            bootstrap: true,
        };
        let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
        let store = HoltMvccStore::open_memory().unwrap();
        store
            .enqueue_pending_root_event(&metapb::RootEvent {
                kind: metapb::RootEventKind::PeerAdded as i32,
                payload: Some(metapb::root_event::Payload::PeerChange(
                    metapb::RootPeerChange {
                        region_id: identity.region_id,
                        store_id: 12,
                        peer_id: 102,
                        target: Some(default_region_descriptor(identity)),
                        ..Default::default()
                    },
                )),
            })
            .unwrap();
        let region = open_openraft_region(
            identity,
            addr,
            dir.path().to_path_buf(),
            AppliedKvEngine::new(identity.region_id, MvccStore::new()),
        )
        .await
        .unwrap();

        let req = coordinator_heartbeat_request(identity, addr, &region, Some(&store));

        assert_eq!(req.region_stats.len(), 1);
        assert!(req.region_stats[0].pending_admin);
    }

    #[tokio::test]
    async fn coordinator_heartbeat_marks_pending_admin_for_pending_scheduler_operations() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 11,
            peer_id: 101,
            bootstrap: true,
        };
        let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
        let store = HoltMvccStore::open_memory().unwrap();
        store
            .record_pending_scheduler_operation(&coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
                region_id: identity.region_id,
                split_key: b"m".to_vec(),
                split_child: Some(metapb::RegionDescriptor {
                    region_id: 8,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap();
        let region = open_openraft_region(
            identity,
            addr,
            dir.path().to_path_buf(),
            AppliedKvEngine::new(identity.region_id, MvccStore::new()),
        )
        .await
        .unwrap();

        let req = coordinator_heartbeat_request(identity, addr, &region, Some(&store));

        assert_eq!(req.region_stats.len(), 1);
        assert!(req.region_stats[0].pending_admin);
    }

    #[test]
    fn startup_root_events_publish_store_and_bootstrap_region() {
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 11,
            peer_id: 101,
            bootstrap: true,
        };

        let events = startup_root_events(identity, default_region_descriptor(identity));

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, metapb::RootEventKind::StoreJoined as i32);
        match events[0].payload.as_ref().unwrap() {
            metapb::root_event::Payload::StoreMembership(membership) => {
                assert_eq!(membership.store_id, 11);
            }
            other => panic!("unexpected startup event payload: {other:?}"),
        }
        assert_eq!(
            events[1].kind,
            metapb::RootEventKind::RegionBootstrap as i32
        );
        let descriptor = match events[1].payload.as_ref().unwrap() {
            metapb::root_event::Payload::RegionDescriptor(record) => {
                record.descriptor.as_ref().unwrap()
            }
            other => panic!("unexpected startup event payload: {other:?}"),
        };
        assert_eq!(descriptor.region_id, 7);
        assert_eq!(descriptor.peers[0].store_id, 11);
        assert_eq!(descriptor.peers[0].peer_id, 101);
    }

    #[test]
    fn startup_root_events_for_joining_peer_only_publish_store_membership() {
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 12,
            peer_id: 102,
            bootstrap: false,
        };

        let events = startup_root_events(identity, default_region_descriptor(identity));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, metapb::RootEventKind::StoreJoined as i32);
        match events[0].payload.as_ref().unwrap() {
            metapb::root_event::Payload::StoreMembership(membership) => {
                assert_eq!(membership.store_id, 12);
            }
            other => panic!("unexpected startup event payload: {other:?}"),
        }
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
