use std::net::SocketAddr;
use std::time::Duration;

use nokv_holtstore::HoltMetadataStore;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_raftnode::{
    ApplyStatusProvider, OpenRaftRegion, RegionSnapshotEngine, RegionTrafficProvider,
};
use nokv_raftstore_server::{root_event_transition_id, RaftRuntimeStatusProvider};

use crate::hosted_region::HostedRegionRegistry;
use crate::range_controller::HoltRangeController;
use crate::root_publication::{publish_root_event_to_any, RootEventPublishError};
use crate::scheduler_operations::{
    execute_scheduler_operation, record_scheduler_operation_outcome,
    retry_pending_scheduler_operations,
};
use crate::startup::{parse_required_nonzero_u64, ServerIdentity};

#[derive(Clone)]
pub(crate) struct CoordinatorHeartbeatConfig {
    pub(crate) endpoints: Vec<String>,
    pub(crate) interval: Duration,
}

pub(crate) fn coordinator_heartbeat_config_from_env(
) -> Result<Option<CoordinatorHeartbeatConfig>, Box<dyn std::error::Error>> {
    let Ok(raw_addr) = std::env::var("NOKV_RAFTSTORE_COORDINATOR_ADDR") else {
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
        "NOKV_RAFTSTORE_COORDINATOR_HEARTBEAT_MS",
        std::env::var("NOKV_RAFTSTORE_COORDINATOR_HEARTBEAT_MS").ok(),
        1_000,
    )?;
    Ok(Some(CoordinatorHeartbeatConfig {
        endpoints,
        interval: Duration::from_millis(interval_ms),
    }))
}

pub(crate) fn coordinator_endpoints(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(coordinator_endpoint)
        .collect()
}

pub(crate) fn coordinator_endpoint(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_owned()
    } else {
        format!("http://{addr}")
    }
}

pub(crate) fn spawn_pending_topology_retries(
    config: Option<CoordinatorHeartbeatConfig>,
    pending_store: HoltMetadataStore,
    addr: SocketAddr,
    range_controller: Option<HoltRangeController>,
) {
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        run_pending_topology_retries(
            config,
            pending_store,
            local_admin_endpoint(addr),
            range_controller,
        )
        .await;
    });
}

pub(crate) async fn run_pending_topology_retries(
    config: CoordinatorHeartbeatConfig,
    pending_store: HoltMetadataStore,
    admin_endpoint: String,
    range_controller: Option<HoltRangeController>,
) {
    let mut ticker = tokio::time::interval(config.interval);
    loop {
        ticker.tick().await;
        retry_pending_topology_events(&config.endpoints, &pending_store).await;
        retry_pending_scheduler_operations(
            &admin_endpoint,
            &pending_store,
            range_controller.as_ref(),
        )
        .await;
    }
}

pub(crate) async fn retry_pending_topology_events(
    endpoints: &[String],
    pending_store: &HoltMetadataStore,
) {
    let pending = match pending_store.pending_root_events() {
        Ok(pending) => pending,
        Err(err) => {
            tracing::debug!(error = %err, "raftstore pending topology load failed");
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
                        "raftstore pending topology delete failed"
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
                        "raftstore pending topology block failed"
                    );
                    return;
                }
                tracing::debug!(
                    error = %err,
                    sequence = item.sequence,
                    "raftstore pending topology blocked"
                );
            }
            Err(RootEventPublishError::Transient(err)) => {
                tracing::debug!(
                    error = %err,
                    sequence = item.sequence,
                    "raftstore pending topology publish failed"
                );
                return;
            }
        }
    }
}

pub(crate) fn spawn_multi_region_coordinator_heartbeat<E>(
    config: Option<CoordinatorHeartbeatConfig>,
    store_id: u64,
    addr: SocketAddr,
    advertised_addr: String,
    regions: HostedRegionRegistry<E>,
    root_events: Option<HoltMetadataStore>,
    range_controller: Option<HoltRangeController>,
) where
    E: RegionSnapshotEngine + RegionTrafficProvider + Send + Sync + 'static,
{
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        run_multi_region_coordinator_heartbeat(
            config,
            store_id,
            addr,
            advertised_addr,
            regions,
            root_events,
            range_controller,
        )
        .await;
    });
}

pub(crate) async fn run_multi_region_coordinator_heartbeat<E>(
    config: CoordinatorHeartbeatConfig,
    store_id: u64,
    addr: SocketAddr,
    advertised_addr: String,
    regions: HostedRegionRegistry<E>,
    root_events: Option<HoltMetadataStore>,
    range_controller: Option<HoltRangeController>,
) where
    E: RegionSnapshotEngine + RegionTrafficProvider + Send + Sync + 'static,
{
    let mut ticker = tokio::time::interval(config.interval);
    let admin_endpoint = local_admin_endpoint(addr);
    loop {
        ticker.tick().await;
        if let Some(controller) = range_controller.as_ref() {
            if let Err(err) = controller.reconcile_local_region_descriptors().await {
                tracing::debug!(
                    error = %err,
                    "raftstore local region descriptor reconcile failed"
                );
            }
        }
        let request = match coordinator_heartbeat_request_for_hosted_regions(
            store_id,
            &advertised_addr,
            &regions,
            root_events.as_ref(),
        ) {
            Ok(request) => request,
            Err(err) => {
                tracing::debug!(error = %err, "raftstore hosted region snapshot failed");
                continue;
            }
        };
        match send_store_heartbeat(&config.endpoints, request).await {
            Ok(operations) => {
                for operation in operations {
                    record_scheduler_operation_outcome(
                        root_events.as_ref(),
                        &operation,
                        execute_scheduler_operation(
                            &admin_endpoint,
                            range_controller.as_ref(),
                            &operation,
                        )
                        .await,
                    );
                }
            }
            Err(err) => {
                tracing::debug!(error = %err, "raftstore coordinator heartbeat failed");
            }
        }
    }
}

pub(crate) async fn send_store_heartbeat(
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

pub(crate) async fn send_store_heartbeat_with<F, Fut>(
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

pub(crate) fn local_admin_endpoint(addr: SocketAddr) -> String {
    if addr.ip().is_unspecified() {
        format!("http://127.0.0.1:{}", addr.port())
    } else {
        format!("http://{addr}")
    }
}

#[cfg(test)]
pub(crate) fn coordinator_heartbeat_request<E>(
    identity: ServerIdentity,
    addr: &str,
    region: &OpenRaftRegion<E>,
    root_events: Option<&HoltMetadataStore>,
) -> coordpb::StoreHeartbeatRequest
where
    E: RegionSnapshotEngine + RegionTrafficProvider,
{
    coordinator_heartbeat_request_for_regions(
        identity.store_id,
        addr,
        &[(identity, region.clone())],
        root_events,
    )
}

pub(crate) fn coordinator_heartbeat_request_for_hosted_regions<E>(
    store_id: u64,
    addr: &str,
    registry: &HostedRegionRegistry<E>,
    root_events: Option<&HoltMetadataStore>,
) -> Result<coordpb::StoreHeartbeatRequest, String>
where
    E: RegionSnapshotEngine + RegionTrafficProvider,
{
    let regions = registry.snapshot()?;
    Ok(coordinator_heartbeat_request_for_regions(
        store_id,
        addr,
        &regions,
        root_events,
    ))
}

pub(crate) fn coordinator_heartbeat_request_for_regions<E>(
    store_id: u64,
    addr: &str,
    regions: &[(ServerIdentity, OpenRaftRegion<E>)],
    root_events: Option<&HoltMetadataStore>,
) -> coordpb::StoreHeartbeatRequest
where
    E: RegionSnapshotEngine + RegionTrafficProvider,
{
    let pending_admin = root_events
        .map(topology_catalog_has_pending_admin_work)
        .unwrap_or(false);
    let mut region_num = 0;
    let mut leader_num = 0;
    let mut leader_region_ids = Vec::new();
    let mut region_stats = Vec::new();
    for (identity, region) in regions {
        let status = region.apply_status();
        let runtime = region.raft_runtime_status();
        let known = status.region_id != 0 && runtime.hosted;
        if !known {
            continue;
        }
        region_num += 1;
        let leader = runtime.leader;
        if leader {
            leader_num += 1;
            leader_region_ids.push(status.region_id);
        }
        let traffic = region.traffic_snapshot();
        region_stats.push(coordpb::RegionRuntimeStats {
            region_id: status.region_id,
            read_qps: traffic.read_ops / traffic.elapsed_secs,
            write_qps: traffic.write_ops / traffic.elapsed_secs,
            write_bytes_per_sec: traffic.write_bytes / traffic.elapsed_secs,
            atomic_mutate_qps: traffic.atomic_ops / traffic.elapsed_secs,
            leader_store_id: if leader { identity.store_id } else { 0 },
            pending_admin,
            ..Default::default()
        });
    }
    coordpb::StoreHeartbeatRequest {
        store_id,
        region_num,
        leader_num,
        leader_region_ids,
        client_addr: addr.to_owned(),
        raft_addr: addr.to_owned(),
        region_stats,
        ..Default::default()
    }
}

pub(crate) fn topology_catalog_has_pending_admin_work(store: &HoltMetadataStore) -> bool {
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
    let blocked_scheduler = store
        .blocked_scheduler_operations()
        .map(|ops| !ops.is_empty())
        .unwrap_or(true);
    pending || blocked || scheduler || blocked_scheduler
}
