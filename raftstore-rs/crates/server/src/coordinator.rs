use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use nokv_holtstore::HoltMvccStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{
    ApplyStatusProvider, OpenRaftRegion, RegionSnapshotEngine, RegionTrafficProvider,
};
use nokv_raftstore_server::{
    root_event_transition_id, RaftRuntimeStatusProvider, TopologyPublishOutcome, TopologyPublisher,
};
use prost::Message;
use prost_types::Any;

use crate::bootstrap::{HoltRangeController, HostedRegionRegistry};
use crate::startup::{parse_required_nonzero_u64, ServerIdentity};

#[derive(Clone)]
pub(crate) struct CoordinatorHeartbeatConfig {
    pub(crate) endpoints: Vec<String>,
    pub(crate) interval: Duration,
}

pub(crate) fn coordinator_heartbeat_config_from_env(
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

#[derive(Clone)]
pub(crate) struct CoordinatorTopologyPublisher {
    pub(crate) endpoints: Vec<String>,
    pub(crate) pending_store: Option<HoltMvccStore>,
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

pub(crate) async fn publish_root_event_with_pending(
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

pub(crate) async fn publish_root_event_to_any(
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

pub(crate) async fn publish_root_event(
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
pub(crate) enum RootEventPublishError {
    Transient(String),
    Permanent(String),
}

pub(crate) fn classify_root_event_publish_status(status: tonic::Status) -> RootEventPublishError {
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

pub(crate) const COORDINATOR_REASON_METADATA: &str = "coordinator_reason";
pub(crate) const NOKV_ERROR_INFO_DOMAIN: &str = "nokv";
pub(crate) const NOKV_ERROR_INFO_REASON: &str = "nokv_error";
pub(crate) const GOOGLE_RPC_ERROR_INFO_TYPE: &str = "type.googleapis.com/google.rpc.ErrorInfo";
pub(crate) const MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS: u32 = 8;

#[derive(Clone, PartialEq, Message)]
pub(crate) struct RpcStatusDetails {
    #[prost(int32, tag = "1")]
    pub(crate) code: i32,
    #[prost(string, tag = "2")]
    pub(crate) message: String,
    #[prost(message, repeated, tag = "3")]
    pub(crate) details: Vec<Any>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct RpcErrorInfo {
    #[prost(string, tag = "1")]
    pub(crate) reason: String,
    #[prost(string, tag = "2")]
    pub(crate) domain: String,
    #[prost(map = "string, string", tag = "3")]
    pub(crate) metadata: HashMap<String, String>,
}

pub(crate) fn root_event_status_metadata(
    status: &tonic::Status,
) -> Option<HashMap<String, String>> {
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

pub(crate) fn spawn_pending_topology_retries(
    config: Option<CoordinatorHeartbeatConfig>,
    pending_store: HoltMvccStore,
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

pub(crate) fn spawn_startup_root_publication_for_regions(
    config: Option<CoordinatorHeartbeatConfig>,
    identities: Vec<ServerIdentity>,
    descriptors: Vec<metapb::RegionDescriptor>,
    pending_store: Option<HoltMvccStore>,
) {
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        for event in startup_root_events_for_regions(&identities, &descriptors) {
            let outcome =
                publish_root_event_with_pending(&config.endpoints, pending_store.as_ref(), event)
                    .await;
            if outcome.publish_state() == adminpb::ExecutionPublishState::TerminalPublished {
                continue;
            }
            tracing::debug!(
                publish = ?outcome.publish_state(),
                error = %outcome.last_error(),
                "rust raftstore multi-region startup root publication deferred"
            );
        }
    });
}

#[cfg(test)]
pub(crate) fn startup_root_events(
    identity: ServerIdentity,
    descriptor: metapb::RegionDescriptor,
) -> Vec<metapb::RootEvent> {
    startup_root_events_for_regions(&[identity], &[descriptor])
}

pub(crate) fn startup_root_events_for_regions(
    identities: &[ServerIdentity],
    descriptors: &[metapb::RegionDescriptor],
) -> Vec<metapb::RootEvent> {
    let store_id = identities
        .first()
        .map(|identity| identity.store_id)
        .unwrap_or_default();
    let mut events = vec![metapb::RootEvent {
        kind: metapb::RootEventKind::StoreJoined as i32,
        payload: Some(metapb::root_event::Payload::StoreMembership(
            metapb::RootStoreMembership { store_id },
        )),
    }];
    for (identity, descriptor) in identities.iter().zip(descriptors) {
        if !identity.bootstrap {
            continue;
        }
        events.push(metapb::RootEvent {
            kind: metapb::RootEventKind::RegionBootstrap as i32,
            payload: Some(metapb::root_event::Payload::RegionDescriptor(
                metapb::RootRegionDescriptor {
                    descriptor: Some(descriptor.clone()),
                },
            )),
        });
    }
    events
}

pub(crate) async fn run_pending_topology_retries(
    config: CoordinatorHeartbeatConfig,
    pending_store: HoltMvccStore,
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
    pending_store: &HoltMvccStore,
) {
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

pub(crate) async fn retry_pending_scheduler_operations(
    admin_endpoint: &str,
    pending_store: &HoltMvccStore,
    range_controller: Option<&HoltRangeController>,
) {
    let pending = match pending_store.pending_scheduler_operations() {
        Ok(pending) => pending,
        Err(err) => {
            tracing::debug!(error = %err, "rust raftstore pending scheduler load failed");
            return;
        }
    };
    for item in pending {
        match execute_scheduler_operation(admin_endpoint, range_controller, &item.operation).await {
            Ok(SchedulerOperationOutcome::Applied)
            | Ok(SchedulerOperationOutcome::Invalid { .. }) => {
                if let Err(err) = pending_store.delete_pending_scheduler_operation(&item.operation)
                {
                    tracing::debug!(
                        error = %err,
                        "rust raftstore pending scheduler delete failed"
                    );
                    return;
                }
            }
            Ok(SchedulerOperationOutcome::Unsupported { kind, reason }) => {
                match record_pending_scheduler_operation_attempt(pending_store, &item.operation) {
                    Ok(attempts) if attempts >= MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS => {
                        if let Err(err) = pending_store.block_pending_scheduler_operation(
                            &item.operation,
                            attempts,
                            reason,
                        ) {
                            tracing::debug!(
                                error = %err,
                                "rust raftstore exhausted pending scheduler block failed"
                            );
                            return;
                        }
                        tracing::warn!(
                            ?kind,
                            %reason,
                            attempts,
                            "rust raftstore abandoned unsupported pending scheduler operation"
                        );
                        continue;
                    }
                    Ok(attempts) => {
                        tracing::debug!(
                            ?kind,
                            %reason,
                            attempts,
                            "rust raftstore pending scheduler operation still unsupported"
                        );
                    }
                    Err(err) => {
                        tracing::debug!(
                            error = %err,
                            "rust raftstore pending scheduler attempt update failed"
                        );
                        return;
                    }
                }
            }
            Err(err) => {
                match record_pending_scheduler_operation_attempt(pending_store, &item.operation) {
                    Ok(attempts) if attempts >= MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS => {
                        if let Err(block_err) = pending_store.block_pending_scheduler_operation(
                            &item.operation,
                            attempts,
                            &err.to_string(),
                        ) {
                            tracing::debug!(
                                error = %block_err,
                                "rust raftstore exhausted pending scheduler block failed"
                            );
                            return;
                        }
                        tracing::warn!(
                            error = %err,
                            attempts,
                            "rust raftstore abandoned pending scheduler operation after retry limit"
                        );
                        continue;
                    }
                    Ok(attempts) => {
                        tracing::debug!(
                            error = %err,
                            attempts,
                            "rust raftstore pending scheduler operation retry failed"
                        );
                    }
                    Err(update_err) => {
                        tracing::debug!(
                            error = %update_err,
                            "rust raftstore pending scheduler attempt update failed"
                        );
                    }
                }
                return;
            }
        }
    }
}

pub(crate) fn record_pending_scheduler_operation_attempt(
    store: &HoltMvccStore,
    operation: &coordpb::SchedulerOperation,
) -> Result<u32, String> {
    store
        .increment_pending_scheduler_operation_attempts(operation)
        .map_err(|err| err.to_string())
}

pub(crate) fn spawn_multi_region_coordinator_heartbeat<E>(
    config: Option<CoordinatorHeartbeatConfig>,
    store_id: u64,
    addr: SocketAddr,
    advertised_addr: String,
    regions: HostedRegionRegistry<E>,
    root_events: Option<HoltMvccStore>,
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
    root_events: Option<HoltMvccStore>,
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
                    "rust raftstore local region descriptor reconcile failed"
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
                tracing::debug!(error = %err, "rust raftstore hosted region snapshot failed");
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
                tracing::debug!(error = %err, "rust raftstore coordinator heartbeat failed");
            }
        }
    }
}

pub(crate) fn record_scheduler_operation_outcome(
    store: Option<&HoltMvccStore>,
    operation: &coordpb::SchedulerOperation,
    outcome: Result<SchedulerOperationOutcome, tonic::Status>,
) {
    match outcome {
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
            record_pending_scheduler_operation(store, operation);
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
            record_pending_scheduler_operation(store, operation);
            tracing::debug!(
                error = %err,
                "rust raftstore coordinator operation failed"
            );
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

pub(crate) fn record_pending_scheduler_operation(
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
pub(crate) enum SchedulerOperationOutcome {
    Applied,
    Invalid {
        reason: &'static str,
    },
    Unsupported {
        kind: coordpb::SchedulerOperationType,
        reason: &'static str,
    },
}

pub(crate) async fn execute_scheduler_operation(
    admin_endpoint: &str,
    range_controller: Option<&HoltRangeController>,
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
            if let Some(controller) = range_controller {
                return controller.execute_split(operation).await;
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
            if let Some(controller) = range_controller {
                return controller.execute_merge(operation).await;
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
    root_events: Option<&HoltMvccStore>,
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
    root_events: Option<&HoltMvccStore>,
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
    root_events: Option<&HoltMvccStore>,
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

pub(crate) fn topology_catalog_has_pending_admin_work(store: &HoltMvccStore) -> bool {
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
