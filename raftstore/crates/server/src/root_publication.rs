use std::collections::HashMap;

use nokv_holtstore::HoltMetadataStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftstore_server::{root_event_transition_id, TopologyPublishOutcome, TopologyPublisher};
use prost::Message;
use prost_types::Any;

use crate::coordinator::CoordinatorHeartbeatConfig;
use crate::startup::ServerIdentity;

#[derive(Clone)]
pub(crate) struct CoordinatorTopologyPublisher {
    pub(crate) endpoints: Vec<String>,
    pub(crate) pending_store: Option<HoltMetadataStore>,
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
    pending_store: Option<&HoltMetadataStore>,
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

pub(crate) fn spawn_startup_root_publication_for_regions(
    config: Option<CoordinatorHeartbeatConfig>,
    identities: Vec<ServerIdentity>,
    descriptors: Vec<metapb::RegionDescriptor>,
    pending_store: Option<HoltMetadataStore>,
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
                "raftstore multi-region startup root publication deferred"
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
