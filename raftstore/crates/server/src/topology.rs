//! Shared topology diagnostic formatting for Rust raftstore services.

use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;

pub fn root_event_transition_id(event: &metapb::RootEvent) -> String {
    match event.payload.as_ref() {
        Some(metapb::root_event::Payload::PeerChange(change)) => {
            let action = match metapb::RootEventKind::try_from(event.kind)
                .unwrap_or(metapb::RootEventKind::Unspecified)
            {
                metapb::RootEventKind::PeerAdditionPlanned
                | metapb::RootEventKind::PeerAdded
                | metapb::RootEventKind::PeerAdditionCancelled => "add",
                metapb::RootEventKind::PeerRemovalPlanned
                | metapb::RootEventKind::PeerRemoved
                | metapb::RootEventKind::PeerRemovalCancelled => "remove",
                _ => "unknown",
            };
            peer_change_transition_id(action, change.region_id, change.store_id, change.peer_id)
        }
        _ => format!("root-event:{}", event.kind),
    }
}

pub(crate) fn peer_change_transition_id(
    action: &str,
    region_id: u64,
    store_id: u64,
    peer_id: u64,
) -> String {
    format!("peer:{region_id}:{action}:{store_id}:{peer_id}")
}

pub(crate) fn scheduler_operation_transition_id(operation: &coordpb::SchedulerOperation) -> String {
    let kind = coordpb::SchedulerOperationType::try_from(operation.r#type)
        .unwrap_or(coordpb::SchedulerOperationType::None);
    match kind {
        coordpb::SchedulerOperationType::LeaderTransfer => format!(
            "leader-transfer:{}:{}:{}",
            operation.region_id, operation.source_peer_id, operation.target_peer_id
        ),
        coordpb::SchedulerOperationType::PruneMetadataVersions => {
            format!(
                "metadata-prune:{}:{}",
                operation.region_id, operation.retention_floor
            )
        }
        coordpb::SchedulerOperationType::None => {
            format!("scheduler:{}:{}", operation.r#type, operation.region_id)
        }
    }
}

pub(crate) fn scheduler_operation_action(operation: &coordpb::SchedulerOperation) -> &'static str {
    let kind = coordpb::SchedulerOperationType::try_from(operation.r#type)
        .unwrap_or(coordpb::SchedulerOperationType::None);
    match kind {
        coordpb::SchedulerOperationType::LeaderTransfer => "leader transfer",
        coordpb::SchedulerOperationType::PruneMetadataVersions => "metadata retention prune",
        coordpb::SchedulerOperationType::None => "scheduler operation",
    }
}
