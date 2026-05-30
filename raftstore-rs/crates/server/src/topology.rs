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
        Some(metapb::root_event::Payload::RangeSplit(split)) => {
            format!(
                "split:{}:{}",
                split.parent_region_id,
                lowercase_hex(&split.split_key)
            )
        }
        Some(metapb::root_event::Payload::RangeMerge(merge)) => {
            format!("merge:{}:{}", merge.left_region_id, merge.right_region_id)
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
        coordpb::SchedulerOperationType::SplitRegion => {
            format!(
                "split:{}:{}",
                operation.region_id,
                lowercase_hex(&operation.split_key)
            )
        }
        coordpb::SchedulerOperationType::MergeRegion => {
            format!(
                "merge:{}:{}",
                operation.region_id, operation.source_region_id
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
        coordpb::SchedulerOperationType::SplitRegion => "range split",
        coordpb::SchedulerOperationType::MergeRegion => "range merge",
        coordpb::SchedulerOperationType::None => "scheduler operation",
    }
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
