use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use prost::Message;

use crate::{Error, Result};

pub const DEFAULT_CURRENT_TREE: &str = "default_current";
pub const MOUNT_CURRENT_TREE: &str = "mount_current";
pub const INODE_CURRENT_TREE: &str = "inode_current";
pub const DENTRY_CURRENT_TREE: &str = "dentry_current";
pub const PARENT_CURRENT_TREE: &str = "parent_current";
pub const CHUNK_CURRENT_TREE: &str = "chunk_current";
pub const SESSION_CURRENT_TREE: &str = "session_current";
pub const QUOTA_CURRENT_TREE: &str = "quota_current";
pub const SNAPSHOT_CURRENT_TREE: &str = "snapshot_current";
pub const PATH_INDEX_CURRENT_TREE: &str = "path_index_current";
pub const WATCH_CURRENT_TREE: &str = "watch_current";
pub const COMMAND_DEDUPE_CURRENT_TREE: &str = "command_dedupe_current";
pub const SEGMENT_CURRENT_TREE: &str = "segment_current";
pub const HISTORY_TREE: &str = "history";
pub const REGION_META_TREE: &str = "region_meta";
pub const APPLY_STATE_TREE: &str = "apply_state";
pub const WATCH_APPLY_TREE: &str = "watch_apply";

pub(crate) const CURRENT_TREES: [&str; 13] = [
    DEFAULT_CURRENT_TREE,
    MOUNT_CURRENT_TREE,
    INODE_CURRENT_TREE,
    DENTRY_CURRENT_TREE,
    PARENT_CURRENT_TREE,
    CHUNK_CURRENT_TREE,
    SESSION_CURRENT_TREE,
    QUOTA_CURRENT_TREE,
    SNAPSHOT_CURRENT_TREE,
    PATH_INDEX_CURRENT_TREE,
    WATCH_CURRENT_TREE,
    COMMAND_DEDUPE_CURRENT_TREE,
    SEGMENT_CURRENT_TREE,
];

pub(crate) const REQUIRED_TREES: [&str; 17] = [
    DEFAULT_CURRENT_TREE,
    MOUNT_CURRENT_TREE,
    INODE_CURRENT_TREE,
    DENTRY_CURRENT_TREE,
    PARENT_CURRENT_TREE,
    CHUNK_CURRENT_TREE,
    SESSION_CURRENT_TREE,
    QUOTA_CURRENT_TREE,
    SNAPSHOT_CURRENT_TREE,
    PATH_INDEX_CURRENT_TREE,
    WATCH_CURRENT_TREE,
    COMMAND_DEDUPE_CURRENT_TREE,
    SEGMENT_CURRENT_TREE,
    HISTORY_TREE,
    REGION_META_TREE,
    APPLY_STATE_TREE,
    WATCH_APPLY_TREE,
];

pub(crate) fn current_tree_for_family(family: metadatapb::MetadataFamily) -> &'static str {
    match family {
        metadatapb::MetadataFamily::Mount => MOUNT_CURRENT_TREE,
        metadatapb::MetadataFamily::Inode => INODE_CURRENT_TREE,
        metadatapb::MetadataFamily::Dentry => DENTRY_CURRENT_TREE,
        metadatapb::MetadataFamily::Parent => PARENT_CURRENT_TREE,
        metadatapb::MetadataFamily::Chunk => CHUNK_CURRENT_TREE,
        metadatapb::MetadataFamily::Session => SESSION_CURRENT_TREE,
        metadatapb::MetadataFamily::Quota => QUOTA_CURRENT_TREE,
        metadatapb::MetadataFamily::Snapshot => SNAPSHOT_CURRENT_TREE,
        metadatapb::MetadataFamily::PathIndex => PATH_INDEX_CURRENT_TREE,
        metadatapb::MetadataFamily::Watch => WATCH_CURRENT_TREE,
        metadatapb::MetadataFamily::CommandDedupe => COMMAND_DEDUPE_CURRENT_TREE,
        metadatapb::MetadataFamily::Segment => SEGMENT_CURRENT_TREE,
        metadatapb::MetadataFamily::Unspecified => DEFAULT_CURRENT_TREE,
    }
}

pub(crate) fn family_from_i32(raw: i32) -> metadatapb::MetadataFamily {
    metadatapb::MetadataFamily::try_from(raw).unwrap_or(metadatapb::MetadataFamily::Unspecified)
}

pub(crate) fn history_prefix(family: metadatapb::MetadataFamily, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + key.len());
    out.extend_from_slice(&(family as i32).to_be_bytes());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

pub(crate) fn history_key(
    family: metadatapb::MetadataFamily,
    key: &[u8],
    commit_ts: u64,
) -> Vec<u8> {
    let mut out = history_prefix(family, key);
    out.extend_from_slice(&(u64::MAX - commit_ts).to_be_bytes());
    out
}

pub(crate) fn decode_history_key(
    key: &[u8],
) -> metadata_state::Result<Option<(metadatapb::MetadataFamily, Vec<u8>, u64)>> {
    if key.len() < 16 {
        return Ok(None);
    }
    let raw_family = i32::from_be_bytes(key[0..4].try_into().unwrap());
    let family = family_from_i32(raw_family);
    let user_len = u32::from_be_bytes(key[4..8].try_into().unwrap()) as usize;
    if key.len() != 8 + user_len + 8 {
        return Ok(None);
    }
    let user_key = key[8..8 + user_len].to_vec();
    let inverted = u64::from_be_bytes(key[8 + user_len..].try_into().unwrap());
    Ok(Some((family, user_key, u64::MAX - inverted)))
}

pub(crate) const REGION_DESCRIPTOR_PREFIX: &[u8] = b"descriptor/";
pub(crate) const PENDING_ROOT_EVENT_PREFIX: &[u8] = b"pending-root-event/";
pub(crate) const BLOCKED_ROOT_EVENT_PREFIX: &[u8] = b"blocked-root-event/";
pub(crate) const PENDING_SCHEDULER_OPERATION_PREFIX: &[u8] = b"pending-scheduler-operation/";
pub(crate) const BLOCKED_SCHEDULER_OPERATION_PREFIX: &[u8] = b"blocked-scheduler-operation/";
pub(crate) const WATCH_APPLY_EVENT_PREFIX: &[u8] = b"event/";

pub(crate) fn region_descriptor_key(region_id: u64) -> Vec<u8> {
    region_meta_key(REGION_DESCRIPTOR_PREFIX, region_id)
}

pub(crate) fn region_apply_state_key(region_id: u64) -> Vec<u8> {
    region_meta_key(b"apply-state/", region_id)
}

pub(crate) fn pending_root_event_key(sequence: u64) -> Vec<u8> {
    region_meta_key(PENDING_ROOT_EVENT_PREFIX, sequence)
}

pub(crate) fn pending_root_event_sequence(key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(PENDING_ROOT_EVENT_PREFIX)?;
    if rest.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

pub(crate) fn blocked_root_event_key(sequence: u64) -> Vec<u8> {
    region_meta_key(BLOCKED_ROOT_EVENT_PREFIX, sequence)
}

pub(crate) fn blocked_root_event_sequence(key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(BLOCKED_ROOT_EVENT_PREFIX)?;
    if rest.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

pub(crate) fn pending_scheduler_operation_key(
    operation: &coordpb::SchedulerOperation,
) -> Result<Vec<u8>> {
    let kind = coordpb::SchedulerOperationType::try_from(operation.r#type)
        .unwrap_or(coordpb::SchedulerOperationType::None);
    if kind == coordpb::SchedulerOperationType::None {
        return Err(Error::InvalidMetadata(
            "scheduler operation type is required".to_owned(),
        ));
    }
    if operation.region_id == 0 {
        return Err(Error::InvalidMetadata(
            "scheduler operation region is required".to_owned(),
        ));
    }
    let mut encoded = Vec::with_capacity(operation.encoded_len());
    operation.encode(&mut encoded)?;
    let mut key =
        Vec::with_capacity(PENDING_SCHEDULER_OPERATION_PREFIX.len() + 4 + 8 + encoded.len());
    key.extend_from_slice(PENDING_SCHEDULER_OPERATION_PREFIX);
    key.extend_from_slice(&(kind as i32).to_be_bytes());
    key.extend_from_slice(&operation.region_id.to_be_bytes());
    key.extend_from_slice(&encoded);
    Ok(key)
}

pub(crate) fn blocked_scheduler_operation_key(
    operation: &coordpb::SchedulerOperation,
) -> Result<Vec<u8>> {
    let pending = pending_scheduler_operation_key(operation)?;
    let rest = pending
        .strip_prefix(PENDING_SCHEDULER_OPERATION_PREFIX)
        .expect("pending scheduler key has scheduler prefix");
    let mut key = Vec::with_capacity(BLOCKED_SCHEDULER_OPERATION_PREFIX.len() + rest.len());
    key.extend_from_slice(BLOCKED_SCHEDULER_OPERATION_PREFIX);
    key.extend_from_slice(rest);
    Ok(key)
}

pub(crate) fn region_meta_key(prefix: &[u8], region_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&region_id.to_be_bytes());
    key
}

pub(crate) fn watch_apply_region_prefix(region_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(WATCH_APPLY_EVENT_PREFIX.len() + 8);
    key.extend_from_slice(WATCH_APPLY_EVENT_PREFIX);
    key.extend_from_slice(&region_id.to_be_bytes());
    key
}

pub(crate) fn watch_apply_event_key(
    region_id: u64,
    term: u64,
    index: u64,
    commit_version: u64,
    encoded_event: &[u8],
) -> Vec<u8> {
    let mut key = watch_apply_region_prefix(region_id);
    key.extend_from_slice(&term.to_be_bytes());
    key.extend_from_slice(&index.to_be_bytes());
    key.extend_from_slice(&commit_version.to_be_bytes());
    key.extend_from_slice(&(encoded_event.len() as u32).to_be_bytes());
    key.extend_from_slice(encoded_event);
    key
}
