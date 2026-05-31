use nokv_metastore as metastore;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use prost::Message;

use crate::{Error, Result};

pub const DATA_TREE: &str = "data";
pub const WRITE_TREE: &str = "write";
pub const REGION_META_TREE: &str = "region_meta";
pub const APPLY_STATE_TREE: &str = "apply_state";
pub const WATCH_APPLY_TREE: &str = "watch_apply";

pub(crate) const REQUIRED_TREES: [&str; 5] = [
    DATA_TREE,
    WRITE_TREE,
    REGION_META_TREE,
    APPLY_STATE_TREE,
    WATCH_APPLY_TREE,
];

pub(crate) fn write_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

pub(crate) fn write_key(key: &[u8], commit_ts: u64) -> Vec<u8> {
    let mut out = write_prefix(key);
    out.extend_from_slice(&(u64::MAX - commit_ts).to_be_bytes());
    out
}

pub(crate) fn decode_write_key(key: &[u8]) -> metastore::Result<Option<(Vec<u8>, u64)>> {
    if key.len() < 12 {
        return Ok(None);
    }
    let user_len = u32::from_be_bytes(key[0..4].try_into().unwrap()) as usize;
    if key.len() != 4 + user_len + 8 {
        return Ok(None);
    }
    let user_key = key[4..4 + user_len].to_vec();
    let inverted = u64::from_be_bytes(key[4 + user_len..].try_into().unwrap());
    Ok(Some((user_key, u64::MAX - inverted)))
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
