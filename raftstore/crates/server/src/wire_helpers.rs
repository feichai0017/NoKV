use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::raft::v1 as raftpb;
use tonic::Status;

use crate::admission::RegionAdmission;
use crate::{
    DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE, DEFAULT_APPLY_WATCH_MAX_KEY_BYTES_PER_MESSAGE,
};

pub(crate) fn trim_scan_response_to_region(
    admission: &RegionAdmission,
    response: &mut kvpb::ScanResponse,
) {
    response
        .kvs
        .retain(|kv| admission.key_in_range(kv.key.as_slice()));
}

pub(crate) fn header_from_context(context: &kvpb::Context) -> raftpb::CmdHeader {
    let peer = context.peer.as_ref();
    raftpb::CmdHeader {
        region_id: context.region_id,
        region_epoch: context.region_epoch.clone(),
        peer_id: peer.map(|peer| peer.peer_id).unwrap_or_default(),
        read_consistency: context.read_consistency,
        read_preference: context.read_preference,
        max_stale_read_index: context.max_stale_read_index,
        max_stale_read_ms: context.max_stale_read_ms,
        store_id: peer.map(|peer| peer.store_id).unwrap_or_default(),
        ..Default::default()
    }
}

pub(crate) fn raft_payload_error(operation: &str, detail: &str) -> Status {
    Status::internal(format!("{operation} raft payload error: {detail}"))
}

pub(crate) fn matching_apply_watch_keys(keys: &[Vec<u8>], prefix: &[u8]) -> Vec<Vec<u8>> {
    keys.iter()
        .filter(|key| prefix.is_empty() || key.starts_with(prefix))
        .cloned()
        .collect()
}

pub(crate) fn chunk_apply_watch_keys(keys: Vec<Vec<u8>>) -> Vec<Vec<Vec<u8>>> {
    if keys.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::with_capacity(
        (keys.len() + DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE - 1)
            / DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE,
    );
    let mut current = Vec::with_capacity(keys.len().min(DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE));
    let mut current_bytes = 0usize;
    for key in keys {
        let key_bytes = key.len();
        if !current.is_empty()
            && (current.len() >= DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE
                || current_bytes + key_bytes > DEFAULT_APPLY_WATCH_MAX_KEY_BYTES_PER_MESSAGE)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += key_bytes;
        current.push(key);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}
