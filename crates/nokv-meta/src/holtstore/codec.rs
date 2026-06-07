//! Value, tombstone, dedupe, and watch-event codec for the Holt metadata
//! store, split out of holtstore.rs by concern.

use crate::command::{CommitResult, MetadataError, Version};

pub(super) const VALUE_HEADER_LEN: usize = 9;
pub(super) const VALUE_KIND_LIVE: u8 = 1;
pub(super) const VALUE_KIND_TOMBSTONE: u8 = 2;

pub(super) fn encode_current_value(version: Version, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALUE_HEADER_LEN + value.len());
    out.extend_from_slice(&version.get().to_be_bytes());
    out.push(VALUE_KIND_LIVE);
    out.extend_from_slice(value);
    out
}

pub(super) fn encode_tombstone_value(version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALUE_HEADER_LEN);
    out.extend_from_slice(&version.get().to_be_bytes());
    out.push(VALUE_KIND_TOMBSTONE);
    out
}

pub(super) fn decode_current_value(
    encoded: &[u8],
) -> Result<(Version, Option<Vec<u8>>), MetadataError> {
    if encoded.len() < VALUE_HEADER_LEN {
        return Err(MetadataError::Backend(
            "encoded current metadata value is truncated".to_owned(),
        ));
    }
    let raw = u64::from_be_bytes(
        encoded[..8]
            .try_into()
            .expect("current value header has fixed width"),
    );
    let version = Version::new(raw)?;
    match encoded[8] {
        VALUE_KIND_LIVE => Ok((version, Some(encoded[VALUE_HEADER_LEN..].to_vec()))),
        VALUE_KIND_TOMBSTONE => {
            if encoded.len() != VALUE_HEADER_LEN {
                return Err(MetadataError::Backend(
                    "encoded tombstone metadata value has trailing bytes".to_owned(),
                ));
            }
            Ok((version, None))
        }
        tag => Err(MetadataError::Backend(format!(
            "encoded metadata value has unknown kind {tag}"
        ))),
    }
}

pub(super) fn watch_event_key(base: &[u8], version: Version, ordinal: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(base.len() + 16);
    key.extend_from_slice(base);
    key.extend_from_slice(&version.get().to_be_bytes());
    key.extend_from_slice(&(ordinal as u64).to_be_bytes());
    key
}

pub(super) fn encode_dedupe_result(result: &CommitResult) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&result.commit_version.get().to_be_bytes());
    out.extend_from_slice(&(result.applied_mutations as u64).to_be_bytes());
    out.extend_from_slice(&(result.watch_events as u64).to_be_bytes());
    out
}

pub(super) fn decode_dedupe_result(encoded: &[u8]) -> Result<CommitResult, MetadataError> {
    if encoded.len() != 24 {
        return Err(MetadataError::Backend(
            "encoded command dedupe result is malformed".to_owned(),
        ));
    }
    Ok(CommitResult {
        commit_version: Version::new(u64::from_be_bytes(encoded[0..8].try_into().unwrap()))?,
        applied_mutations: u64::from_be_bytes(encoded[8..16].try_into().unwrap()) as usize,
        watch_events: u64::from_be_bytes(encoded[16..24].try_into().unwrap()) as usize,
    })
}

pub(super) fn history_user_prefix(key: &[u8]) -> Result<&[u8], MetadataError> {
    if key.len() <= std::mem::size_of::<u64>() {
        return Err(MetadataError::Backend(
            "history key is missing version suffix".to_owned(),
        ));
    }
    Ok(&key[..key.len() - std::mem::size_of::<u64>()])
}
