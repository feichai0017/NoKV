use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Inner, MemoryMetadataStore, VersionedValue};

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }
}

pub fn scan_read_version(version: u64) -> u64 {
    if version == 0 {
        u64::MAX
    } else {
        version
    }
}

pub fn scan_limit(limit: u32) -> usize {
    if limit == 0 {
        1
    } else {
        limit as usize
    }
}

pub fn value_is_expired(expires_at: u64) -> bool {
    expires_at > 0 && expires_at <= current_unix_seconds()
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn read_committed(inner: &Inner, key: &[u8], version: u64) -> Option<VersionedValue> {
    inner
        .writes
        .get(key)
        .and_then(|versions| versions.range(..=version).next_back())
        .map(|(_, value)| value.clone())
}

pub(crate) fn write_by_start_version(
    inner: &Inner,
    key: &[u8],
    start_version: u64,
) -> Option<(u64, VersionedValue)> {
    inner.writes.get(key).and_then(|versions| {
        versions.iter().find_map(|(commit_version, value)| {
            (value.start_version == start_version).then(|| (*commit_version, value.clone()))
        })
    })
}
