use std::time::{SystemTime, UNIX_EPOCH};

use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{Inner, LockRecord, MvccStore, VersionedValue};

impl MvccStore {
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

pub(crate) fn blocking_lock<'a>(
    inner: &'a Inner,
    key: &[u8],
    read_version: u64,
) -> Option<&'a LockRecord> {
    inner
        .locks
        .get(key)
        .filter(|lock| lock.start_version <= read_version)
}

pub(crate) fn read_committed(inner: &Inner, key: &[u8], version: u64) -> Option<VersionedValue> {
    inner
        .writes
        .get(key)
        .and_then(|versions| versions.range(..=version).next_back())
        .and_then(|(_, value)| {
            if value.kind == kvpb::mutation::Op::Lock || value.kind == kvpb::mutation::Op::Rollback
            {
                return versions_before(inner, key, version, value);
            }
            Some(value.clone())
        })
}

fn versions_before(
    inner: &Inner,
    key: &[u8],
    version: u64,
    skipped: &VersionedValue,
) -> Option<VersionedValue> {
    inner.writes.get(key).and_then(|versions| {
        versions.range(..=version).rev().find_map(|(_, value)| {
            if std::ptr::eq(value, skipped) {
                return None;
            }
            if value.kind == kvpb::mutation::Op::Lock || value.kind == kvpb::mutation::Op::Rollback
            {
                return None;
            }
            Some(value.clone())
        })
    })
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

pub(crate) fn apply_lock(inner: &mut Inner, key: &[u8], lock: LockRecord, commit_version: u64) {
    let mutation = kvpb::Mutation {
        op: lock.op as i32,
        key: key.to_vec(),
        value: lock.value,
        expires_at: lock.expires_at,
        ..Default::default()
    };
    apply_mutation(inner, &mutation, lock.start_version, commit_version);
    inner.locks.remove(key);
}

pub(crate) fn apply_mutation(
    inner: &mut Inner,
    mutation: &kvpb::Mutation,
    start_version: u64,
    commit_version: u64,
) {
    let op = kvpb::mutation::Op::try_from(mutation.op).unwrap_or(kvpb::mutation::Op::Put);
    let value = match op {
        kvpb::mutation::Op::Put | kvpb::mutation::Op::Lock => Some(mutation.value.clone()),
        kvpb::mutation::Op::Delete | kvpb::mutation::Op::Rollback => None,
    };
    inner
        .writes
        .entry(mutation.key.clone())
        .or_default()
        .insert(
            commit_version,
            VersionedValue {
                kind: op,
                start_version,
                value,
                expires_at: mutation.expires_at,
            },
        );
}

pub(crate) fn apply_rollback(inner: &mut Inner, key: &[u8], start_version: u64) {
    inner.writes.entry(key.to_vec()).or_default().insert(
        start_version,
        VersionedValue {
            kind: kvpb::mutation::Op::Rollback,
            start_version,
            value: None,
            expires_at: 0,
        },
    );
    inner.rollbacks.insert((key.to_vec(), start_version));
}

pub(crate) fn current_physical_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn is_lock_expired(lock: &LockRecord, current_time: u64) -> bool {
    lock.ttl != 0
        && lock.start_time != 0
        && current_time != 0
        && current_time >= lock.start_time
        && current_time - lock.start_time >= lock.ttl
}

pub(crate) fn lock_expire_time(lock: &LockRecord) -> u64 {
    if lock.start_time == 0 || lock.ttl == 0 {
        return 0;
    }
    lock.start_time.saturating_add(lock.ttl)
}
