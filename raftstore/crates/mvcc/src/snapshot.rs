use prost::Message;

use crate::{
    Error, Inner, LockRecord, MvccSnapshot, MvccSnapshotEngine, MvccSnapshotLock,
    MvccSnapshotRollback, MvccSnapshotWrite, MvccStore, Result, VersionedValue,
};
use nokv_proto::nokv::kv::v1 as kvpb;

impl MvccStore {
    pub fn export_snapshot(&self) -> Result<MvccSnapshot> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        Ok(snapshot_from_inner(&inner))
    }

    pub fn install_snapshot(&self, snapshot: MvccSnapshot) -> Result<()> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        *inner = inner_from_snapshot(snapshot)?;
        Ok(())
    }
}

impl MvccSnapshotEngine for MvccStore {
    fn export_mvcc_snapshot(&self) -> Result<MvccSnapshot> {
        self.export_snapshot()
    }

    fn install_mvcc_snapshot(&self, snapshot: MvccSnapshot) -> Result<()> {
        self.install_snapshot(snapshot)
    }
}

pub fn encode_mvcc_snapshot(snapshot: &MvccSnapshot) -> Vec<u8> {
    let payload = SnapshotPayload {
        writes: snapshot
            .writes
            .iter()
            .map(|write| SnapshotWrite {
                key: write.key.clone(),
                commit_version: write.commit_version,
                kind: write.value.kind as i32,
                start_version: write.value.start_version,
                has_value: write.value.value.is_some(),
                value: write.value.value.clone().unwrap_or_default(),
                expires_at: write.value.expires_at,
            })
            .collect(),
        locks: snapshot
            .locks
            .iter()
            .map(|lock| SnapshotLock {
                key: lock.key.clone(),
                primary: lock.lock.primary.clone(),
                start_version: lock.lock.start_version,
                start_time: lock.lock.start_time,
                ttl: lock.lock.ttl,
                min_commit_ts: lock.lock.min_commit_ts,
                op: lock.lock.op as i32,
                value: lock.lock.value.clone(),
                expires_at: lock.lock.expires_at,
            })
            .collect(),
        rollbacks: snapshot
            .rollbacks
            .iter()
            .map(|rollback| SnapshotRollback {
                key: rollback.key.clone(),
                start_version: rollback.start_version,
            })
            .collect(),
    };
    payload.encode_to_vec()
}

pub fn decode_mvcc_snapshot(bytes: &[u8]) -> Result<MvccSnapshot> {
    let payload = SnapshotPayload::decode(bytes).map_err(|err| Error::Decode(err.to_string()))?;
    let mut writes = Vec::with_capacity(payload.writes.len());
    for write in payload.writes {
        let kind = kvpb::mutation::Op::try_from(write.kind).unwrap_or(kvpb::mutation::Op::Put);
        writes.push(MvccSnapshotWrite {
            key: write.key,
            commit_version: write.commit_version,
            value: VersionedValue {
                kind,
                start_version: write.start_version,
                value: write.has_value.then_some(write.value),
                expires_at: write.expires_at,
            },
        });
    }
    let mut locks = Vec::with_capacity(payload.locks.len());
    for lock in payload.locks {
        locks.push(MvccSnapshotLock {
            key: lock.key,
            lock: LockRecord {
                primary: lock.primary,
                start_version: lock.start_version,
                start_time: lock.start_time,
                ttl: lock.ttl,
                min_commit_ts: lock.min_commit_ts,
                op: kvpb::mutation::Op::try_from(lock.op).unwrap_or(kvpb::mutation::Op::Put),
                value: lock.value,
                expires_at: lock.expires_at,
            },
        });
    }
    let rollbacks = payload
        .rollbacks
        .into_iter()
        .map(|rollback| MvccSnapshotRollback {
            key: rollback.key,
            start_version: rollback.start_version,
        })
        .collect();
    Ok(MvccSnapshot {
        writes,
        locks,
        rollbacks,
    })
}

fn snapshot_from_inner(inner: &Inner) -> MvccSnapshot {
    let writes = inner
        .writes
        .iter()
        .flat_map(|(key, versions)| {
            versions
                .iter()
                .map(|(commit_version, value)| MvccSnapshotWrite {
                    key: key.clone(),
                    commit_version: *commit_version,
                    value: value.clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let locks = inner
        .locks
        .iter()
        .map(|(key, lock)| MvccSnapshotLock {
            key: key.clone(),
            lock: lock.clone(),
        })
        .collect();
    let rollbacks = inner
        .rollbacks
        .iter()
        .map(|(key, start_version)| MvccSnapshotRollback {
            key: key.clone(),
            start_version: *start_version,
        })
        .collect();
    MvccSnapshot {
        writes,
        locks,
        rollbacks,
    }
}

fn inner_from_snapshot(snapshot: MvccSnapshot) -> Result<Inner> {
    let mut inner = Inner::default();
    for write in snapshot.writes {
        inner
            .writes
            .entry(write.key.clone())
            .or_default()
            .insert(write.commit_version, write.value.clone());
        if write.value.kind == kvpb::mutation::Op::Rollback {
            inner
                .rollbacks
                .insert((write.key, write.value.start_version));
        }
    }
    for lock in snapshot.locks {
        inner.locks.insert(lock.key, lock.lock);
    }
    for rollback in snapshot.rollbacks {
        inner
            .rollbacks
            .insert((rollback.key, rollback.start_version));
    }
    Ok(inner)
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotPayload {
    #[prost(message, repeated, tag = "1")]
    writes: Vec<SnapshotWrite>,
    #[prost(message, repeated, tag = "2")]
    locks: Vec<SnapshotLock>,
    #[prost(message, repeated, tag = "3")]
    rollbacks: Vec<SnapshotRollback>,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotWrite {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(uint64, tag = "2")]
    commit_version: u64,
    #[prost(int32, tag = "3")]
    kind: i32,
    #[prost(uint64, tag = "4")]
    start_version: u64,
    #[prost(bool, tag = "5")]
    has_value: bool,
    #[prost(bytes = "vec", tag = "6")]
    value: Vec<u8>,
    #[prost(uint64, tag = "7")]
    expires_at: u64,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotLock {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    primary: Vec<u8>,
    #[prost(uint64, tag = "3")]
    start_version: u64,
    #[prost(uint64, tag = "4")]
    start_time: u64,
    #[prost(uint64, tag = "5")]
    ttl: u64,
    #[prost(uint64, tag = "6")]
    min_commit_ts: u64,
    #[prost(int32, tag = "7")]
    op: i32,
    #[prost(bytes = "vec", tag = "8")]
    value: Vec<u8>,
    #[prost(uint64, tag = "9")]
    expires_at: u64,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotRollback {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(uint64, tag = "2")]
    start_version: u64,
}
