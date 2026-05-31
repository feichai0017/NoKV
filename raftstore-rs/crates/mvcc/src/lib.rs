//! MVCC and Percolator-compatible metadata operations for Rust raftstore.
//!
//! The crate owns the storage-level transaction semantics that `StoreKV` exposes
//! over gRPC. It intentionally does not know fsmeta inode/dentry semantics.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_proto::nokv::kv::v1 as kvpb;
use prost::Message;

pub mod errors;
pub mod validation;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("mvcc store mutex poisoned")]
    Poisoned,
    #[error("mvcc backend error: {0}")]
    Backend(String),
    #[error("mvcc decode error: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub kind: kvpb::mutation::Op,
    pub start_version: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockRecord {
    pub primary: Vec<u8>,
    pub start_version: u64,
    pub start_time: u64,
    pub ttl: u64,
    pub min_commit_ts: u64,
    pub op: kvpb::mutation::Op,
    pub value: Vec<u8>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshot {
    pub writes: Vec<MvccSnapshotWrite>,
    pub locks: Vec<MvccSnapshotLock>,
    pub rollbacks: Vec<MvccSnapshotRollback>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshotWrite {
    pub key: Vec<u8>,
    pub commit_version: u64,
    pub value: VersionedValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshotLock {
    pub key: Vec<u8>,
    pub lock: LockRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccSnapshotRollback {
    pub key: Vec<u8>,
    pub start_version: u64,
}

#[derive(Debug, Default)]
struct Inner {
    writes: BTreeMap<Vec<u8>, BTreeMap<u64, VersionedValue>>,
    locks: BTreeMap<Vec<u8>, LockRecord>,
    rollbacks: BTreeSet<(Vec<u8>, u64)>,
}

/// In-memory MVCC implementation used by the first Rust raftstore service
/// slice and by tests. The Holt-backed implementation will reuse the same
/// request semantics and move persistence below this boundary.
#[derive(Debug, Clone, Default)]
pub struct MvccStore {
    inner: Arc<Mutex<Inner>>,
}

pub trait KvEngine: Clone + Send + Sync + 'static {
    fn get(&self, req: &kvpb::GetRequest) -> Result<kvpb::GetResponse>;
    fn batch_get(&self, req: &kvpb::BatchGetRequest) -> Result<kvpb::BatchGetResponse>;
    fn scan(&self, req: &kvpb::ScanRequest) -> Result<kvpb::ScanResponse>;
    fn prewrite(&self, req: &kvpb::PrewriteRequest) -> Result<kvpb::PrewriteResponse>;
    fn commit(&self, req: &kvpb::CommitRequest) -> Result<kvpb::CommitResponse>;
    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> Result<kvpb::BatchRollbackResponse>;
    fn resolve_lock(&self, req: &kvpb::ResolveLockRequest) -> Result<kvpb::ResolveLockResponse>;
    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> Result<kvpb::CheckTxnStatusResponse>;
    fn txn_heartbeat(&self, req: &kvpb::TxnHeartBeatRequest) -> Result<kvpb::TxnHeartBeatResponse>;
    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> Result<kvpb::TryAtomicMutateResponse>;
    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> Result<kvpb::InstallPreparedMvccEntriesResponse>;
    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> Result<kvpb::MvccMaintenanceResponse>;
}

pub trait MvccSnapshotEngine: Clone + Send + Sync + 'static {
    fn export_mvcc_snapshot(&self) -> Result<MvccSnapshot>;
    fn install_mvcc_snapshot(&self, snapshot: MvccSnapshot) -> Result<()>;
}

impl MvccStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, req: &kvpb::GetRequest) -> Result<kvpb::GetResponse> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(lock) = blocking_lock(&inner, &req.key, req.version) {
            return Ok(kvpb::GetResponse {
                error: Some(errors::locked(&req.key, lock)),
                ..Default::default()
            });
        }
        Ok(match read_committed(&inner, &req.key, req.version) {
            Some(value) => {
                if value_is_expired(value.expires_at) {
                    return Ok(kvpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    });
                }
                let not_found = value.value.is_none();
                kvpb::GetResponse {
                    value: value.value.unwrap_or_default(),
                    not_found,
                    expires_at: value.expires_at,
                    ..Default::default()
                }
            }
            None => kvpb::GetResponse {
                not_found: true,
                ..Default::default()
            },
        })
    }

    pub fn batch_get(&self, req: &kvpb::BatchGetRequest) -> Result<kvpb::BatchGetResponse> {
        let mut responses = Vec::with_capacity(req.requests.len());
        for get in &req.requests {
            responses.push(self.get(get)?);
        }
        Ok(kvpb::BatchGetResponse { responses })
    }

    pub fn scan(&self, req: &kvpb::ScanRequest) -> Result<kvpb::ScanResponse> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        let read_version = scan_read_version(req.version);
        let mut kvs = Vec::new();
        let start = req.start_key.as_slice();
        let include_start = req.include_start;
        for key in inner.writes.keys() {
            if key.as_slice() < start || (!include_start && key.as_slice() == start) {
                continue;
            }
            if let Some(lock) = blocking_lock(&inner, key, read_version) {
                return Ok(kvpb::ScanResponse {
                    error: Some(errors::locked(key, lock)),
                    ..Default::default()
                });
            }
            if let Some(value) = read_committed(&inner, key, read_version) {
                if value_is_expired(value.expires_at) {
                    continue;
                }
                if let Some(bytes) = &value.value {
                    kvs.push(kvpb::Kv {
                        key: key.clone(),
                        value: bytes.clone(),
                        version: read_version,
                        expires_at: value.expires_at,
                        ..Default::default()
                    });
                    if req.limit > 0 && kvs.len() >= req.limit as usize {
                        break;
                    }
                }
            }
        }
        Ok(kvpb::ScanResponse {
            kvs,
            ..Default::default()
        })
    }

    pub fn prewrite(&self, req: &kvpb::PrewriteRequest) -> Result<kvpb::PrewriteResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        let mut errors = Vec::new();
        for mutation in &req.mutations {
            if let Some(error) = validation::prewrite_mutation(mutation) {
                errors.push(error);
                continue;
            }
            if let Some(existing) = inner.locks.get(&mutation.key) {
                if existing.start_version != req.start_version {
                    errors.push(errors::locked(&mutation.key, existing));
                    continue;
                }
            }
            if let Some((commit_ts, value)) = inner
                .writes
                .get(&mutation.key)
                .and_then(|versions| versions.range(req.start_version..).next())
            {
                errors.push(errors::write_conflict(
                    &mutation.key,
                    &req.primary_lock,
                    *commit_ts,
                    value.start_version,
                    req.start_version,
                ));
                continue;
            }
            if mutation.assertion_not_exist
                && read_committed(&inner, &mutation.key, req.start_version)
                    .and_then(|value| value.value)
                    .is_some()
            {
                errors.push(errors::already_exists(&mutation.key));
            }
        }
        if !errors.is_empty() {
            return Ok(kvpb::PrewriteResponse { errors });
        }
        for mutation in &req.mutations {
            inner.locks.insert(
                mutation.key.clone(),
                LockRecord {
                    primary: req.primary_lock.clone(),
                    start_version: req.start_version,
                    start_time: current_physical_time_millis(),
                    ttl: req.lock_ttl,
                    min_commit_ts: req.min_commit_ts,
                    op: kvpb::mutation::Op::try_from(mutation.op)
                        .unwrap_or(kvpb::mutation::Op::Put),
                    value: mutation.value.clone(),
                    expires_at: mutation.expires_at,
                },
            );
        }
        Ok(kvpb::PrewriteResponse::default())
    }

    pub fn commit(&self, req: &kvpb::CommitRequest) -> Result<kvpb::CommitResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(err) = validation::commit_version(req.start_version, req.commit_version) {
            return Ok(kvpb::CommitResponse { error: Some(err) });
        }
        let mut locks = Vec::new();
        for key in &req.keys {
            if key.is_empty() {
                return Ok(kvpb::CommitResponse {
                    error: Some(errors::empty_commit_key()),
                });
            }
            let Some(lock) = inner.locks.get(key).cloned() else {
                if let Some((_commit_version, value)) =
                    write_by_start_version(&inner, key, req.start_version)
                {
                    if value.kind == kvpb::mutation::Op::Rollback {
                        return Ok(kvpb::CommitResponse {
                            error: Some(errors::txn_already_rolled_back()),
                        });
                    }
                    continue;
                }
                return Ok(kvpb::CommitResponse {
                    error: Some(errors::txn_lock_not_found()),
                });
            };
            if lock.start_version != req.start_version {
                return Ok(kvpb::CommitResponse {
                    error: Some(errors::locked(key, &lock)),
                });
            }
            if let Some((_commit_version, value)) =
                write_by_start_version(&inner, key, req.start_version)
            {
                if value.kind == kvpb::mutation::Op::Rollback {
                    return Ok(kvpb::CommitResponse {
                        error: Some(errors::txn_already_rolled_back()),
                    });
                }
                locks.push((key.clone(), lock, true));
                continue;
            }
            if req.commit_version < lock.min_commit_ts {
                return Ok(kvpb::CommitResponse {
                    error: Some(errors::commit_ts_expired(
                        key,
                        req.commit_version,
                        lock.min_commit_ts,
                    )),
                });
            }
            locks.push((key.clone(), lock, false));
        }
        for (key, lock, committed) in locks {
            if !committed {
                apply_lock(&mut inner, &key, lock, req.commit_version);
            } else {
                inner.locks.remove(&key);
            }
        }
        Ok(kvpb::CommitResponse::default())
    }

    pub fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> Result<kvpb::BatchRollbackResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if req.keys.iter().any(Vec::is_empty) {
            return Ok(kvpb::BatchRollbackResponse {
                error: Some(errors::empty_rollback_key()),
            });
        }
        for key in &req.keys {
            if write_by_start_version(&inner, key, req.start_version).is_some() {
                continue;
            }
            if inner
                .locks
                .get(key)
                .is_some_and(|lock| lock.start_version == req.start_version)
            {
                inner.locks.remove(key);
            }
            apply_rollback(&mut inner, key, req.start_version);
        }
        Ok(kvpb::BatchRollbackResponse::default())
    }

    pub fn resolve_lock(
        &self,
        req: &kvpb::ResolveLockRequest,
    ) -> Result<kvpb::ResolveLockResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if req.commit_version != 0 {
            if let Some(err) = validation::commit_version(req.start_version, req.commit_version) {
                return Ok(kvpb::ResolveLockResponse {
                    error: Some(err),
                    ..Default::default()
                });
            }
        }
        let keys = validation::resolve_lock_keys(req);
        let mut locks = Vec::new();
        for key in keys {
            let Some(lock) = inner.locks.get(&key).cloned() else {
                continue;
            };
            if lock.start_version != req.start_version {
                continue;
            }
            if let Some((_commit_version, value)) =
                write_by_start_version(&inner, &key, req.start_version)
            {
                if req.commit_version != 0 && value.kind == kvpb::mutation::Op::Rollback {
                    return Ok(kvpb::ResolveLockResponse {
                        error: Some(errors::txn_already_rolled_back()),
                        ..Default::default()
                    });
                }
                locks.push((key, lock, true));
                continue;
            }
            if req.commit_version != 0 {
                if req.commit_version < lock.min_commit_ts {
                    return Ok(kvpb::ResolveLockResponse {
                        error: Some(errors::commit_ts_expired(
                            &key,
                            req.commit_version,
                            lock.min_commit_ts,
                        )),
                        ..Default::default()
                    });
                }
            }
            locks.push((key, lock, false));
        }
        let resolved = locks.len() as u64;
        for (key, lock, already_written) in locks {
            if already_written {
                if req.commit_version != 0 {
                    inner.locks.remove(&key);
                }
            } else if req.commit_version == 0 {
                inner.locks.remove(&key);
                apply_rollback(&mut inner, &key, req.start_version);
            } else {
                apply_lock(&mut inner, &key, lock, req.commit_version);
            }
        }
        Ok(kvpb::ResolveLockResponse {
            resolved_locks: resolved,
            ..Default::default()
        })
    }

    pub fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> Result<kvpb::CheckTxnStatusResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(lock) = inner.locks.get(&req.primary_key).cloned() {
            if lock.start_version == req.lock_ts {
                if is_lock_expired(&lock, req.current_time) {
                    if req.primary_key.is_empty() {
                        return Ok(kvpb::CheckTxnStatusResponse {
                            error: Some(errors::empty_rollback_key()),
                            ..Default::default()
                        });
                    }
                    inner.locks.remove(&req.primary_key);
                    apply_rollback(&mut inner, &req.primary_key, req.lock_ts);
                    return Ok(kvpb::CheckTxnStatusResponse {
                        action: kvpb::CheckTxnStatusAction::CheckTxnStatusTtlExpireRollback as i32,
                        ..Default::default()
                    });
                }
                let mut action = kvpb::CheckTxnStatusAction::CheckTxnStatusNoAction;
                let mut lock_ttl = lock.ttl;
                if req.caller_start_ts > 0 && lock.min_commit_ts < req.caller_start_ts + 1 {
                    if let Some(stored) = inner.locks.get_mut(&req.primary_key) {
                        stored.min_commit_ts = req.caller_start_ts + 1;
                        lock_ttl = stored.ttl;
                    }
                    action = kvpb::CheckTxnStatusAction::CheckTxnStatusMinCommitTsPushed;
                }
                return Ok(kvpb::CheckTxnStatusResponse {
                    lock_ttl,
                    action: action as i32,
                    ..Default::default()
                });
            } else {
                return Ok(kvpb::CheckTxnStatusResponse {
                    error: Some(errors::locked(&req.primary_key, &lock)),
                    ..Default::default()
                });
            }
        }
        if let Some((commit_version, value)) =
            write_by_start_version(&inner, &req.primary_key, req.lock_ts)
        {
            if value.kind == kvpb::mutation::Op::Rollback {
                return Ok(kvpb::CheckTxnStatusResponse {
                    action: kvpb::CheckTxnStatusAction::CheckTxnStatusLockNotExistRollback as i32,
                    ..Default::default()
                });
            }
            return Ok(kvpb::CheckTxnStatusResponse {
                commit_version,
                action: kvpb::CheckTxnStatusAction::CheckTxnStatusNoAction as i32,
                ..Default::default()
            });
        }
        if req.rollback_if_not_exist {
            if req.primary_key.is_empty() {
                return Ok(kvpb::CheckTxnStatusResponse {
                    error: Some(errors::empty_rollback_key()),
                    ..Default::default()
                });
            }
            apply_rollback(&mut inner, &req.primary_key, req.lock_ts);
            return Ok(kvpb::CheckTxnStatusResponse {
                action: kvpb::CheckTxnStatusAction::CheckTxnStatusLockNotExistRollback as i32,
                ..Default::default()
            });
        }
        Ok(kvpb::CheckTxnStatusResponse::default())
    }

    pub fn txn_heartbeat(
        &self,
        req: &kvpb::TxnHeartBeatRequest,
    ) -> Result<kvpb::TxnHeartBeatResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(error) = errors::txn_heartbeat_validation(req) {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        let Some(lock) = inner.locks.get(&req.primary_key).cloned() else {
            if let Some((commit_version, value)) =
                write_by_start_version(&inner, &req.primary_key, req.start_version)
            {
                if value.kind != kvpb::mutation::Op::Rollback {
                    return Ok(kvpb::TxnHeartBeatResponse {
                        commit_version,
                        action: kvpb::TxnHeartBeatAction::TxnHeartBeatNoAction as i32,
                        ..Default::default()
                    });
                }
            }
            apply_rollback(&mut inner, &req.primary_key, req.start_version);
            return Ok(kvpb::TxnHeartBeatResponse {
                action: kvpb::TxnHeartBeatAction::TxnHeartBeatLockNotExistRollback as i32,
                ..Default::default()
            });
        };
        if lock.start_version != req.start_version {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(errors::locked(&req.primary_key, &lock)),
                ..Default::default()
            });
        }
        if lock.primary.as_slice() != req.primary_key.as_slice() {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(errors::txn_heartbeat_primary_mismatch()),
                ..Default::default()
            });
        }
        if is_lock_expired(&lock, req.current_time) {
            inner.locks.remove(&req.primary_key);
            apply_rollback(&mut inner, &req.primary_key, req.start_version);
            return Ok(kvpb::TxnHeartBeatResponse {
                action: kvpb::TxnHeartBeatAction::TxnHeartBeatTtlExpireRollback as i32,
                ..Default::default()
            });
        }
        let desired_ttl = if req.current_time > lock.start_time {
            req.current_time - lock.start_time + req.ttl_extension
        } else {
            req.ttl_extension
        };
        let mut action = kvpb::TxnHeartBeatAction::TxnHeartBeatNoAction;
        let mut updated = lock.clone();
        if desired_ttl > lock.ttl {
            updated.ttl = desired_ttl;
            inner.locks.insert(req.primary_key.clone(), updated.clone());
            action = kvpb::TxnHeartBeatAction::TxnHeartBeatTtlExtended;
        }
        Ok(kvpb::TxnHeartBeatResponse {
            lock_ttl: updated.ttl,
            lock_expire_time: lock_expire_time(&updated),
            action: action as i32,
            ..Default::default()
        })
    }

    pub fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> Result<kvpb::TryAtomicMutateResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(error) = validation::commit_version(req.start_version, req.commit_version) {
            return Ok(kvpb::TryAtomicMutateResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        if atomic_mutate_already_applied(&inner, req) {
            return Ok(kvpb::TryAtomicMutateResponse {
                applied_keys: req.mutations.len() as u64,
                ..Default::default()
            });
        }
        for predicate in &req.predicates {
            if predicate.key.is_empty() {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::empty_mutation_key()),
                    ..Default::default()
                });
            }
            let read_version = if predicate.read_version == 0 {
                req.start_version
            } else {
                predicate.read_version
            };
            if let Some(lock) = blocking_lock(&inner, &predicate.key, read_version) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::locked(&predicate.key, lock)),
                    ..Default::default()
                });
            }
            let observed =
                read_committed(&inner, &predicate.key, read_version).and_then(|value| value.value);
            if let Some(error) =
                validation::atomic_predicate_observation(predicate, observed.as_deref())
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(error),
                    ..Default::default()
                });
            }
        }
        let primary = req
            .mutations
            .first()
            .map(|mutation| mutation.key.as_slice())
            .unwrap_or_default();
        for mutation in &req.mutations {
            if let Some(error) = validation::atomic_mutation(mutation) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(error),
                    ..Default::default()
                });
            }
            if let Some(lock) = inner.locks.get(&mutation.key) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::locked(&mutation.key, lock)),
                    ..Default::default()
                });
            }
            if let Some((commit_ts, value)) = inner
                .writes
                .get(&mutation.key)
                .and_then(|versions| versions.range(req.start_version..).next())
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::write_conflict(
                        &mutation.key,
                        primary,
                        *commit_ts,
                        value.start_version,
                        req.start_version,
                    )),
                    ..Default::default()
                });
            }
            if mutation.assertion_not_exist
                && read_committed(&inner, &mutation.key, req.start_version)
                    .and_then(|value| value.value)
                    .is_some()
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::already_exists(&mutation.key)),
                    ..Default::default()
                });
            }
        }
        let applied = req.mutations.len() as u64;
        for mutation in &req.mutations {
            apply_mutation(&mut inner, mutation, req.start_version, req.commit_version);
        }
        Ok(kvpb::TryAtomicMutateResponse {
            applied_keys: applied,
            ..Default::default()
        })
    }

    pub fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> Result<kvpb::InstallPreparedMvccEntriesResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        let mut applied = 0;
        for entry in &req.entries {
            match kvpb::prepared_mvcc_entry::ColumnFamily::try_from(entry.column_family)
                .unwrap_or(kvpb::prepared_mvcc_entry::ColumnFamily::Default)
            {
                kvpb::prepared_mvcc_entry::ColumnFamily::Default
                | kvpb::prepared_mvcc_entry::ColumnFamily::Write => {
                    inner.writes.entry(entry.key.clone()).or_default().insert(
                        entry.version,
                        VersionedValue {
                            kind: if entry.has_value {
                                kvpb::mutation::Op::Put
                            } else {
                                kvpb::mutation::Op::Delete
                            },
                            start_version: entry.version,
                            value: entry.has_value.then(|| entry.value.clone()),
                            expires_at: entry.expires_at,
                        },
                    );
                    applied += 1;
                }
                kvpb::prepared_mvcc_entry::ColumnFamily::Lock => {
                    inner.locks.remove(&entry.key);
                    applied += 1;
                }
            }
        }
        Ok(kvpb::InstallPreparedMvccEntriesResponse {
            applied_entries: applied,
            commit_version: req.commit_version,
            ..Default::default()
        })
    }

    pub fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> Result<kvpb::MvccMaintenanceResponse> {
        let mut tombstones = Vec::with_capacity(req.tombstones.len());
        for tombstone in &req.tombstones {
            match kvpb::internal_entry_tombstone::ColumnFamily::try_from(tombstone.column_family) {
                Ok(kvpb::internal_entry_tombstone::ColumnFamily::Default)
                | Ok(kvpb::internal_entry_tombstone::ColumnFamily::Write) => {}
                Err(_) => {
                    return Ok(kvpb::MvccMaintenanceResponse {
                        error: Some(maintenance_abort("invalid column family")),
                        ..Default::default()
                    })
                }
            };
            if tombstone.key.is_empty() {
                return Ok(kvpb::MvccMaintenanceResponse {
                    error: Some(maintenance_abort("empty key")),
                    ..Default::default()
                });
            }
            tombstones.push((tombstone.key.clone(), tombstone.version));
        }

        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        for (key, version) in &tombstones {
            if let Some(versions) = inner.writes.get_mut(key) {
                versions.remove(version);
                if versions.is_empty() {
                    inner.writes.remove(key);
                }
            }
        }
        Ok(kvpb::MvccMaintenanceResponse {
            applied_entries: tombstones.len() as u64,
            ..Default::default()
        })
    }

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

impl KvEngine for MvccStore {
    fn get(&self, req: &kvpb::GetRequest) -> Result<kvpb::GetResponse> {
        self.get(req)
    }

    fn batch_get(&self, req: &kvpb::BatchGetRequest) -> Result<kvpb::BatchGetResponse> {
        self.batch_get(req)
    }

    fn scan(&self, req: &kvpb::ScanRequest) -> Result<kvpb::ScanResponse> {
        self.scan(req)
    }

    fn prewrite(&self, req: &kvpb::PrewriteRequest) -> Result<kvpb::PrewriteResponse> {
        self.prewrite(req)
    }

    fn commit(&self, req: &kvpb::CommitRequest) -> Result<kvpb::CommitResponse> {
        self.commit(req)
    }

    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> Result<kvpb::BatchRollbackResponse> {
        self.batch_rollback(req)
    }

    fn resolve_lock(&self, req: &kvpb::ResolveLockRequest) -> Result<kvpb::ResolveLockResponse> {
        self.resolve_lock(req)
    }

    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> Result<kvpb::CheckTxnStatusResponse> {
        self.check_txn_status(req)
    }

    fn txn_heartbeat(&self, req: &kvpb::TxnHeartBeatRequest) -> Result<kvpb::TxnHeartBeatResponse> {
        self.txn_heartbeat(req)
    }

    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> Result<kvpb::TryAtomicMutateResponse> {
        self.try_atomic_mutate(req)
    }

    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> Result<kvpb::InstallPreparedMvccEntriesResponse> {
        self.install_prepared(req)
    }

    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> Result<kvpb::MvccMaintenanceResponse> {
        self.mvcc_maintenance(req)
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

pub fn scan_read_version(version: u64) -> u64 {
    if version == 0 {
        u64::MAX
    } else {
        version
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

fn blocking_lock<'a>(inner: &'a Inner, key: &[u8], read_version: u64) -> Option<&'a LockRecord> {
    inner
        .locks
        .get(key)
        .filter(|lock| lock.start_version <= read_version)
}

fn read_committed(inner: &Inner, key: &[u8], version: u64) -> Option<VersionedValue> {
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

fn write_by_start_version(
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

fn atomic_mutate_already_applied(inner: &Inner, req: &kvpb::TryAtomicMutateRequest) -> bool {
    let mut any_present = false;
    let mut all_present = true;
    for mutation in &req.mutations {
        let Some((commit_version, value)) =
            write_by_start_version(inner, &mutation.key, req.start_version)
        else {
            all_present = false;
            continue;
        };
        any_present = true;
        if commit_version != req.commit_version
            || !validation::atomic_mutation_matches_value(mutation, &value)
        {
            return false;
        }
    }
    any_present && all_present
}

fn apply_lock(inner: &mut Inner, key: &[u8], lock: LockRecord, commit_version: u64) {
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

fn apply_mutation(
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

fn apply_rollback(inner: &mut Inner, key: &[u8], start_version: u64) {
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

fn current_physical_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_lock_expired(lock: &LockRecord, current_time: u64) -> bool {
    lock.ttl != 0
        && lock.start_time != 0
        && current_time != 0
        && current_time >= lock.start_time
        && current_time - lock.start_time >= lock.ttl
}

fn lock_expire_time(lock: &LockRecord) -> u64 {
    if lock.start_time == 0 || lock.ttl == 0 {
        return 0;
    }
    lock.start_time.saturating_add(lock.ttl)
}

fn maintenance_abort(message: &str) -> kvpb::KeyError {
    kvpb::KeyError {
        abort: message.to_owned(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_abort_contains(error: Option<kvpb::KeyError>, needle: &str) {
        let err = error.expect("expected key error");
        assert!(
            err.abort.contains(needle),
            "expected abort containing {needle:?}, got {err:?}"
        );
    }

    #[test]
    fn prewrite_commit_makes_value_visible_at_commit_ts() {
        let store = MvccStore::new();
        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"k".to_vec(),
                start_version: 10,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"k".to_vec()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap();

        let old = store
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 19,
            })
            .unwrap();
        assert!(old.not_found);

        let current = store
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 20,
            })
            .unwrap();
        assert_eq!(current.value, b"v1");
    }

    #[test]
    fn empty_key_txn_requests_abort_without_partial_apply() {
        let store = MvccStore::new();

        let prewrite = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![
                    kvpb::Mutation {
                        key: b"prewrite-valid".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    kvpb::Mutation {
                        key: Vec::new(),
                        value: b"bad".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                ],
                primary_lock: b"prewrite-valid".to_vec(),
                start_version: 10,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(prewrite.errors.len(), 1);
        assert!(prewrite.errors[0].abort.contains("empty key in mutation"));
        let missing_lock = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"prewrite-valid".to_vec()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap();
        assert!(missing_lock
            .error
            .unwrap()
            .retryable
            .contains("lock not found"));

        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"commit-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"commit-valid".to_vec(),
                start_version: 30,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        let commit = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"commit-valid".to_vec(), Vec::new()],
                start_version: 30,
                commit_version: 40,
            })
            .unwrap();
        assert_abort_contains(commit.error, "empty key in commit");
        let not_committed = store
            .get(&kvpb::GetRequest {
                key: b"commit-valid".to_vec(),
                version: 40,
            })
            .unwrap();
        assert!(not_committed.error.unwrap().locked.is_some());

        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"rollback-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"rollback-valid".to_vec(),
                start_version: 50,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        let rollback = store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"rollback-valid".to_vec(), Vec::new()],
                start_version: 50,
            })
            .unwrap();
        assert_abort_contains(rollback.error, "empty key in rollback");
        let still_committable = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"rollback-valid".to_vec()],
                start_version: 50,
                commit_version: 60,
            })
            .unwrap();
        assert!(still_committable.error.is_none());
    }

    #[test]
    fn empty_key_atomic_mutate_aborts_without_partial_apply() {
        let store = MvccStore::new();

        let empty_predicate = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: Vec::new(),
                    kind: kvpb::AtomicPredicateKind::NotExists as i32,
                    read_version: 1,
                    ..Default::default()
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"predicate-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 2,
            })
            .unwrap();
        assert_abort_contains(empty_predicate.error, "empty key in mutation");
        assert!(
            store
                .get(&kvpb::GetRequest {
                    key: b"predicate-valid".to_vec(),
                    version: 2,
                })
                .unwrap()
                .not_found
        );

        let empty_mutation = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![
                    kvpb::Mutation {
                        key: b"mutation-valid".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    kvpb::Mutation {
                        key: Vec::new(),
                        value: b"bad".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                ],
                predicates: Vec::new(),
                start_version: 3,
                commit_version: 4,
            })
            .unwrap();
        assert_abort_contains(empty_mutation.error, "empty key in mutation");
        assert!(
            store
                .get(&kvpb::GetRequest {
                    key: b"mutation-valid".to_vec(),
                    version: 4,
                })
                .unwrap()
                .not_found
        );
    }

    #[test]
    fn atomic_mutate_rejects_invalid_commit_version_before_apply() {
        let store = MvccStore::new();

        let result = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"invalid-commit-version".to_vec(),
                    value: b"bad".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 10,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();
        assert_abort_contains(result.error, "greater than start version");
        assert!(
            store
                .get(&kvpb::GetRequest {
                    key: b"invalid-commit-version".to_vec(),
                    version: 11,
                })
                .unwrap()
                .not_found
        );
    }

    #[test]
    fn scan_reports_read_version_and_skips_marker_writes() {
        let store = MvccStore::new();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();
        store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"k".to_vec()],
                start_version: 20,
            })
            .unwrap();

        let scan = store
            .scan(&kvpb::ScanRequest {
                start_key: b"k".to_vec(),
                limit: 1,
                version: 30,
                include_start: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(scan.kvs.len(), 1);
        assert_eq!(scan.kvs[0].key, b"k");
        assert_eq!(scan.kvs[0].value, b"v1");
        assert_eq!(scan.kvs[0].version, 30);

        let latest = store
            .scan(&kvpb::ScanRequest {
                start_key: b"k".to_vec(),
                limit: 1,
                include_start: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(latest.kvs.len(), 1);
        assert_eq!(latest.kvs[0].version, u64::MAX);
    }

    #[test]
    fn expired_values_are_not_visible_to_get_or_scan() {
        let store = MvccStore::new();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"expired".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    expires_at: 1,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();

        let got = store
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 20,
            })
            .unwrap();
        assert!(got.not_found);

        let scan = store
            .scan(&kvpb::ScanRequest {
                start_key: b"k".to_vec(),
                limit: 1,
                version: 20,
                include_start: true,
                ..Default::default()
            })
            .unwrap();
        assert!(scan.kvs.is_empty());
    }

    #[test]
    fn atomic_not_exists_rejects_existing_key() {
        let store = MvccStore::new();
        let first = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: b"k".to_vec(),
                    kind: kvpb::AtomicPredicateKind::NotExists as i32,
                    read_version: 1,
                    ..Default::default()
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 2,
            })
            .unwrap();
        assert_eq!(first.applied_keys, 1);

        let second = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: b"k".to_vec(),
                    kind: kvpb::AtomicPredicateKind::NotExists as i32,
                    read_version: 2,
                    ..Default::default()
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v2".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 3,
                commit_version: 4,
            })
            .unwrap();
        assert!(second.error.unwrap().already_exists.is_some());
    }

    #[test]
    fn atomic_mutate_matches_go_validation_and_idempotency() {
        let store = MvccStore::new();
        let request = kvpb::TryAtomicMutateRequest {
            predicates: vec![kvpb::AtomicPredicate {
                key: b"atomic-idempotent".to_vec(),
                kind: kvpb::AtomicPredicateKind::NotExists as i32,
                ..Default::default()
            }],
            mutations: vec![kvpb::Mutation {
                key: b"atomic-idempotent".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 10,
            commit_version: 11,
        };
        let first = store.try_atomic_mutate(&request).unwrap();
        assert_eq!(first.applied_keys, 1);
        let retry = store.try_atomic_mutate(&request).unwrap();
        assert_eq!(retry.applied_keys, 1);
        assert!(retry.error.is_none());

        let mismatch = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: b"atomic-idempotent".to_vec(),
                    kind: kvpb::AtomicPredicateKind::ValueEquals as i32,
                    expected_value: b"old".to_vec(),
                    read_version: 11,
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"atomic-idempotent".to_vec(),
                    value: b"bad".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 12,
                commit_version: 13,
            })
            .unwrap();
        assert!(mismatch
            .error
            .unwrap()
            .retryable
            .contains("atomic predicate mismatch"));

        let unsupported = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"atomic-lock".to_vec(),
                    value: b"bad".to_vec(),
                    op: kvpb::mutation::Op::Lock as i32,
                    ..Default::default()
                }],
                start_version: 14,
                commit_version: 15,
                ..Default::default()
            })
            .unwrap();
        assert_abort_contains(unsupported.error, "unsupported mutation op");
    }

    #[test]
    fn resolve_lock_matches_go_key_set_boundary() {
        let store = MvccStore::new();
        let key = b"resolve-key-boundary".to_vec();
        let prewrite = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.clone(),
                    value: b"resolve-value".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: key.clone(),
                start_version: 40,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        assert!(prewrite.errors.is_empty());

        let empty = store
            .resolve_lock(&kvpb::ResolveLockRequest {
                start_version: 40,
                commit_version: 50,
                ..Default::default()
            })
            .unwrap();
        assert!(empty.error.is_none());
        assert_eq!(empty.resolved_locks, 0);

        let duplicate = store
            .resolve_lock(&kvpb::ResolveLockRequest {
                keys: vec![Vec::new(), key.clone(), key.clone()],
                start_version: 40,
                commit_version: 50,
            })
            .unwrap();
        assert!(duplicate.error.is_none());
        assert_eq!(duplicate.resolved_locks, 1);

        let retry = store
            .resolve_lock(&kvpb::ResolveLockRequest {
                keys: vec![key.clone()],
                start_version: 40,
                commit_version: 50,
            })
            .unwrap();
        assert!(retry.error.is_none());
        assert_eq!(retry.resolved_locks, 0);

        let got = store
            .get(&kvpb::GetRequest {
                key: key.clone(),
                version: 60,
            })
            .unwrap();
        assert_eq!(got.value, b"resolve-value");
    }

    #[test]
    fn resolve_lock_commit_matches_go_lingering_lock_boundary() {
        let store = MvccStore::new();
        let key = b"resolve-lingering-lock".to_vec();
        store
            .install_snapshot(MvccSnapshot {
                writes: vec![MvccSnapshotWrite {
                    key: key.clone(),
                    commit_version: 50,
                    value: VersionedValue {
                        kind: kvpb::mutation::Op::Put,
                        start_version: 45,
                        value: Some(b"committed".to_vec()),
                        expires_at: 0,
                    },
                }],
                locks: vec![MvccSnapshotLock {
                    key: key.clone(),
                    lock: LockRecord {
                        primary: key.clone(),
                        start_version: 45,
                        start_time: 1,
                        ttl: 10_000,
                        min_commit_ts: 0,
                        op: kvpb::mutation::Op::Put,
                        value: b"stale-lock".to_vec(),
                        expires_at: 0,
                    },
                }],
                rollbacks: Vec::new(),
            })
            .unwrap();

        let resolved = store
            .resolve_lock(&kvpb::ResolveLockRequest {
                keys: vec![key.clone()],
                start_version: 45,
                commit_version: 60,
            })
            .unwrap();
        assert!(resolved.error.is_none());
        assert_eq!(resolved.resolved_locks, 1);
        let snapshot = store.export_snapshot().unwrap();
        assert!(snapshot.locks.is_empty());
        assert!(snapshot
            .writes
            .iter()
            .all(|write| write.commit_version != 60));
        let got = store.get(&kvpb::GetRequest { key, version: 70 }).unwrap();
        assert_eq!(got.value, b"committed");
    }

    #[test]
    fn prewrite_rejects_unsupported_ops_without_partial_apply() {
        let store = MvccStore::new();
        let valid_key = b"prewrite-valid-before-unsupported".to_vec();
        let invalid_key = b"prewrite-unsupported".to_vec();
        let response = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![
                    kvpb::Mutation {
                        key: valid_key.clone(),
                        value: b"valid".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    kvpb::Mutation {
                        key: invalid_key,
                        op: kvpb::mutation::Op::Rollback as i32,
                        ..Default::default()
                    },
                ],
                primary_lock: valid_key.clone(),
                start_version: 70,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(response.errors.len(), 1);
        assert_abort_contains(
            response.errors.into_iter().next(),
            "unsupported mutation op",
        );

        let got = store
            .get(&kvpb::GetRequest {
                key: valid_key,
                version: 80,
            })
            .unwrap();
        assert!(got.not_found);
    }

    #[test]
    fn prewrite_write_conflict_matches_go_fields_and_rollback_fence() {
        let store = MvccStore::new();
        let key = b"prewrite-conflict-fields".to_vec();
        assert!(store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.clone(),
                    value: b"old".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: key.clone(),
                start_version: 10,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap()
            .errors
            .is_empty());
        assert!(store
            .commit(&kvpb::CommitRequest {
                keys: vec![key.clone()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap()
            .error
            .is_none());

        let conflict = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.clone(),
                    value: b"new".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: key,
                start_version: 15,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        let conflict = conflict.errors[0].write_conflict.as_ref().unwrap();
        assert_eq!(conflict.conflict_ts, 20);
        assert_eq!(conflict.start_ts, 10);
        assert_eq!(conflict.commit_ts, 15);

        let rollback_key = b"prewrite-rollback-fence".to_vec();
        assert!(store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![rollback_key.clone()],
                start_version: 30,
            })
            .unwrap()
            .error
            .is_none());
        let fenced = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: rollback_key,
                    value: b"new".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"prewrite-rollback-fence".to_vec(),
                start_version: 30,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        let fenced = fenced.errors[0].write_conflict.as_ref().unwrap();
        assert_eq!(fenced.conflict_ts, 30);
        assert_eq!(fenced.start_ts, 30);
        assert_eq!(fenced.commit_ts, 30);
    }

    #[test]
    fn commit_matches_go_missing_lock_and_lingering_lock_boundaries() {
        let store = MvccStore::new();
        let missing = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"commit-missing-lock".to_vec()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap();
        assert!(missing.error.unwrap().retryable.contains("lock not found"));

        let rollback_key = b"commit-rolled-back".to_vec();
        assert!(store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![rollback_key.clone()],
                start_version: 30,
            })
            .unwrap()
            .error
            .is_none());
        let rolled_back = store
            .commit(&kvpb::CommitRequest {
                keys: vec![rollback_key],
                start_version: 30,
                commit_version: 40,
            })
            .unwrap();
        assert!(rolled_back
            .error
            .unwrap()
            .retryable
            .contains("transaction already rolled back"));

        let lingering = b"commit-lingering-lock".to_vec();
        store
            .install_snapshot(MvccSnapshot {
                writes: vec![MvccSnapshotWrite {
                    key: lingering.clone(),
                    commit_version: 50,
                    value: VersionedValue {
                        kind: kvpb::mutation::Op::Put,
                        start_version: 45,
                        value: Some(b"committed".to_vec()),
                        expires_at: 0,
                    },
                }],
                locks: vec![MvccSnapshotLock {
                    key: lingering.clone(),
                    lock: LockRecord {
                        primary: lingering.clone(),
                        start_version: 45,
                        start_time: 1,
                        ttl: 10_000,
                        min_commit_ts: 0,
                        op: kvpb::mutation::Op::Put,
                        value: b"stale-lock".to_vec(),
                        expires_at: 0,
                    },
                }],
                rollbacks: Vec::new(),
            })
            .unwrap();
        assert!(store
            .commit(&kvpb::CommitRequest {
                keys: vec![lingering.clone()],
                start_version: 45,
                commit_version: 60,
            })
            .unwrap()
            .error
            .is_none());
        let snapshot = store.export_snapshot().unwrap();
        assert!(snapshot.locks.is_empty());
        assert!(snapshot
            .writes
            .iter()
            .all(|write| write.commit_version != 60));
        let got = store
            .get(&kvpb::GetRequest {
                key: lingering,
                version: 70,
            })
            .unwrap();
        assert_eq!(got.value, b"committed");
    }

    #[test]
    fn mvcc_maintenance_counts_requested_tombstones() {
        let store = MvccStore::new();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"maint/k".to_vec(),
                    value: b"value".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 2,
                ..Default::default()
            })
            .unwrap();

        let response = store
            .mvcc_maintenance(&kvpb::MvccMaintenanceRequest {
                tombstones: vec![
                    kvpb::InternalEntryTombstone {
                        column_family: kvpb::internal_entry_tombstone::ColumnFamily::Write as i32,
                        key: b"maint/k".to_vec(),
                        version: 2,
                    },
                    kvpb::InternalEntryTombstone {
                        column_family: kvpb::internal_entry_tombstone::ColumnFamily::Default as i32,
                        key: b"maint/missing".to_vec(),
                        version: 9,
                    },
                ],
            })
            .unwrap();
        assert_eq!(response.applied_entries, 2);
        assert!(response.error.is_none());

        let got = store
            .get(&kvpb::GetRequest {
                key: b"maint/k".to_vec(),
                version: 2,
            })
            .unwrap();
        assert!(got.not_found);
    }

    #[test]
    fn mvcc_maintenance_rejects_malformed_batch_without_partial_apply() {
        let store = MvccStore::new();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"maint/keep".to_vec(),
                    value: b"value".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 2,
                ..Default::default()
            })
            .unwrap();

        let response = store
            .mvcc_maintenance(&kvpb::MvccMaintenanceRequest {
                tombstones: vec![
                    kvpb::InternalEntryTombstone {
                        column_family: 99,
                        key: b"maint/bad".to_vec(),
                        version: 1,
                    },
                    kvpb::InternalEntryTombstone {
                        column_family: kvpb::internal_entry_tombstone::ColumnFamily::Write as i32,
                        key: b"maint/keep".to_vec(),
                        version: 2,
                    },
                ],
            })
            .unwrap();
        assert!(response.error.unwrap().abort.contains("column family"));

        let got = store
            .get(&kvpb::GetRequest {
                key: b"maint/keep".to_vec(),
                version: 2,
            })
            .unwrap();
        assert_eq!(got.value, b"value");

        let empty_key = store
            .mvcc_maintenance(&kvpb::MvccMaintenanceRequest {
                tombstones: vec![kvpb::InternalEntryTombstone {
                    column_family: kvpb::internal_entry_tombstone::ColumnFamily::Write as i32,
                    key: Vec::new(),
                    version: 2,
                }],
            })
            .unwrap();
        assert!(empty_key.error.unwrap().abort.contains("empty key"));
    }

    #[test]
    fn rollback_marker_does_not_hide_older_visible_put() {
        let store = MvccStore::new();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();
        store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"k".to_vec()],
                start_version: 20,
            })
            .unwrap();

        let current = store
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 20,
            })
            .unwrap();
        assert!(!current.not_found);
        assert_eq!(current.value, b"v1");
    }

    #[test]
    fn snapshot_round_trips_writes_locks_and_rollbacks() {
        let store = MvccStore::new();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();
        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"locked".to_vec(),
                    value: b"pending".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"locked".to_vec(),
                start_version: 20,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"rolled-back".to_vec()],
                start_version: 30,
            })
            .unwrap();

        let encoded = encode_mvcc_snapshot(&store.export_snapshot().unwrap());
        let restored = MvccStore::new();
        restored
            .install_snapshot(decode_mvcc_snapshot(&encoded).unwrap())
            .unwrap();

        let committed = restored
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 10,
            })
            .unwrap();
        assert_eq!(committed.value, b"v1");

        let locked = restored
            .get(&kvpb::GetRequest {
                key: b"locked".to_vec(),
                version: 20,
            })
            .unwrap();
        assert!(locked.error.unwrap().locked.is_some());

        let rolled_back = restored
            .commit(&kvpb::CommitRequest {
                keys: vec![b"rolled-back".to_vec()],
                start_version: 30,
                commit_version: 40,
            })
            .unwrap();
        assert!(rolled_back.error.unwrap().retryable.contains("rolled back"));
    }

    #[test]
    fn commit_after_rollback_is_rejected() {
        let store = MvccStore::new();
        store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"k".to_vec()],
                start_version: 10,
            })
            .unwrap();

        let committed = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"k".to_vec()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap();
        assert!(committed.error.unwrap().retryable.contains("rolled back"));
    }

    #[test]
    fn check_txn_status_ttl_expire_rolls_back_primary() {
        let store = MvccStore::new();
        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"k".to_vec(),
                start_version: 10,
                lock_ttl: 1,
                ..Default::default()
            })
            .unwrap();

        let status = store
            .check_txn_status(&kvpb::CheckTxnStatusRequest {
                primary_key: b"k".to_vec(),
                lock_ts: 10,
                current_time: u64::MAX,
                rollback_if_not_exist: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            status.action,
            kvpb::CheckTxnStatusAction::CheckTxnStatusTtlExpireRollback as i32
        );

        let committed = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"k".to_vec()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap();
        assert!(committed.error.unwrap().retryable.contains("rolled back"));
    }

    #[test]
    fn check_txn_status_empty_primary_rollback_aborts_without_marker() {
        let store = MvccStore::new();
        let status = store
            .check_txn_status(&kvpb::CheckTxnStatusRequest {
                primary_key: Vec::new(),
                lock_ts: 10,
                current_time: 1,
                rollback_if_not_exist: true,
                ..Default::default()
            })
            .unwrap();
        assert_abort_contains(status.error, "empty key in rollback");

        let snapshot = store.export_mvcc_snapshot().unwrap();
        assert!(snapshot
            .rollbacks
            .iter()
            .all(|rollback| !rollback.key.is_empty()));
    }

    #[test]
    fn txn_heartbeat_extends_ttl_and_reports_expiry() {
        let store = MvccStore::new();
        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"k".to_vec(),
                start_version: 10,
                lock_ttl: 10,
                ..Default::default()
            })
            .unwrap();

        let heartbeat = store
            .txn_heartbeat(&kvpb::TxnHeartBeatRequest {
                primary_key: b"k".to_vec(),
                start_version: 10,
                ttl_extension: 100,
                current_time: current_physical_time_millis(),
            })
            .unwrap();
        assert!(heartbeat.lock_ttl >= 100);
        assert!(heartbeat.lock_expire_time > 0);
    }

    #[test]
    fn txn_heartbeat_validates_request_like_go_percolator() {
        let store = MvccStore::new();
        let cases = [
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: Vec::new(),
                    start_version: 10,
                    ttl_extension: 1,
                    current_time: 1,
                },
                "heartbeat primary key is required",
            ),
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: b"k".to_vec(),
                    start_version: 0,
                    ttl_extension: 1,
                    current_time: 1,
                },
                "heartbeat start version is required",
            ),
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: b"k".to_vec(),
                    start_version: 10,
                    ttl_extension: 0,
                    current_time: 1,
                },
                "heartbeat ttl extension is required",
            ),
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: b"k".to_vec(),
                    start_version: 10,
                    ttl_extension: 1,
                    current_time: 0,
                },
                "heartbeat current time is required",
            ),
        ];
        for (request, needle) in cases {
            let heartbeat = store.txn_heartbeat(&request).unwrap();
            assert_abort_contains(heartbeat.error, needle);
        }
    }

    #[test]
    fn txn_heartbeat_rejects_secondary_lock_primary_mismatch() {
        let store = MvccStore::new();
        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"secondary".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"primary".to_vec(),
                start_version: 10,
                lock_ttl: 10,
                ..Default::default()
            })
            .unwrap();

        let heartbeat = store
            .txn_heartbeat(&kvpb::TxnHeartBeatRequest {
                primary_key: b"secondary".to_vec(),
                start_version: 10,
                ttl_extension: 100,
                current_time: current_physical_time_millis(),
            })
            .unwrap();
        assert_abort_contains(heartbeat.error, "primary key does not match lock primary");
    }
}
