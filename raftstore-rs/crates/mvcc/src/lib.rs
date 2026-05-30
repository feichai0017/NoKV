//! MVCC and Percolator-compatible metadata operations for Rust raftstore.
//!
//! The crate owns the storage-level transaction semantics that `StoreKV` exposes
//! over gRPC. It intentionally does not know fsmeta inode/dentry semantics.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_proto::nokv::kv::v1 as kvpb;

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

#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub kind: kvpb::mutation::Op,
    pub start_version: u64,
    pub value: Option<Vec<u8>>,
    pub expires_at: u64,
}

#[derive(Debug, Clone)]
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

impl MvccStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, req: &kvpb::GetRequest) -> Result<kvpb::GetResponse> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(lock) = blocking_lock(&inner, &req.key, req.version) {
            return Ok(kvpb::GetResponse {
                error: Some(locked_error(&req.key, lock)),
                ..Default::default()
            });
        }
        Ok(match read_committed(&inner, &req.key, req.version) {
            Some(value) => {
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
        let mut kvs = Vec::new();
        let start = req.start_key.as_slice();
        let include_start = req.include_start;
        for (key, versions) in inner.writes.iter() {
            if key.as_slice() < start || (!include_start && key.as_slice() == start) {
                continue;
            }
            if let Some(lock) = blocking_lock(&inner, key, req.version) {
                return Ok(kvpb::ScanResponse {
                    error: Some(locked_error(key, lock)),
                    ..Default::default()
                });
            }
            if let Some((commit_version, value)) = versions.range(..=req.version).next_back() {
                if let Some(bytes) = &value.value {
                    kvs.push(kvpb::Kv {
                        key: key.clone(),
                        value: bytes.clone(),
                        version: *commit_version,
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
            if let Some(existing) = inner.locks.get(&mutation.key) {
                if existing.start_version != req.start_version {
                    errors.push(locked_error(&mutation.key, existing));
                    continue;
                }
            }
            if let Some((commit_ts, _)) = inner
                .writes
                .get(&mutation.key)
                .and_then(|versions| versions.range((req.start_version + 1)..).next())
            {
                errors.push(write_conflict_error(
                    &mutation.key,
                    &req.primary_lock,
                    req.start_version,
                    *commit_ts,
                ));
                continue;
            }
            if mutation.assertion_not_exist
                && read_committed(&inner, &mutation.key, req.start_version)
                    .and_then(|value| value.value)
                    .is_some()
            {
                errors.push(already_exists_error(&mutation.key));
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
        if let Some(err) = validate_commit_version(req.start_version, req.commit_version) {
            return Ok(kvpb::CommitResponse { error: Some(err) });
        }
        let mut locks = Vec::new();
        for key in &req.keys {
            let Some(lock) = inner.locks.get(key).cloned() else {
                if let Some((_commit_version, value)) =
                    write_by_start_version(&inner, key, req.start_version)
                {
                    if value.kind == kvpb::mutation::Op::Rollback {
                        return Ok(kvpb::CommitResponse {
                            error: Some(kvpb::KeyError {
                                abort: "transaction already rolled back".to_owned(),
                                ..Default::default()
                            }),
                        });
                    }
                    continue;
                }
                return Ok(kvpb::CommitResponse {
                    error: Some(kvpb::KeyError {
                        abort: "transaction lock not found".to_owned(),
                        ..Default::default()
                    }),
                });
            };
            if lock.start_version != req.start_version {
                return Ok(kvpb::CommitResponse {
                    error: Some(locked_error(key, &lock)),
                });
            }
            if req.commit_version < lock.min_commit_ts {
                return Ok(kvpb::CommitResponse {
                    error: Some(commit_ts_expired_error(
                        key,
                        req.commit_version,
                        lock.min_commit_ts,
                    )),
                });
            }
            locks.push((key.clone(), lock));
        }
        for (key, lock) in locks {
            apply_lock(&mut inner, &key, lock, req.commit_version);
        }
        Ok(kvpb::CommitResponse::default())
    }

    pub fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> Result<kvpb::BatchRollbackResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
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
        let keys: Vec<Vec<u8>> = if req.keys.is_empty() {
            inner
                .locks
                .iter()
                .filter(|(_, lock)| lock.start_version == req.start_version)
                .map(|(key, _)| key.clone())
                .collect()
        } else {
            req.keys.clone()
        };
        if req.commit_version != 0 {
            if let Some(err) = validate_commit_version(req.start_version, req.commit_version) {
                return Ok(kvpb::ResolveLockResponse {
                    error: Some(err),
                    ..Default::default()
                });
            }
        }
        let mut locks = Vec::new();
        for key in keys {
            let Some(lock) = inner.locks.get(&key).cloned() else {
                continue;
            };
            if lock.start_version != req.start_version {
                continue;
            }
            if req.commit_version != 0 && req.commit_version < lock.min_commit_ts {
                return Ok(kvpb::ResolveLockResponse {
                    error: Some(commit_ts_expired_error(
                        &key,
                        req.commit_version,
                        lock.min_commit_ts,
                    )),
                    ..Default::default()
                });
            }
            locks.push((key, lock));
        }
        let resolved = locks.len() as u64;
        for (key, lock) in locks {
            if req.commit_version == 0 {
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
                    error: Some(locked_error(&req.primary_key, &lock)),
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
        if req.ttl_extension == 0 || req.current_time == 0 {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(kvpb::KeyError {
                    abort: "invalid txn heartbeat request".to_owned(),
                    ..Default::default()
                }),
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
                error: Some(locked_error(&req.primary_key, &lock)),
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
        for predicate in &req.predicates {
            if let Some(lock) = blocking_lock(&inner, &predicate.key, predicate.read_version) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(locked_error(&predicate.key, lock)),
                    ..Default::default()
                });
            }
            let observed = read_committed(&inner, &predicate.key, predicate.read_version)
                .and_then(|value| value.value);
            if !predicate_matches(predicate, observed.as_deref()) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(predicate_error(predicate)),
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
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        let mut applied = 0;
        for tombstone in &req.tombstones {
            match kvpb::internal_entry_tombstone::ColumnFamily::try_from(tombstone.column_family)
                .unwrap_or(kvpb::internal_entry_tombstone::ColumnFamily::Default)
            {
                kvpb::internal_entry_tombstone::ColumnFamily::Default
                | kvpb::internal_entry_tombstone::ColumnFamily::Write => {
                    if let Some(versions) = inner.writes.get_mut(&tombstone.key) {
                        if versions.remove(&tombstone.version).is_some() {
                            applied += 1;
                        }
                    }
                }
            }
        }
        Ok(kvpb::MvccMaintenanceResponse {
            applied_entries: applied,
            ..Default::default()
        })
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

fn validate_commit_version(start_version: u64, commit_version: u64) -> Option<kvpb::KeyError> {
    (commit_version <= start_version).then(|| kvpb::KeyError {
        abort: "commit version must be greater than start version".to_owned(),
        ..Default::default()
    })
}

pub fn predicate_matches(predicate: &kvpb::AtomicPredicate, observed: Option<&[u8]>) -> bool {
    match kvpb::AtomicPredicateKind::try_from(predicate.kind)
        .unwrap_or(kvpb::AtomicPredicateKind::NotExists)
    {
        kvpb::AtomicPredicateKind::NotExists => observed.is_none(),
        kvpb::AtomicPredicateKind::Exists => observed.is_some(),
        kvpb::AtomicPredicateKind::ValueEquals => {
            observed == Some(predicate.expected_value.as_slice())
        }
    }
}

pub fn predicate_error(predicate: &kvpb::AtomicPredicate) -> kvpb::KeyError {
    match kvpb::AtomicPredicateKind::try_from(predicate.kind)
        .unwrap_or(kvpb::AtomicPredicateKind::NotExists)
    {
        kvpb::AtomicPredicateKind::NotExists => already_exists_error(&predicate.key),
        _ => kvpb::KeyError {
            abort: "atomic predicate rejected".to_owned(),
            ..Default::default()
        },
    }
}

pub fn locked_error(key: &[u8], lock: &LockRecord) -> kvpb::KeyError {
    kvpb::KeyError {
        locked: Some(kvpb::Locked {
            primary_lock: lock.primary.clone(),
            key: key.to_vec(),
            lock_version: lock.start_version,
            lock_ttl: lock.ttl,
            lock_type: lock.op as i32,
            min_commit_ts: lock.min_commit_ts,
        }),
        ..Default::default()
    }
}

pub fn write_conflict_error(
    key: &[u8],
    primary: &[u8],
    start_ts: u64,
    commit_ts: u64,
) -> kvpb::KeyError {
    kvpb::KeyError {
        write_conflict: Some(kvpb::WriteConflict {
            key: key.to_vec(),
            primary: primary.to_vec(),
            conflict_ts: commit_ts,
            commit_ts,
            start_ts,
        }),
        ..Default::default()
    }
}

pub fn already_exists_error(key: &[u8]) -> kvpb::KeyError {
    kvpb::KeyError {
        already_exists: Some(kvpb::KeyAlreadyExists { key: key.to_vec() }),
        ..Default::default()
    }
}

pub fn commit_ts_expired_error(key: &[u8], commit_ts: u64, min_commit_ts: u64) -> kvpb::KeyError {
    kvpb::KeyError {
        commit_ts_expired: Some(kvpb::CommitTsExpired {
            key: key.to_vec(),
            commit_ts,
            min_commit_ts,
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(committed.error.unwrap().abort.contains("rolled back"));
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
        assert!(committed.error.unwrap().abort.contains("rolled back"));
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
}
