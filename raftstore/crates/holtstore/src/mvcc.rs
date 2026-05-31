use std::time::{SystemTime, UNIX_EPOCH};

use holt::RangeEntry;
use nokv_mvcc as mvcc;
use nokv_proto::nokv::kv::v1 as kvpb;

use crate::codec::{decode_lock, decode_value, encode_lock, encode_value};
use crate::store::to_backend_error;
use crate::trees::{decode_write_key, write_key, write_prefix, DATA_TREE, LOCK_TREE, WRITE_TREE};
use crate::HoltMvccStore;

impl HoltMvccStore {
    pub(crate) fn get_lock(&self, key: &[u8]) -> mvcc::Result<Option<mvcc::LockRecord>> {
        let Some(bytes) = self
            .store
            .lock()
            .map_err(to_backend_error)?
            .get(key)
            .map_err(to_backend_error)?
        else {
            return Ok(None);
        };
        decode_lock(&bytes).map(Some)
    }

    pub(crate) fn read_committed(
        &self,
        key: &[u8],
        version: u64,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        let prefix = write_prefix(key);
        let mut best = None;
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            if commit_ts <= version {
                let decoded = decode_value(&value)?;
                if decoded.kind == kvpb::mutation::Op::Lock
                    || decoded.kind == kvpb::mutation::Op::Rollback
                {
                    continue;
                }
                if best.as_ref().is_none_or(|(ts, _)| commit_ts > *ts) {
                    best = Some((commit_ts, decoded));
                }
            }
        }
        Ok(best)
    }

    pub(crate) fn write_by_start_version(
        &self,
        key: &[u8],
        start_version: u64,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        let prefix = write_prefix(key);
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            let decoded = decode_value(&value)?;
            if decoded.start_version == start_version {
                return Ok(Some((commit_ts, decoded)));
            }
        }
        Ok(None)
    }

    pub(crate) fn first_write_after_or_at(
        &self,
        key: &[u8],
        version: u64,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        self.first_write_matching(key, |commit_ts| commit_ts >= version)
    }

    fn first_write_matching(
        &self,
        key: &[u8],
        matches: impl Fn(u64) -> bool,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        let prefix = write_prefix(key);
        let mut best = None;
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            if matches(commit_ts) && best.as_ref().is_none_or(|(ts, _)| commit_ts < *ts) {
                best = Some((commit_ts, decode_value(&value)?));
            }
        }
        Ok(best)
    }

    fn atomic_mutate_already_applied(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> mvcc::Result<bool> {
        let mut any_present = false;
        let mut all_present = true;
        for mutation in &req.mutations {
            let Some((commit_version, value)) =
                self.write_by_start_version(&mutation.key, req.start_version)?
            else {
                all_present = false;
                continue;
            };
            any_present = true;
            if commit_version != req.commit_version
                || !mvcc::validation::atomic_mutation_matches_value(mutation, &value)
            {
                return Ok(false);
            }
        }
        Ok(any_present && all_present)
    }

    fn scan_write_user_keys(&self) -> mvcc::Result<Vec<Vec<u8>>> {
        let mut keys = std::collections::BTreeSet::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, .. } = entry else {
                continue;
            };
            if let Some((user_key, _commit_ts)) = decode_write_key(&key)? {
                keys.insert(user_key);
            }
        }
        Ok(keys.into_iter().collect())
    }
}

impl mvcc::KvEngine for HoltMvccStore {
    fn get(&self, req: &kvpb::GetRequest) -> mvcc::Result<kvpb::GetResponse> {
        let _guard = self.lock()?;
        if let Some(lock) = self.get_lock(&req.key)? {
            if lock.start_version <= req.version {
                return Ok(kvpb::GetResponse {
                    error: Some(mvcc::errors::locked(&req.key, &lock)),
                    ..Default::default()
                });
            }
        }
        Ok(match self.read_committed(&req.key, req.version)? {
            Some((_commit, value)) => {
                if mvcc::value_is_expired(value.expires_at) {
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

    fn batch_get(&self, req: &kvpb::BatchGetRequest) -> mvcc::Result<kvpb::BatchGetResponse> {
        let mut responses = Vec::with_capacity(req.requests.len());
        for get in &req.requests {
            responses.push(self.get(get)?);
        }
        Ok(kvpb::BatchGetResponse { responses })
    }

    fn scan(&self, req: &kvpb::ScanRequest) -> mvcc::Result<kvpb::ScanResponse> {
        let _guard = self.lock()?;
        let read_version = mvcc::scan_read_version(req.version);
        let limit = mvcc::scan_limit(req.limit);
        let mut keys = self.scan_write_user_keys()?;
        if req.reverse {
            keys.reverse();
        }
        let mut kvs = Vec::new();
        for key in keys {
            if !req.reverse
                && (key.as_slice() < req.start_key.as_slice()
                    || (!req.include_start && key == req.start_key))
            {
                continue;
            }
            if req.reverse
                && (key.as_slice() > req.start_key.as_slice()
                    || (!req.include_start && key == req.start_key))
            {
                continue;
            }
            if let Some(lock) = self.get_lock(&key)? {
                if lock.start_version <= read_version {
                    return Ok(kvpb::ScanResponse {
                        error: Some(mvcc::errors::locked(&key, &lock)),
                        ..Default::default()
                    });
                }
            }
            if let Some((_commit_version, value)) = self.read_committed(&key, read_version)? {
                if mvcc::value_is_expired(value.expires_at) {
                    continue;
                }
                if let Some(bytes) = value.value {
                    kvs.push(kvpb::Kv {
                        key,
                        value: bytes,
                        version: read_version,
                        expires_at: value.expires_at,
                        ..Default::default()
                    });
                    if kvs.len() >= limit {
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

    fn prewrite(&self, req: &kvpb::PrewriteRequest) -> mvcc::Result<kvpb::PrewriteResponse> {
        let _guard = self.lock()?;
        let mut errors = Vec::new();
        for mutation in &req.mutations {
            if let Some(error) = mvcc::validation::prewrite_mutation(mutation) {
                errors.push(error);
                continue;
            }
            if let Some(existing) = self.get_lock(&mutation.key)? {
                if existing.start_version != req.start_version {
                    errors.push(mvcc::errors::locked(&mutation.key, &existing));
                    continue;
                }
            }
            if let Some((commit_ts, value)) =
                self.first_write_after_or_at(&mutation.key, req.start_version)?
            {
                errors.push(mvcc::errors::write_conflict(
                    &mutation.key,
                    &req.primary_lock,
                    commit_ts,
                    value.start_version,
                    req.start_version,
                ));
                continue;
            }
            if mutation.assertion_not_exist
                && self
                    .read_committed(&mutation.key, req.start_version)?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                errors.push(mvcc::errors::already_exists(&mutation.key));
            }
        }
        if !errors.is_empty() {
            return Ok(kvpb::PrewriteResponse { errors });
        }
        let locks = req
            .mutations
            .iter()
            .map(|mutation| {
                let op =
                    kvpb::mutation::Op::try_from(mutation.op).unwrap_or(kvpb::mutation::Op::Put);
                let lock = mvcc::LockRecord {
                    primary: req.primary_lock.clone(),
                    start_version: req.start_version,
                    start_time: current_physical_time_millis(),
                    ttl: req.lock_ttl,
                    min_commit_ts: req.min_commit_ts,
                    op,
                    value: mutation.value.clone(),
                    expires_at: mutation.expires_at,
                };
                encode_lock(&lock).map(|encoded| (mutation.key.clone(), encoded))
            })
            .collect::<mvcc::Result<Vec<_>>>()?;
        self.atomic(|batch| {
            for (key, encoded) in &locks {
                batch.put(LOCK_TREE, key, encoded);
            }
        })?;
        Ok(kvpb::PrewriteResponse::default())
    }

    fn commit(&self, req: &kvpb::CommitRequest) -> mvcc::Result<kvpb::CommitResponse> {
        let _guard = self.lock()?;
        if let Some(err) = mvcc::validation::commit_version(req.start_version, req.commit_version) {
            return Ok(kvpb::CommitResponse { error: Some(err) });
        }
        let mut locks = Vec::new();
        for key in &req.keys {
            if key.is_empty() {
                return Ok(kvpb::CommitResponse {
                    error: Some(mvcc::errors::empty_commit_key()),
                });
            }
            let Some(lock) = self.get_lock(key)? else {
                if let Some((_commit_version, value)) =
                    self.write_by_start_version(key, req.start_version)?
                {
                    if value.kind == kvpb::mutation::Op::Rollback {
                        return Ok(kvpb::CommitResponse {
                            error: Some(mvcc::errors::txn_already_rolled_back()),
                        });
                    }
                    continue;
                }
                return Ok(kvpb::CommitResponse {
                    error: Some(mvcc::errors::txn_lock_not_found()),
                });
            };
            if lock.start_version != req.start_version {
                return Ok(kvpb::CommitResponse {
                    error: Some(mvcc::errors::locked(key, &lock)),
                });
            }
            if let Some((_commit_version, value)) =
                self.write_by_start_version(key, req.start_version)?
            {
                if value.kind == kvpb::mutation::Op::Rollback {
                    return Ok(kvpb::CommitResponse {
                        error: Some(mvcc::errors::txn_already_rolled_back()),
                    });
                }
                locks.push((key.clone(), lock.clone(), true));
                continue;
            }
            if req.commit_version < lock.min_commit_ts {
                return Ok(kvpb::CommitResponse {
                    error: Some(mvcc::errors::commit_ts_expired(
                        key,
                        req.commit_version,
                        lock.min_commit_ts,
                    )),
                });
            }
            locks.push((key.clone(), lock, false));
        }
        self.atomic(|batch| {
            for (key, lock, committed) in &locks {
                if !committed {
                    let value = lock_value(lock);
                    apply_committed(batch, key, req.commit_version, &value);
                }
                batch.delete(LOCK_TREE, key);
            }
        })?;
        Ok(kvpb::CommitResponse::default())
    }

    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> mvcc::Result<kvpb::BatchRollbackResponse> {
        let _guard = self.lock()?;
        if req.keys.iter().any(Vec::is_empty) {
            return Ok(kvpb::BatchRollbackResponse {
                error: Some(mvcc::errors::empty_rollback_key()),
            });
        }
        let mut delete_locks = Vec::new();
        let mut rollbacks = Vec::new();
        for key in &req.keys {
            if self
                .write_by_start_version(key, req.start_version)?
                .is_some()
            {
                continue;
            }
            if self
                .get_lock(key)?
                .is_some_and(|lock| lock.start_version == req.start_version)
            {
                delete_locks.push(key.clone());
            }
            rollbacks.push(key.clone());
        }
        self.atomic(|batch| {
            for key in &delete_locks {
                batch.delete(LOCK_TREE, key);
            }
            for key in &rollbacks {
                let value = rollback_value(req.start_version);
                apply_committed(batch, key, req.start_version, &value);
            }
        })?;
        Ok(kvpb::BatchRollbackResponse::default())
    }

    fn resolve_lock(
        &self,
        req: &kvpb::ResolveLockRequest,
    ) -> mvcc::Result<kvpb::ResolveLockResponse> {
        let _guard = self.lock()?;
        if req.commit_version != 0 {
            if let Some(err) =
                mvcc::validation::commit_version(req.start_version, req.commit_version)
            {
                return Ok(kvpb::ResolveLockResponse {
                    error: Some(err),
                    ..Default::default()
                });
            }
        }
        let keys = mvcc::validation::resolve_lock_keys(req);
        let mut locks = Vec::new();
        for key in keys {
            let Some(lock) = self.get_lock(&key)? else {
                continue;
            };
            if lock.start_version == req.start_version {
                if let Some((_commit_version, value)) =
                    self.write_by_start_version(&key, req.start_version)?
                {
                    if req.commit_version != 0 && value.kind == kvpb::mutation::Op::Rollback {
                        return Ok(kvpb::ResolveLockResponse {
                            error: Some(mvcc::errors::txn_already_rolled_back()),
                            ..Default::default()
                        });
                    }
                    locks.push((key, lock, true));
                    continue;
                }
                if req.commit_version != 0 {
                    if req.commit_version < lock.min_commit_ts {
                        return Ok(kvpb::ResolveLockResponse {
                            error: Some(mvcc::errors::commit_ts_expired(
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
        }
        let resolved = locks.len() as u64;
        self.atomic(|batch| {
            for (key, lock, already_written) in &locks {
                if *already_written {
                    if req.commit_version != 0 {
                        batch.delete(LOCK_TREE, key);
                    }
                } else if req.commit_version == 0 {
                    batch.delete(LOCK_TREE, key);
                    let value = rollback_value(req.start_version);
                    apply_committed(batch, key, req.start_version, &value);
                } else {
                    let value = lock_value(lock);
                    apply_committed(batch, key, req.commit_version, &value);
                    batch.delete(LOCK_TREE, key);
                }
            }
        })?;
        Ok(kvpb::ResolveLockResponse {
            resolved_locks: resolved,
            ..Default::default()
        })
    }

    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> mvcc::Result<kvpb::CheckTxnStatusResponse> {
        let _guard = self.lock()?;
        if let Some(lock) = self.get_lock(&req.primary_key)? {
            if lock.start_version == req.lock_ts {
                if is_lock_expired(&lock, req.current_time) {
                    if req.primary_key.is_empty() {
                        return Ok(kvpb::CheckTxnStatusResponse {
                            error: Some(mvcc::errors::empty_rollback_key()),
                            ..Default::default()
                        });
                    }
                    self.atomic(|batch| {
                        batch.delete(LOCK_TREE, &req.primary_key);
                        let value = rollback_value(req.lock_ts);
                        apply_committed(batch, &req.primary_key, req.lock_ts, &value);
                    })?;
                    return Ok(kvpb::CheckTxnStatusResponse {
                        action: kvpb::CheckTxnStatusAction::CheckTxnStatusTtlExpireRollback as i32,
                        ..Default::default()
                    });
                }
                let mut action = kvpb::CheckTxnStatusAction::CheckTxnStatusNoAction;
                let mut current = lock;
                if req.caller_start_ts > 0 && current.min_commit_ts < req.caller_start_ts + 1 {
                    current.min_commit_ts = req.caller_start_ts + 1;
                    let encoded = encode_lock(&current)?;
                    self.atomic(|batch| {
                        batch.put(LOCK_TREE, &req.primary_key, &encoded);
                    })?;
                    action = kvpb::CheckTxnStatusAction::CheckTxnStatusMinCommitTsPushed;
                }
                return Ok(kvpb::CheckTxnStatusResponse {
                    lock_ttl: current.ttl,
                    action: action as i32,
                    ..Default::default()
                });
            } else {
                return Ok(kvpb::CheckTxnStatusResponse {
                    error: Some(mvcc::errors::locked(&req.primary_key, &lock)),
                    ..Default::default()
                });
            }
        }
        if let Some((commit_version, value)) =
            self.write_by_start_version(&req.primary_key, req.lock_ts)?
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
                    error: Some(mvcc::errors::empty_rollback_key()),
                    ..Default::default()
                });
            }
            self.atomic(|batch| {
                let value = rollback_value(req.lock_ts);
                apply_committed(batch, &req.primary_key, req.lock_ts, &value);
            })?;
            return Ok(kvpb::CheckTxnStatusResponse {
                action: kvpb::CheckTxnStatusAction::CheckTxnStatusLockNotExistRollback as i32,
                ..Default::default()
            });
        }
        Ok(kvpb::CheckTxnStatusResponse::default())
    }

    fn txn_heartbeat(
        &self,
        req: &kvpb::TxnHeartBeatRequest,
    ) -> mvcc::Result<kvpb::TxnHeartBeatResponse> {
        let _guard = self.lock()?;
        if let Some(error) = mvcc::errors::txn_heartbeat_validation(req) {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        let Some(mut lock) = self.get_lock(&req.primary_key)? else {
            if let Some((commit_version, value)) =
                self.write_by_start_version(&req.primary_key, req.start_version)?
            {
                if value.kind != kvpb::mutation::Op::Rollback {
                    return Ok(kvpb::TxnHeartBeatResponse {
                        commit_version,
                        action: kvpb::TxnHeartBeatAction::TxnHeartBeatNoAction as i32,
                        ..Default::default()
                    });
                }
            }
            self.atomic(|batch| {
                let value = rollback_value(req.start_version);
                apply_committed(batch, &req.primary_key, req.start_version, &value);
            })?;
            return Ok(kvpb::TxnHeartBeatResponse {
                action: kvpb::TxnHeartBeatAction::TxnHeartBeatLockNotExistRollback as i32,
                ..Default::default()
            });
        };
        if lock.start_version != req.start_version {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(mvcc::errors::locked(&req.primary_key, &lock)),
                ..Default::default()
            });
        }
        if lock.primary.as_slice() != req.primary_key.as_slice() {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(mvcc::errors::txn_heartbeat_primary_mismatch()),
                ..Default::default()
            });
        }
        if is_lock_expired(&lock, req.current_time) {
            self.atomic(|batch| {
                batch.delete(LOCK_TREE, &req.primary_key);
                let value = rollback_value(req.start_version);
                apply_committed(batch, &req.primary_key, req.start_version, &value);
            })?;
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
        if desired_ttl > lock.ttl {
            lock.ttl = desired_ttl;
            let encoded = encode_lock(&lock)?;
            self.atomic(|batch| {
                batch.put(LOCK_TREE, &req.primary_key, &encoded);
            })?;
            action = kvpb::TxnHeartBeatAction::TxnHeartBeatTtlExtended;
        }
        Ok(kvpb::TxnHeartBeatResponse {
            lock_ttl: lock.ttl,
            lock_expire_time: lock_expire_time(&lock),
            action: action as i32,
            ..Default::default()
        })
    }

    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> mvcc::Result<kvpb::TryAtomicMutateResponse> {
        let _guard = self.lock()?;
        if let Some(error) = mvcc::validation::commit_version(req.start_version, req.commit_version)
        {
            return Ok(kvpb::TryAtomicMutateResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        if self.atomic_mutate_already_applied(req)? {
            return Ok(kvpb::TryAtomicMutateResponse {
                applied_keys: req.mutations.len() as u64,
                ..Default::default()
            });
        }
        for predicate in &req.predicates {
            if predicate.key.is_empty() {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::empty_mutation_key()),
                    ..Default::default()
                });
            }
            let read_version = if predicate.read_version == 0 {
                req.start_version
            } else {
                predicate.read_version
            };
            if let Some(lock) = self.get_lock(&predicate.key)? {
                if lock.start_version <= read_version {
                    return Ok(kvpb::TryAtomicMutateResponse {
                        error: Some(mvcc::errors::locked(&predicate.key, &lock)),
                        ..Default::default()
                    });
                }
            }
            let observed = self
                .read_committed(&predicate.key, read_version)?
                .and_then(|(_, value)| value.value);
            if let Some(error) =
                mvcc::validation::atomic_predicate_observation(predicate, observed.as_deref())
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
            if let Some(error) = mvcc::validation::atomic_mutation(mutation) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(error),
                    ..Default::default()
                });
            }
            if let Some(lock) = self.get_lock(&mutation.key)? {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::locked(&mutation.key, &lock)),
                    ..Default::default()
                });
            }
            if let Some((commit_ts, value)) =
                self.first_write_after_or_at(&mutation.key, req.start_version)?
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::write_conflict(
                        &mutation.key,
                        primary,
                        commit_ts,
                        value.start_version,
                        req.start_version,
                    )),
                    ..Default::default()
                });
            }
            if mutation.assertion_not_exist
                && self
                    .read_committed(&mutation.key, req.start_version)?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::already_exists(&mutation.key)),
                    ..Default::default()
                });
            }
        }
        let values = req
            .mutations
            .iter()
            .map(|mutation| {
                (
                    mutation.key.clone(),
                    mutation_value(mutation, req.start_version),
                )
            })
            .collect::<Vec<_>>();
        self.atomic(|batch| {
            for (key, value) in &values {
                apply_committed(batch, key, req.commit_version, value);
            }
        })?;
        Ok(kvpb::TryAtomicMutateResponse {
            applied_keys: req.mutations.len() as u64,
            ..Default::default()
        })
    }

    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> mvcc::Result<kvpb::InstallPreparedMvccEntriesResponse> {
        if let Some(error) = mvcc::validation::install_prepared_request(req) {
            return Ok(kvpb::InstallPreparedMvccEntriesResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        let _guard = self.lock()?;
        let mut applied = 0;
        self.atomic(|batch| {
            for entry in &req.entries {
                match kvpb::prepared_mvcc_entry::ColumnFamily::try_from(entry.column_family)
                    .unwrap_or(kvpb::prepared_mvcc_entry::ColumnFamily::Default)
                {
                    kvpb::prepared_mvcc_entry::ColumnFamily::Default
                    | kvpb::prepared_mvcc_entry::ColumnFamily::Write => {
                        let value = mvcc::VersionedValue {
                            kind: if entry.has_value {
                                kvpb::mutation::Op::Put
                            } else {
                                kvpb::mutation::Op::Delete
                            },
                            start_version: entry.version,
                            value: entry.has_value.then(|| entry.value.clone()),
                            expires_at: entry.expires_at,
                        };
                        apply_committed(batch, &entry.key, entry.version, &value);
                        applied += 1;
                    }
                    kvpb::prepared_mvcc_entry::ColumnFamily::Lock => {
                        batch.delete(LOCK_TREE, &entry.key);
                        applied += 1;
                    }
                }
            }
        })?;
        Ok(kvpb::InstallPreparedMvccEntriesResponse {
            applied_entries: applied,
            commit_version: req.commit_version,
            ..Default::default()
        })
    }

    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> mvcc::Result<kvpb::MvccMaintenanceResponse> {
        if let Some(error) = mvcc::validation::mvcc_maintenance_request(req) {
            return Ok(kvpb::MvccMaintenanceResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        let _guard = self.lock()?;
        let mut applied = 0;
        self.atomic(|batch| {
            for tombstone in &req.tombstones {
                batch.delete(WRITE_TREE, &write_key(&tombstone.key, tombstone.version));
                applied += 1;
            }
        })?;
        Ok(kvpb::MvccMaintenanceResponse {
            applied_entries: applied,
            ..Default::default()
        })
    }
}

fn lock_value(lock: &mvcc::LockRecord) -> mvcc::VersionedValue {
    let value = match lock.op {
        kvpb::mutation::Op::Put | kvpb::mutation::Op::Lock => Some(lock.value.clone()),
        kvpb::mutation::Op::Delete | kvpb::mutation::Op::Rollback => None,
    };
    mvcc::VersionedValue {
        kind: lock.op,
        start_version: lock.start_version,
        value,
        expires_at: lock.expires_at,
    }
}

fn mutation_value(mutation: &kvpb::Mutation, start_version: u64) -> mvcc::VersionedValue {
    let op = kvpb::mutation::Op::try_from(mutation.op).unwrap_or(kvpb::mutation::Op::Put);
    let value = match op {
        kvpb::mutation::Op::Put | kvpb::mutation::Op::Lock => Some(mutation.value.clone()),
        kvpb::mutation::Op::Delete | kvpb::mutation::Op::Rollback => None,
    };
    mvcc::VersionedValue {
        kind: op,
        start_version,
        value,
        expires_at: mutation.expires_at,
    }
}

fn rollback_value(start_version: u64) -> mvcc::VersionedValue {
    mvcc::VersionedValue {
        kind: kvpb::mutation::Op::Rollback,
        start_version,
        value: None,
        expires_at: 0,
    }
}

pub(crate) fn apply_committed(
    batch: &mut holt::DBAtomicBatch,
    key: &[u8],
    commit_ts: u64,
    value: &mvcc::VersionedValue,
) {
    let encoded = encode_value(value);
    batch.put(WRITE_TREE, &write_key(key, commit_ts), &encoded);
    match value.kind {
        kvpb::mutation::Op::Put => {
            if let Some(bytes) = &value.value {
                batch.put(DATA_TREE, key, bytes);
            }
        }
        kvpb::mutation::Op::Delete => {
            batch.delete(DATA_TREE, key);
        }
        kvpb::mutation::Op::Lock | kvpb::mutation::Op::Rollback => {}
    }
}

pub(crate) fn current_physical_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_lock_expired(lock: &mvcc::LockRecord, current_time: u64) -> bool {
    lock.ttl != 0
        && lock.start_time != 0
        && current_time != 0
        && current_time >= lock.start_time
        && current_time - lock.start_time >= lock.ttl
}

fn lock_expire_time(lock: &mvcc::LockRecord) -> u64 {
    if lock.start_time == 0 || lock.ttl == 0 {
        return 0;
    }
    lock.start_time.saturating_add(lock.ttl)
}
