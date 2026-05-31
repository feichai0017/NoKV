use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{
    apply_lock, apply_rollback, current_physical_time_millis, errors, is_lock_expired,
    lock_expire_time, read_committed, validation, write_by_start_version, Error, LockRecord,
    MvccStore, Result,
};

impl MvccStore {
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
}
