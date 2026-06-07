use std::collections::BTreeMap;

use nokv_types::{AdvisoryLock, AdvisoryLockKind, AdvisoryLockRequest};

use super::*;

#[derive(Default)]
pub(super) struct AdvisoryLockTable {
    by_inode: BTreeMap<InodeId, Vec<AdvisoryLock>>,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn get_advisory_lock(
        &self,
        request: AdvisoryLockRequest,
    ) -> Result<Option<AdvisoryLock>, MetadError> {
        self.validate_advisory_lock_request(request)?;
        let table = self
            .advisory_locks
            .lock()
            .map_err(|_| MetadError::InvalidPath("advisory lock table is poisoned".to_owned()))?;
        Ok(table.conflict(request))
    }

    pub fn set_advisory_lock(&self, request: AdvisoryLockRequest) -> Result<(), MetadError> {
        self.validate_advisory_lock_request(request)?;
        let mut table = self
            .advisory_locks
            .lock()
            .map_err(|_| MetadError::InvalidPath("advisory lock table is poisoned".to_owned()))?;
        table.set(request)
    }

    fn validate_advisory_lock_request(
        &self,
        request: AdvisoryLockRequest,
    ) -> Result<(), MetadError> {
        if request.start > request.end {
            return Err(MetadError::InvalidPath(format!(
                "advisory lock range {}..={} is invalid",
                request.start, request.end
            )));
        }
        let Some(attr) = self.get_attr(request.inode)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        Ok(())
    }
}

impl AdvisoryLockTable {
    fn conflict(&self, request: AdvisoryLockRequest) -> Option<AdvisoryLock> {
        if request.kind == AdvisoryLockKind::Unlock {
            return None;
        }
        self.by_inode
            .get(&request.inode)?
            .iter()
            .copied()
            .find(|lock| lock.owner != request.owner && locks_conflict(*lock, request))
    }

    fn set(&mut self, request: AdvisoryLockRequest) -> Result<(), MetadError> {
        if request.kind == AdvisoryLockKind::Unlock {
            self.unlock(request);
            return Ok(());
        }
        if let Some(conflict) = self.conflict(request) {
            return Err(MetadError::LockConflict(conflict));
        }
        self.unlock(request);
        let lock = AdvisoryLock {
            inode: request.inode,
            owner: request.owner,
            start: request.start,
            end: request.end,
            kind: request.kind,
            pid: request.pid,
        };
        let locks = self.by_inode.entry(request.inode).or_default();
        locks.push(lock);
        normalize_locks(locks);
        Ok(())
    }

    fn unlock(&mut self, request: AdvisoryLockRequest) {
        let Some(locks) = self.by_inode.get_mut(&request.inode) else {
            return;
        };
        let mut next = Vec::with_capacity(locks.len());
        for lock in locks.drain(..) {
            if lock.owner != request.owner
                || !ranges_overlap(lock.start, lock.end, request.start, request.end)
            {
                next.push(lock);
                continue;
            }
            if lock.start < request.start {
                next.push(AdvisoryLock {
                    end: request.start.saturating_sub(1),
                    ..lock
                });
            }
            if request.end < lock.end {
                next.push(AdvisoryLock {
                    start: request.end.saturating_add(1),
                    ..lock
                });
            }
        }
        *locks = next;
        normalize_locks(locks);
        if locks.is_empty() {
            self.by_inode.remove(&request.inode);
        }
    }
}

fn locks_conflict(existing: AdvisoryLock, request: AdvisoryLockRequest) -> bool {
    ranges_overlap(existing.start, existing.end, request.start, request.end)
        && (existing.kind == AdvisoryLockKind::Write || request.kind == AdvisoryLockKind::Write)
}

fn ranges_overlap(left_start: u64, left_end: u64, right_start: u64, right_end: u64) -> bool {
    left_start <= right_end && right_start <= left_end
}

fn normalize_locks(locks: &mut Vec<AdvisoryLock>) {
    locks.sort_by_key(|lock| (lock.owner, lock.kind, lock.start, lock.end));
    let mut merged: Vec<AdvisoryLock> = Vec::with_capacity(locks.len());
    for lock in locks.drain(..) {
        if let Some(last) = merged.last_mut() {
            let adjacent_or_overlapping = lock.start <= last.end.saturating_add(1);
            if last.owner == lock.owner
                && last.kind == lock.kind
                && last.pid == lock.pid
                && last.inode == lock.inode
                && adjacent_or_overlapping
            {
                last.end = last.end.max(lock.end);
                continue;
            }
        }
        merged.push(lock);
    }
    *locks = merged;
}
