use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::{
    CheckpointRef, ControlError, LogRef, NodeId, ShardId, ShardLease, ShardRecord, ShardState,
};

pub trait ControlStore: Send + Sync {
    fn ensure_shard(&self, shard_id: ShardId) -> Result<ShardRecord, ControlError>;
    /// Register a shard's stable identity (prefix + index) before it is acquired.
    /// Idempotent; identity is only set while the shard is unowned so a live
    /// owner's routing cannot change underneath it.
    fn register_shard(
        &self,
        shard_id: ShardId,
        prefix: String,
        shard_index: u16,
    ) -> Result<ShardRecord, ControlError>;
    /// Record (or, with `None`, clear) the durable subtree-root inode for a
    /// subtree shard — the atomic registration point of a cross-shard graft.
    /// Idempotent and not lease-gated: it is a topology fact about the shard's
    /// own namespace, set by `register_graft` before the (reconcilable) parent
    /// graft dentry is written, and cleared by `unregister_graft` after the
    /// graft is torn down. Returns the updated record.
    fn set_subtree_root_inode(
        &self,
        shard_id: &ShardId,
        subtree_root_inode: Option<u64>,
    ) -> Result<ShardRecord, ControlError>;
    /// Enumerate every known shard record so clients can build the routing map
    /// and placement can find unowned/owned shards.
    fn list_shards(&self) -> Result<Vec<ShardRecord>, ControlError>;
    fn get_shard(&self, shard_id: &ShardId) -> Result<ShardRecord, ControlError>;
    fn acquire_unassigned(
        &self,
        shard_id: ShardId,
        owner: NodeId,
    ) -> Result<ShardLease, ControlError>;
    fn acquire_after_failure(
        &self,
        shard_id: ShardId,
        owner: NodeId,
        previous_epoch: u64,
    ) -> Result<ShardLease, ControlError>;
    fn renew(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError>;
    fn mark_serving(
        &self,
        lease: &ShardLease,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ShardRecord, ControlError>;
    fn release(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError>;
}

/// Apply a `register_shard` to a record in place, enforcing that a shard's stable
/// identity (`prefix`, `shard_index`) can only be set while the record is pristine
/// — never leased (`epoch == 0`). Once a shard has taken a lease its identity is
/// frozen: it is encoded in inode high bits and the client routing map, so a drift
/// after a release would misroute existing data. Idempotent: re-registering the
/// same identity always succeeds. Shared by both control-store backends.
pub(crate) fn register_shard_identity(
    record: &mut ShardRecord,
    prefix: String,
    shard_index: u16,
) -> Result<(), ControlError> {
    if record.prefix == prefix && record.shard_index == shard_index {
        return Ok(());
    }
    // Pristine == never owned and never leased. `epoch == 0` is the durable
    // marker (acquire bumps it to >= 1 and release does not reset it).
    if record.epoch == 0 && record.owner.is_none() {
        record.prefix = prefix;
        record.shard_index = shard_index;
        return Ok(());
    }
    Err(ControlError::ShardIdentityLocked {
        shard_id: record.shard_id.clone(),
    })
}

/// Whether a shard id denotes the default/root shard (prefix `/`), which is the
/// single shard allowed to be acquired without a prior `register_shard` — its
/// identity (prefix `/`, index 0) is the unambiguous bootstrap default. Every
/// non-root shard must be registered first so its index cannot silently be 0.
pub(crate) fn is_default_shard(shard_id: &ShardId) -> bool {
    shard_id
        .as_str()
        .split_once(':')
        .map(|(_, path)| path)
        .unwrap_or("/")
        == "/"
}

#[derive(Default)]
pub struct InMemoryControlStore {
    shards: Mutex<BTreeMap<ShardId, ShardRecord>>,
}

impl InMemoryControlStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_lease(record: &ShardRecord) -> u64 {
        record.lease_id.saturating_add(1).max(1)
    }

    fn validate_lease(record: &ShardRecord, lease: &ShardLease) -> Result<(), ControlError> {
        if record.owner.as_ref() != Some(&lease.owner) {
            return Err(ControlError::NotOwner {
                shard_id: lease.shard_id.clone(),
            });
        }
        if record.epoch != lease.epoch || record.lease_id != lease.lease_id {
            return Err(ControlError::StaleLease {
                shard_id: lease.shard_id.clone(),
                epoch: lease.epoch,
                lease_id: lease.lease_id,
            });
        }
        Ok(())
    }
}

impl ControlStore for InMemoryControlStore {
    fn ensure_shard(&self, shard_id: ShardId) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .entry(shard_id.clone())
            .or_insert_with(|| ShardRecord::unassigned(shard_id));
        Ok(record.clone())
    }

    fn register_shard(
        &self,
        shard_id: ShardId,
        prefix: String,
        shard_index: u16,
    ) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .entry(shard_id.clone())
            .or_insert_with(|| ShardRecord::unassigned(shard_id));
        register_shard_identity(record, prefix, shard_index)?;
        Ok(record.clone())
    }

    fn set_subtree_root_inode(
        &self,
        shard_id: &ShardId,
        subtree_root_inode: Option<u64>,
    ) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(shard_id.clone()))?;
        record.subtree_root_inode = subtree_root_inode;
        Ok(record.clone())
    }

    fn list_shards(&self) -> Result<Vec<ShardRecord>, ControlError> {
        let shards = self.shards.lock().expect("control store mutex poisoned");
        Ok(shards.values().cloned().collect())
    }

    fn get_shard(&self, shard_id: &ShardId) -> Result<ShardRecord, ControlError> {
        let shards = self.shards.lock().expect("control store mutex poisoned");
        shards
            .get(shard_id)
            .cloned()
            .ok_or_else(|| ControlError::ShardNotFound(shard_id.clone()))
    }

    fn acquire_unassigned(
        &self,
        shard_id: ShardId,
        owner: NodeId,
    ) -> Result<ShardLease, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        // A non-default shard MUST be registered first: auto-creating it here via
        // `unassigned` would default its `shard_index` to 0 and collide with the
        // root shard, breaking shard-index uniqueness and inode routing. The
        // default/root shard keeps its bootstrap path (auto-create with index 0).
        let record = match shards.get_mut(&shard_id) {
            Some(record) => record,
            None if is_default_shard(&shard_id) => shards
                .entry(shard_id.clone())
                .or_insert_with(|| ShardRecord::unassigned(shard_id.clone())),
            None => {
                return Err(ControlError::ShardNotRegistered { shard_id });
            }
        };
        if let Some(existing_owner) = record.owner.clone() {
            return Err(ControlError::ShardAlreadyOwned {
                shard_id,
                owner: existing_owner,
                epoch: record.epoch,
            });
        }
        record.owner = Some(owner.clone());
        record.endpoint = Some(owner.as_str().to_owned());
        record.epoch = record.epoch.saturating_add(1).max(1);
        record.lease_id = Self::next_lease(record);
        record.state = ShardState::Serving;
        Ok(ShardLease {
            shard_id,
            owner,
            epoch: record.epoch,
            lease_id: record.lease_id,
        })
    }

    fn acquire_after_failure(
        &self,
        shard_id: ShardId,
        owner: NodeId,
        previous_epoch: u64,
    ) -> Result<ShardLease, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(&shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(shard_id.clone()))?;
        if record.epoch != previous_epoch {
            return Err(ControlError::StaleEpoch {
                shard_id,
                expected: previous_epoch,
                actual: record.epoch,
            });
        }
        record.owner = Some(owner.clone());
        record.endpoint = Some(owner.as_str().to_owned());
        record.epoch = record.epoch.saturating_add(1);
        record.lease_id = Self::next_lease(record);
        record.state = ShardState::Recovering;
        Ok(ShardLease {
            shard_id,
            owner,
            epoch: record.epoch,
            lease_id: record.lease_id,
        })
    }

    fn renew(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
        let shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get(&lease.shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(lease.shard_id.clone()))?;
        Self::validate_lease(record, lease)?;
        Ok(record.clone())
    }

    fn mark_serving(
        &self,
        lease: &ShardLease,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(&lease.shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(lease.shard_id.clone()))?;
        Self::validate_lease(record, lease)?;
        if checkpoint.is_some() {
            record.checkpoint = checkpoint;
        }
        if log.is_some() {
            record.log = log;
        }
        record.durable_lsn = record.durable_lsn.max(durable_lsn);
        record.state = ShardState::Serving;
        Ok(record.clone())
    }

    fn release(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(&lease.shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(lease.shard_id.clone()))?;
        Self::validate_lease(record, lease)?;
        record.owner = None;
        record.endpoint = None;
        record.state = ShardState::Unassigned;
        Ok(record.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogSegmentRef;

    fn shard() -> ShardId {
        ShardId::new("mount-1:/runs")
    }

    fn node(raw: &str) -> NodeId {
        NodeId::new(raw)
    }

    /// A store with the non-default test shard already registered, matching the
    /// production precondition that every non-root shard is registered before it
    /// is acquired.
    fn registered_store() -> InMemoryControlStore {
        let store = InMemoryControlStore::new();
        store
            .register_shard(shard(), "/runs".to_owned(), 2)
            .unwrap();
        store
    }

    #[test]
    fn fresh_acquire_sets_owner_epoch_and_lease() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();

        assert_eq!(lease.epoch, 1);
        assert_eq!(lease.lease_id, 1);

        let record = store.get_shard(&lease.shard_id).unwrap();
        assert_eq!(record.owner, Some(node("node-a")));
        assert_eq!(record.state, ShardState::Serving);
        // Registered identity survives acquisition.
        assert_eq!(record.shard_index, 2);
        assert_eq!(record.prefix, "/runs");
    }

    #[test]
    fn second_fresh_owner_is_rejected() {
        let store = registered_store();
        let _lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();

        let err = store
            .acquire_unassigned(shard(), node("node-b"))
            .unwrap_err();

        assert!(matches!(
            err,
            ControlError::ShardAlreadyOwned {
                owner,
                epoch: 1,
                ..
            } if owner == node("node-a")
        ));
    }

    #[test]
    fn failover_bumps_epoch_and_fences_old_lease() {
        let store = registered_store();
        let old = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let new = store
            .acquire_after_failure(shard(), node("node-b"), old.epoch)
            .unwrap();

        assert_eq!(new.epoch, 2);
        assert_eq!(new.lease_id, 2);
        assert_eq!(store.renew(&new).unwrap().state, ShardState::Recovering);
        assert!(matches!(
            store.renew(&old).unwrap_err(),
            ControlError::NotOwner { .. }
        ));
    }

    #[test]
    fn mark_serving_requires_current_lease() {
        let store = registered_store();
        let old = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let new = store
            .acquire_after_failure(shard(), node("node-b"), old.epoch)
            .unwrap();

        assert!(store.mark_serving(&old, None, None, 7).is_err());

        let record = store.mark_serving(&new, None, None, 7).unwrap();
        assert_eq!(record.state, ShardState::Serving);
        assert_eq!(record.durable_lsn, 7);
    }

    #[test]
    fn mark_serving_preserves_recovery_refs_when_not_replaced() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let log = LogRef {
            segments: vec![LogSegmentRef {
                segment_key: "meta/log/segment".to_owned(),
                first_lsn: 8,
                last_lsn: 9,
                digest: "abc123".to_owned(),
            }],
            durable_lsn: 9,
            digest: "abc123".to_owned(),
        };
        let checkpoint = CheckpointRef {
            object_key: "meta/checkpoints/ckpt".to_owned(),
            lsn: 9,
            image_bytes: 1024,
            digest: "def456".to_owned(),
        };
        store
            .mark_serving(&lease, Some(checkpoint.clone()), Some(log.clone()), 9)
            .unwrap();

        let record = store.mark_serving(&lease, None, None, 0).unwrap();

        assert_eq!(record.checkpoint, Some(checkpoint));
        assert_eq!(record.log, Some(log));
        assert_eq!(record.durable_lsn, 9);
    }

    #[test]
    fn set_subtree_root_inode_records_and_clears() {
        let store = InMemoryControlStore::new();
        store.ensure_shard(shard()).unwrap();

        // Set, then read back through the durable record.
        let updated = store
            .set_subtree_root_inode(&shard(), Some(0x0001_0000_0000_0002))
            .unwrap();
        assert_eq!(updated.subtree_root_inode, Some(0x0001_0000_0000_0002));
        assert_eq!(
            store.get_shard(&shard()).unwrap().subtree_root_inode,
            Some(0x0001_0000_0000_0002)
        );

        // Idempotent re-set to the same value.
        store
            .set_subtree_root_inode(&shard(), Some(0x0001_0000_0000_0002))
            .unwrap();

        // Clearing (unregister) returns to None.
        let cleared = store.set_subtree_root_inode(&shard(), None).unwrap();
        assert_eq!(cleared.subtree_root_inode, None);
        assert_eq!(store.get_shard(&shard()).unwrap().subtree_root_inode, None);
    }

    #[test]
    fn set_subtree_root_inode_on_missing_shard_is_not_found() {
        let store = InMemoryControlStore::new();
        let err = store
            .set_subtree_root_inode(&ShardId::new("mount-1:/absent"), Some(7))
            .unwrap_err();
        assert!(matches!(err, ControlError::ShardNotFound(_)));
    }

    #[test]
    fn release_requires_current_lease() {
        let store = registered_store();
        let old = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let new = store
            .acquire_after_failure(shard(), node("node-b"), old.epoch)
            .unwrap();

        assert!(store.release(&old).is_err());

        let released = store.release(&new).unwrap();
        assert_eq!(released.owner, None);
        assert_eq!(released.state, ShardState::Unassigned);
        assert_eq!(released.epoch, new.epoch);
    }

    #[test]
    fn register_shard_freezes_identity_after_first_lease() {
        let store = registered_store();
        // Re-registering the SAME identity is idempotent, even while owned.
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        store
            .register_shard(shard(), "/runs".to_owned(), 2)
            .unwrap();

        // Releasing leaves epoch > 0; identity must stay frozen so a later
        // re-register cannot drift the index a live client routes by.
        store.release(&lease).unwrap();
        let record = store.get_shard(&shard()).unwrap();
        assert!(record.owner.is_none());
        assert!(record.epoch > 0);

        let err = store
            .register_shard(shard(), "/runs".to_owned(), 9)
            .unwrap_err();
        assert!(matches!(err, ControlError::ShardIdentityLocked { .. }));
        // The original index is unchanged.
        assert_eq!(store.get_shard(&shard()).unwrap().shard_index, 2);
    }

    #[test]
    fn register_shard_assigns_identity_while_pristine() {
        let store = InMemoryControlStore::new();
        // Before any lease, identity is freely (re)assignable.
        store
            .register_shard(shard(), "/runs".to_owned(), 2)
            .unwrap();
        let record = store
            .register_shard(shard(), "/runs".to_owned(), 5)
            .unwrap();
        assert_eq!(record.shard_index, 5);
        assert_eq!(record.epoch, 0);
    }

    #[test]
    fn acquire_unassigned_requires_registration_for_non_default_shard() {
        let store = InMemoryControlStore::new();
        // No register_shard: a non-default shard cannot be acquired (would
        // otherwise auto-create with shard_index 0 and collide with root).
        let err = store
            .acquire_unassigned(shard(), node("node-a"))
            .unwrap_err();
        assert!(matches!(err, ControlError::ShardNotRegistered { .. }));
        assert!(store.get_shard(&shard()).is_err());
    }

    #[test]
    fn acquire_unassigned_bootstraps_default_shard_without_registration() {
        let store = InMemoryControlStore::new();
        let default_shard = ShardId::new("mount-1:/");
        let lease = store
            .acquire_unassigned(default_shard.clone(), node("node-a"))
            .unwrap();
        assert_eq!(lease.epoch, 1);
        let record = store.get_shard(&default_shard).unwrap();
        assert_eq!(record.shard_index, 0);
        assert_eq!(record.prefix, "/");
    }
}
