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
        // Only (re)assign identity while unowned; a live owner keeps its routing.
        if record.owner.is_none() {
            record.prefix = prefix;
            record.shard_index = shard_index;
        }
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
        let record = shards
            .entry(shard_id.clone())
            .or_insert_with(|| ShardRecord::unassigned(shard_id.clone()));
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

    #[test]
    fn fresh_acquire_sets_owner_epoch_and_lease() {
        let store = InMemoryControlStore::new();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();

        assert_eq!(lease.epoch, 1);
        assert_eq!(lease.lease_id, 1);

        let record = store.get_shard(&lease.shard_id).unwrap();
        assert_eq!(record.owner, Some(node("node-a")));
        assert_eq!(record.state, ShardState::Serving);
    }

    #[test]
    fn second_fresh_owner_is_rejected() {
        let store = InMemoryControlStore::new();
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
        let store = InMemoryControlStore::new();
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
        let store = InMemoryControlStore::new();
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
        let store = InMemoryControlStore::new();
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
        let store = InMemoryControlStore::new();
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
}
