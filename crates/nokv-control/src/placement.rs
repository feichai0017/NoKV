//! Thin shard-placement helpers over a [`ControlStore`].
//!
//! Placement decides *which node owns which shard*. It manipulates only control
//! metadata (id, prefix, index, owner, epoch, lease, checkpoint/log pointers) —
//! never inode/dentry/chunk state.
//!
//! Ownership handoff is **the same mechanism as HA failover**: bump the shard
//! epoch, and the target node restores from the shard's checkpoint image + log
//! segments (which live in object storage) via the normal failover path. Data
//! never moves node-to-node; it is already durable in the object store.

use crate::{ControlError, ControlStore, NodeId, ShardId, ShardLease, ShardRecord, ShardState};

/// Register a shard's stable identity (prefix + index) so it can be acquired.
pub fn register_shard(
    store: &dyn ControlStore,
    shard_id: ShardId,
    prefix: impl Into<String>,
    shard_index: u16,
) -> Result<ShardRecord, ControlError> {
    store.register_shard(shard_id, prefix.into(), shard_index)
}

/// Assign an unowned shard to a node (fresh acquire).
pub fn assign(
    store: &dyn ControlStore,
    shard_id: ShardId,
    node: NodeId,
) -> Result<ShardLease, ControlError> {
    store.acquire_unassigned(shard_id, node)
}

/// Hand a shard off to a new owner by bumping its epoch. The target node then
/// restores from the shard's checkpoint + log refs via the failover path; the
/// epoch bump fences the previous owner.
pub fn handoff(
    store: &dyn ControlStore,
    shard_id: ShardId,
    to: NodeId,
    previous_epoch: u64,
) -> Result<ShardLease, ControlError> {
    store.acquire_after_failure(shard_id, to, previous_epoch)
}

/// Shards with no current owner — candidates for assignment.
pub fn unowned_shards(store: &dyn ControlStore) -> Result<Vec<ShardRecord>, ControlError> {
    Ok(store
        .list_shards()?
        .into_iter()
        .filter(|record| record.owner.is_none() || record.state == ShardState::Unassigned)
        .collect())
}

/// Shards currently owned by `node` — what a restarting node would re-adopt.
pub fn shards_owned_by(
    store: &dyn ControlStore,
    node: &NodeId,
) -> Result<Vec<ShardRecord>, ControlError> {
    Ok(store
        .list_shards()?
        .into_iter()
        .filter(|record| record.owner.as_ref() == Some(node))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryControlStore;

    #[test]
    fn register_assign_handoff_flow() {
        let store = InMemoryControlStore::new();
        let shard = ShardId::new("mount-1:/dataset/imagenet");

        let record = register_shard(&store, shard.clone(), "/dataset/imagenet", 3).unwrap();
        assert_eq!(record.prefix, "/dataset/imagenet");
        assert_eq!(record.shard_index, 3);
        assert!(record.owner.is_none());

        // Unowned -> candidate for assignment.
        let unowned = unowned_shards(&store).unwrap();
        assert_eq!(unowned.len(), 1);

        // Assign to node-a; identity is preserved, endpoint set.
        let lease = assign(&store, shard.clone(), NodeId::new("10.0.0.1:7000")).unwrap();
        let record = store.get_shard(&shard).unwrap();
        assert_eq!(record.shard_index, 3);
        assert_eq!(record.endpoint.as_deref(), Some("10.0.0.1:7000"));
        assert_eq!(
            shards_owned_by(&store, &NodeId::new("10.0.0.1:7000"))
                .unwrap()
                .len(),
            1
        );
        assert!(unowned_shards(&store).unwrap().is_empty());

        // Handoff bumps the epoch and fences the old owner.
        let new = handoff(
            &store,
            shard.clone(),
            NodeId::new("10.0.0.2:7000"),
            lease.epoch,
        )
        .unwrap();
        assert_eq!(new.epoch, lease.epoch + 1);
        let record = store.get_shard(&shard).unwrap();
        assert_eq!(record.endpoint.as_deref(), Some("10.0.0.2:7000"));
        assert_eq!(record.shard_index, 3, "identity survives handoff");
    }

    #[test]
    fn list_shards_enumerates_all() {
        let store = InMemoryControlStore::new();
        register_shard(&store, ShardId::new("mount-1:/"), "/", 0).unwrap();
        register_shard(&store, ShardId::new("mount-1:/dataset"), "/dataset", 1).unwrap();
        register_shard(&store, ShardId::new("mount-1:/runs"), "/runs", 2).unwrap();

        let mut indices: Vec<u16> = store
            .list_shards()
            .unwrap()
            .into_iter()
            .map(|record| record.shard_index)
            .collect();
        indices.sort_unstable();
        assert_eq!(indices, vec![0, 1, 2]);
    }
}
