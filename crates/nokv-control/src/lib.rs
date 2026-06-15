//! Control-plane state for NoKV metadata shards.
//!
//! This crate owns shard ownership, lease epochs, and checkpoint/log pointers.
//! It must not own namespace semantics, chunk manifests, object GC policy, Holt
//! internals, FUSE behavior, or provider-specific object-store behavior.

mod codec;
mod errors;
#[cfg(feature = "etcd")]
mod etcd;
mod options;
mod placement;
mod store;
mod types;

pub use codec::{decode_shard_record, encode_shard_record};
pub use errors::ControlError;
#[cfg(feature = "etcd")]
pub use etcd::EtcdControlStore;
pub use options::EtcdControlStoreOptions;
pub use placement::{assign, handoff, register_shard, shards_owned_by, unowned_shards};
pub use store::{ControlStore, InMemoryControlStore};
pub use types::{
    CheckpointRef, LogRef, LogSegmentRef, NodeId, ShardId, ShardLease, ShardRecord, ShardState,
};
