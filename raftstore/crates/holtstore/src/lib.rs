//! Holt multi-tree adapter for Rust raftstore.
//!
//! The adapter fixes the tree layout used by the Rust data plane while keeping
//! Holt internals out of the raftstore service and MVCC protocol crates.

mod codec;
#[path = "mvcc.rs"]
mod mvcc_engine;
mod region_meta;
mod scheduler_state;
mod snapshot;
mod store;
mod trees;
mod types;
mod watch_apply;

pub use store::{HoltMvccStore, HoltStore};
pub use trees::{
    APPLY_STATE_TREE, DATA_TREE, LOCK_TREE, REGION_META_TREE, WATCH_APPLY_TREE, WRITE_TREE,
};
pub use types::{
    BlockedRootEvent, BlockedSchedulerOperation, Error, PendingRootEvent,
    PendingSchedulerOperation, RegionApplyState, Result,
};
pub use watch_apply::DEFAULT_WATCH_APPLY_REPLAY_LIMIT;

#[cfg(test)]
mod tests;
