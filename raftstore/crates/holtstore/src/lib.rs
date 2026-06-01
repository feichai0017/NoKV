//! Holt multi-tree adapter for Rust raftstore.
//!
//! The adapter fixes the tree layout used by the Rust data plane while keeping
//! Holt internals out of the raftstore service and metadata-store crates.

mod codec;
mod metadata;
mod metrics;
mod region_meta;
mod retention;
mod scheduler_state;
mod snapshot;
mod store;
mod trees;
mod types;
mod versions;
mod watch_apply;

pub use metrics::{holt_metadata_metrics_snapshot, HoltMetadataMetricsSnapshot};
pub use store::{HoltMetadataStore, HoltStore};
pub use trees::{
    APPLY_STATE_TREE, DEFAULT_CURRENT_TREE, HISTORY_TREE, REGION_META_TREE, WATCH_APPLY_TREE,
};
pub use types::{
    BlockedRootEvent, BlockedSchedulerOperation, Error, PendingRootEvent,
    PendingSchedulerOperation, RegionApplyState, Result,
};
pub use watch_apply::DEFAULT_WATCH_APPLY_REPLAY_LIMIT;

#[cfg(test)]
mod tests;
