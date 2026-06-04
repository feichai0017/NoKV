//! Shared-log metadata replication contracts for NoKV-FS.
//!
//! This crate owns the metadata replication boundary above `nokvfs-meta`.
//! Log entries contain semantic `MetadataCommand` batches, not raw storage
//! mutations. Concrete implementations may use Raft, an external shared log, or
//! another quorum log, but those details must not leak into filesystem metadata
//! semantics.

mod errors;
mod group;
mod log;
mod memory;
mod replay;
mod types;

pub use errors::{ReplayError, SharedLogError};
pub use group::{MetadataGroup, MetadataGroupCommit};
pub use log::SharedMetadataLog;
pub use memory::InMemorySharedLog;
pub use replay::{replay_entries, MetadataLogSink, ReplayDriver, ReplayOutcome};
pub use types::{
    AppliedMetadataCommand, ApplyFrontier, CheckpointFrontier, DurableReceipt, LogIndex,
    LogPosition, LogTerm, MetadataLogEntry,
};

#[cfg(test)]
mod tests;
