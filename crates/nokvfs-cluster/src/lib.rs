//! Shared-log metadata replication contracts for NoKV-FS.
//!
//! This crate owns the metadata replication boundary above `nokvfs-meta`.
//! Log entries contain semantic `MetadataCommand` batches, not raw storage
//! mutations. Concrete implementations may use Raft, an external shared log, or
//! another quorum log, but those details must not leak into filesystem metadata
//! semantics.

mod checkpoint;
mod errors;
mod file;
mod frontier;
mod group;
mod log;
mod memory;
mod quorum;
mod replay;
mod replication;
mod store;
mod types;

pub use checkpoint::{
    compact_log_to_checkpoint, compact_log_to_latest_checkpoint, CheckpointCatalog,
    CheckpointCompactionOutcome, FileCheckpointCatalog, MemoryCheckpointCatalog,
};
pub use errors::{ReplayError, SharedLogError};
pub use file::{
    decode_metadata_log_entry, encode_metadata_log_entry, FileSharedLog, FileSharedLogOptions,
    FileSharedLogSync,
};
pub use frontier::{AppliedFrontierStore, FileAppliedFrontierStore, MemoryAppliedFrontierStore};
pub use group::{MetadataGroup, MetadataGroupCommit};
pub use log::SharedMetadataLog;
pub use memory::InMemorySharedLog;
pub use quorum::{InMemoryQuorumLog, QuorumNodeLog, QuorumNodeRole};
pub use replay::{replay_entries, MetadataLogSink, ReplayDriver, ReplayOutcome};
pub use replication::{
    AppendMetadataBatchRequest, AppendMetadataBatchResponse, InstallCheckpointRequest,
    InstallCheckpointResponse, ReadMetadataLogRequest, ReadMetadataLogResponse,
};
pub use store::{SharedLogMetadataStore, SharedLogRuntimeStats};
pub use types::{
    AppliedMetadataCommand, ApplyFrontier, CheckpointArtifact, CheckpointFrontier,
    CheckpointManifest, DurableReceipt, LearnerBootstrapPlan, LogIndex, LogPosition, LogTerm,
    MetadataLogEntry, NodeId, ReadFreshness,
};

#[cfg(test)]
mod tests;
