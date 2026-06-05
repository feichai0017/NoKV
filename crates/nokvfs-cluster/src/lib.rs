//! Metadata replication contracts for NoKV-FS.
//!
//! This crate owns the metadata replication boundary above `nokvfs-meta`.
//! Replicated log entries contain semantic `MetadataCommand` batches, not raw
//! storage mutations. OpenRaft is the production ordering mechanism; older
//! shared-log types remain only until the server cutover removes that path.

mod checkpoint;
mod errors;
mod file;
mod frontier;
mod group;
mod log;
mod membership;
mod memory;
mod openraft_file_log;
mod openraft_log;
mod openraft_network;
mod openraft_store;
mod openraft_wire;
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
    decode_metadata_command_batch, decode_metadata_log_entry, encode_metadata_command_batch,
    encode_metadata_log_entry, FileSharedLog, FileSharedLogOptions, FileSharedLogSync,
};
pub use frontier::{AppliedFrontierStore, FileAppliedFrontierStore, MemoryAppliedFrontierStore};
pub use group::{MetadataGroup, MetadataGroupCommit};
pub use log::SharedMetadataLog;
pub use membership::{FileMembershipCatalog, MembershipCatalog, MemoryMembershipCatalog};
pub use memory::InMemorySharedLog;
pub use openraft_file_log::{FileMetadataRaftLogOptions, FileMetadataRaftLogSync};
pub use openraft_network::{MetadataRaftRpcClient, MetadataRaftRpcNetworkFactory};
pub use openraft_store::{
    OpenRaftMetadataStats, OpenRaftMetadataStatsHandle, OpenRaftMetadataStore,
};
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
    MetadataLogEntry, MetadataMembership, NodeId, ReadFreshness,
};

#[cfg(test)]
mod tests;
