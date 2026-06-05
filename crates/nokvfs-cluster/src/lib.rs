//! Metadata replication contracts for NoKV-FS.
//!
//! This crate owns the metadata replication boundary above `nokvfs-meta`.
//! Replicated log entries contain semantic `MetadataCommand` batches, not raw
//! storage mutations. OpenRaft is the production ordering mechanism.

mod codec;
mod errors;
mod file_log;
mod log;
mod network;
mod store;
mod types;
mod wire;

pub use errors::MetadataRaftError;
pub use file_log::{FileMetadataRaftLogOptions, FileMetadataRaftLogSync};
pub use network::{MetadataRaftRpcClient, MetadataRaftRpcNetworkFactory};
pub use store::{
    OpenRaftMetadataStats, OpenRaftMetadataStatsHandle, OpenRaftMetadataStore,
    ProposalCoalescerOptions,
};
pub use types::{LogIndex, LogPosition, LogTerm, NodeId};
