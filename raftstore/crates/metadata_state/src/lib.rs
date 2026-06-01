//! Metadata state primitives for Rust raftstore.
//!
//! The crate owns metadata read, write, snapshot, and key-error semantics shared
//! by in-memory tests and Holt-backed state-machine storage. It intentionally
//! does not know fsmeta inode/dentry semantics or raftstore topology.

pub mod errors;
mod metadata;
mod retention;
mod snapshot;
mod store;
mod types;
pub mod validation;

pub use snapshot::{decode_metadata_snapshot, encode_metadata_snapshot};
pub use store::{scan_key_matches_start, scan_limit, scan_read_version, value_is_expired};
pub use types::{
    Error, MemoryMetadataStore, MetadataApplyResult, MetadataEngine, MetadataRetentionEngine,
    MetadataRetentionResult, MetadataSnapshot, MetadataSnapshotEngine, MetadataSnapshotWrite,
    Result, ValueKind, VersionedValue,
};

pub use metadata::{metadata_mutation_matches_value, metadata_mutation_value};

pub(crate) use store::{read_committed, write_by_start_version};
pub(crate) use types::Inner;

#[cfg(test)]
mod tests;
