//! MVCC and Percolator-compatible metadata operations for Rust raftstore.
//!
//! The crate owns the storage-level transaction semantics that `StoreKV` exposes
//! over gRPC. It intentionally does not know fsmeta inode/dentry semantics.

mod atomic;
pub mod errors;
mod maintenance;
mod metadata;
mod prepared;
mod read;
mod snapshot;
mod store;
mod txn;
mod types;
pub mod validation;

pub use snapshot::{decode_mvcc_snapshot, encode_mvcc_snapshot};
pub use store::{scan_limit, scan_read_version, value_is_expired};
pub use types::{
    Error, KvEngine, LockRecord, MetadataApplyResult, MetadataEngine, MvccSnapshot,
    MvccSnapshotEngine, MvccSnapshotLock, MvccSnapshotRollback, MvccSnapshotWrite, MvccStore,
    Result, VersionedValue,
};

pub use metadata::{
    metadata_get_response_from_kv, metadata_key_error_from_kv, metadata_mutation_matches_value,
    metadata_mutation_value, metadata_scan_response_from_kv,
};

pub(crate) use store::{
    apply_lock, apply_mutation, apply_rollback, blocking_lock, current_physical_time_millis,
    is_lock_expired, lock_expire_time, read_committed, write_by_start_version,
};
pub(crate) use types::Inner;

#[cfg(test)]
mod tests;
