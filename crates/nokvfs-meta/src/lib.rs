//! Metadata model execution for NoKV-FS.
//!
//! This crate owns the Holt-friendly metadata layout, the storage-neutral
//! metadata command contract, the Holt-backed metastore, and the in-process
//! metadata service. It does not own object-store provider behavior, FUSE, or
//! client path ergonomics.

pub mod command;
pub mod gc;
pub mod holtstore;
pub mod layout;
pub mod service;

pub use command::{
    CommandKind, CommitResult, HistoryPruneOutcome, HistoryPruneRequest, MetadataCommand,
    MetadataError, MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, Mutation,
    MutationOp, Predicate, PredicateRef, ReadPurpose, ScanItem, ScanRequest, Value, Version,
    WatchProjection,
};
pub use gc::{
    HistoryGcOptions, HistoryGcWorker, HistoryGcWorkerState, ObjectGcOptions, ObjectGcWorker,
    ObjectGcWorkerState,
};
pub use holtstore::HoltMetadataStore;
pub use service::{
    BodyReadPlan, DentryWithAttr, MetadError, NoKvFs, ObjectTransferStats,
    PendingObjectCleanupOutcome, PreparedArtifact, PublishArtifact, RenameReplaceResult,
};
