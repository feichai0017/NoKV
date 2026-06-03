//! Metadata model execution for NoKV-FS.
//!
//! This crate owns the Holt-friendly metadata layout, the storage-neutral
//! metadata command contract, the Holt-backed metastore, and the in-process
//! metadata service. It does not own object-store provider behavior, FUSE, or
//! client path ergonomics.

pub mod command;
pub mod holtstore;
pub mod layout;
pub mod service;

pub use command::{
    CommandKind, CommitResult, MetadataCommand, MetadataError, MetadataStore, Mutation, MutationOp,
    Predicate, PredicateRef, ReadPurpose, ScanItem, ScanRequest, Value, Version, WatchProjection,
};
pub use holtstore::HoltMetadataStore;
pub use service::{
    DentryWithAttr, MetadError, NoKvFs, ObjectTransferStats, PublishArtifact, RenameReplaceResult,
};
