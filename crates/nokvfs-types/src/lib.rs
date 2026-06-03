//! Storage-engine-neutral NoKV-FS namespace model.
//!
//! This crate owns filesystem metadata domain types: mounts, inodes, dentries,
//! file attributes, body descriptors, record families, and typed watch events.
//! It does not own key layout, Holt integration, Raft replication, object-store
//! clients, or service wire types.

mod names;
mod types;

pub use names::{DentryName, NameError};
pub use types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryProjection, DentryRecord, FileType,
    InodeAttr, InodeId, ModelError, MountId, RecordFamily, WatchEvent, WatchEventKind,
};
