//! Storage-engine-neutral NoKV namespace model.
//!
//! This crate owns filesystem metadata domain types: mounts, inodes, dentries,
//! file attributes, body descriptors, record families, and typed watch events.
//! It does not own key layout, Holt integration, Raft replication, object-store
//! clients, or service wire types.

mod names;
mod types;

pub use names::{parse_absolute_path, DentryName, NameError, PathError};
pub use types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryProjection, DentryRecord, FileType,
    InodeAttr, InodeId, ModelError, MountId, ObjectGcRecord, RecordFamily, SnapshotPin,
    WatchCursor, WatchEvent, WatchEventKind, WatchRecord,
};
