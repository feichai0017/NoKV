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
    AdvisoryLock, AdvisoryLockKind, AdvisoryLockRequest, BlockDescriptor, BodyDescriptor,
    ChunkManifest, DentryProjection, DentryRecord, FileType, ForkBinding, InodeAttr, InodeId,
    ModelError, MountId, ObjectGcRecord, PathMetadata, RecordFamily, SliceManifest, SnapshotPin,
    SpecialNodeSpec, WatchCursor, WatchEvent, WatchEventKind, WatchRecord,
};
