//! FUSE frontend for NoKV.
//!
//! This crate owns the low-level FUSE mapping from kernel inode operations to
//! remote metadata-service calls. It does not own metadata layout, Holt state,
//! object provider semantics, or path-oriented SDK behavior.

mod attr;
mod backend;
mod filesystem;
mod invalidation;

pub use attr::{file_attr, fuse_file_type};
pub use filesystem::{mount_client, FuseAccessMode, FuseOptions, FuseView, FuseWritebackOptions};
pub use invalidation::FuseInvalidationOptions;
