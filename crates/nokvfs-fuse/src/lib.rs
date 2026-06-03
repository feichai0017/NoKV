//! FUSE frontend for NoKV-FS.
//!
//! This crate owns the low-level FUSE mapping from kernel inode operations to
//! `metad` service calls. It does not own metadata layout, Holt state, object
//! provider semantics, or path-oriented SDK behavior.

mod attr;
mod filesystem;

pub use attr::{file_attr, fuse_file_type};
pub use filesystem::{mount, FuseOptions, FuseView, NoKvFuse};
