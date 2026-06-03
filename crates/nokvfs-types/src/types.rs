use std::fmt;
use std::num::NonZeroU64;

use crate::DentryName;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MountId(NonZeroU64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InodeId(NonZeroU64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelError {
    ZeroMountId,
    ZeroInodeId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordFamily {
    System,
    Mount,
    Inode,
    Dentry,
    Parent,
    ChunkManifest,
    Session,
    PathIndex,
    Watch,
    Snapshot,
    Gc,
    CommandDedupe,
    History,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodeAttr {
    pub inode: InodeId,
    pub file_type: FileType,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryRecord {
    pub parent: InodeId,
    pub name: DentryName,
    pub child: InodeId,
    pub child_type: FileType,
    pub attr_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryProjection {
    pub dentry: DentryRecord,
    pub attr: InodeAttr,
    pub body: Option<BodyDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BodyDescriptor {
    pub producer: String,
    pub digest_uri: String,
    pub size: u64,
    pub content_type: String,
    pub manifest_id: String,
    pub generation: u64,
    pub chunk_size: u64,
    pub block_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkManifest {
    pub chunk_index: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub blocks: Vec<BlockDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockDescriptor {
    pub object_key: String,
    pub logical_offset: u64,
    pub object_offset: u64,
    pub len: u64,
    pub digest_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectGcRecord {
    pub inode: InodeId,
    pub generation: u64,
    pub object_key: String,
    pub size: u64,
    pub digest_uri: String,
    pub enqueue_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPin {
    pub snapshot_id: u64,
    pub root: InodeId,
    pub read_version: u64,
    pub created_version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchEventKind {
    Create,
    Remove,
    Rename,
    UpdateAttr,
    PublishArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchEvent {
    pub kind: WatchEventKind,
    pub parent: Option<InodeId>,
    pub name: Option<DentryName>,
    pub inode: InodeId,
    pub version: u64,
}

impl MountId {
    pub fn new(id: u64) -> Result<Self, ModelError> {
        NonZeroU64::new(id).map(Self).ok_or(ModelError::ZeroMountId)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl InodeId {
    pub const ROOT_RAW: u64 = 1;

    pub fn new(id: u64) -> Result<Self, ModelError> {
        NonZeroU64::new(id).map(Self).ok_or(ModelError::ZeroInodeId)
    }

    pub fn root() -> Self {
        Self(NonZeroU64::new(Self::ROOT_RAW).expect("root inode id is non-zero"))
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMountId => write!(f, "mount id must be non-zero"),
            Self::ZeroInodeId => write!(f, "inode id must be non-zero"),
        }
    }
}

impl std::error::Error for ModelError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_reject_zero() {
        assert_eq!(MountId::new(0), Err(ModelError::ZeroMountId));
        assert_eq!(InodeId::new(0), Err(ModelError::ZeroInodeId));
    }

    #[test]
    fn root_inode_is_one() {
        assert_eq!(InodeId::root().get(), 1);
    }
}
