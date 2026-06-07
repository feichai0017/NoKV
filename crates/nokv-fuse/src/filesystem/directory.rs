use nokv_meta::DentryWithAttr;
use nokv_types::{DentryName, InodeAttr, InodeId};

#[derive(Clone, Debug)]
pub(super) struct DirectoryHandle {
    pub(super) inode: InodeId,
    pub(super) attr: InodeAttr,
    pub(super) entries: Vec<DentryWithAttr>,
    pub(super) next_cursor: Option<DentryName>,
    pub(super) exhausted: bool,
}

#[cfg(not(test))]
pub(super) const FUSE_READDIR_PAGE_SIZE: usize = 1024;
#[cfg(test)]
pub(super) const FUSE_READDIR_PAGE_SIZE: usize = 4;
pub(super) const FUSE_DOT_OFFSET: u64 = 1;
pub(super) const FUSE_DOT_DOT_OFFSET: u64 = 2;
const FUSE_FIRST_CHILD_OFFSET: u64 = 3;

pub(super) fn child_index_from_offset(offset: u64) -> Option<usize> {
    let raw = offset.saturating_sub(FUSE_FIRST_CHILD_OFFSET);
    usize::try_from(raw).ok()
}

pub(super) fn child_offset(index: usize) -> u64 {
    u64::try_from(index)
        .unwrap_or(u64::MAX.saturating_sub(FUSE_FIRST_CHILD_OFFSET))
        .saturating_add(FUSE_FIRST_CHILD_OFFSET)
}
