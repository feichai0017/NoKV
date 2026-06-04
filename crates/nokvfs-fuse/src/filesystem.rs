use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    AccessFlags, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType as FuseFileType,
    Filesystem, FopenFlags, Generation, INodeNo, MountOption, OpenAccMode, OpenFlags, RenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use nokvfs_meta::command::MetadataStore;
use nokvfs_meta::{
    DentryWithAttr, MetadError, NoKvFs, PreparedArtifact, PublishArtifactRange,
    PublishArtifactStagedSession, ReadDirPlusPage, RenameReplaceResult, UpdateAttr, XattrSetMode,
};
use nokvfs_object::{ObjectReadBlock, ObjectStore, StagedObjectSet, StoredChunk};
use nokvfs_types::{DentryName, FileType, InodeAttr, InodeId};
use sha2::{Digest, Sha256};

use crate::attr::{file_attr, fuse_file_type};
use crate::invalidation::{FuseInvalidationOptions, FuseInvalidationWorker, InvalidationRegistry};

#[derive(Clone, Debug)]
pub struct FuseOptions {
    pub entry_ttl: Duration,
    pub attr_ttl: Duration,
    pub fs_name: String,
    pub threads: usize,
    pub view: FuseView,
    pub access: FuseAccessMode,
    pub invalidation: FuseInvalidationOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuseView {
    Live,
    Snapshot { snapshot_id: u64, root: InodeId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuseAccessMode {
    ReadWrite,
    ReadOnly,
}

pub struct NoKvFuse<M, O> {
    service: Arc<NoKvFs<M, O>>,
    options: FuseOptions,
    parents: RwLock<HashMap<u64, u64>>,
    names: RwLock<HashMap<u64, Vec<u8>>>,
    next_handle: AtomicU64,
    write_handles: RwLock<HashMap<u64, WriteHandle>>,
    directory_handles: RwLock<HashMap<u64, DirectoryHandle>>,
    invalidation: Arc<InvalidationRegistry>,
}

#[derive(Clone, Debug)]
struct WriteHandle {
    inode: InodeId,
    parent: InodeId,
    name: DentryName,
    prepared: Option<PreparedArtifact>,
    mode: u32,
    uid: u32,
    gid: u32,
    base_size: u64,
    size: u64,
    dirty_extents: Vec<DirtyExtent>,
    staged_chunks: Vec<StoredChunk>,
    staged: StagedObjectSet,
    next_block_index: u64,
    dirty: bool,
}

#[derive(Clone, Debug)]
struct DirtyExtent {
    chunks: Vec<StoredChunk>,
}

#[derive(Clone, Debug)]
struct DirectoryHandle {
    inode: InodeId,
    attr: InodeAttr,
    entries: Vec<DentryWithAttr>,
    next_cursor: Option<DentryName>,
    exhausted: bool,
}

#[cfg(not(test))]
const FUSE_READDIR_PAGE_SIZE: usize = 1024;
#[cfg(test)]
const FUSE_READDIR_PAGE_SIZE: usize = 4;
const FUSE_DOT_OFFSET: u64 = 1;
const FUSE_DOT_DOT_OFFSET: u64 = 2;
const FUSE_FIRST_CHILD_OFFSET: u64 = 3;
const XATTR_CREATE: i32 = 0x1;
const XATTR_REPLACE: i32 = 0x2;
const STATFS_BLOCK_SIZE: u32 = 4096;
const STATFS_TOTAL_BYTES: u64 = 1 << 40;
const STATFS_TOTAL_FILES: u64 = 1 << 32;
const STATFS_NAME_MAX: u32 = 255;

impl Default for FuseOptions {
    fn default() -> Self {
        Self {
            entry_ttl: Duration::from_secs(1),
            attr_ttl: Duration::from_secs(1),
            fs_name: "nokv-fs".to_owned(),
            threads: default_threads(),
            view: FuseView::Live,
            access: FuseAccessMode::ReadWrite,
            invalidation: FuseInvalidationOptions::default(),
        }
    }
}

#[cfg(target_os = "linux")]
fn default_threads() -> usize {
    4
}

#[cfg(not(target_os = "linux"))]
fn default_threads() -> usize {
    1
}

impl<M, O> NoKvFuse<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn new(service: NoKvFs<M, O>, options: FuseOptions) -> Self {
        Self::from_shared(Arc::new(service), options)
    }

    pub fn from_shared(service: Arc<NoKvFs<M, O>>, options: FuseOptions) -> Self {
        let mut parents = HashMap::new();
        parents.insert(options.view.root().get(), options.view.root().get());
        let invalidation = Arc::new(InvalidationRegistry::default());
        let fuse = Self {
            service,
            options,
            parents: RwLock::new(parents),
            names: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            write_handles: RwLock::new(HashMap::new()),
            directory_handles: RwLock::new(HashMap::new()),
            invalidation,
        };
        fuse.register_watch_scope(fuse.options.view.root());
        fuse
    }

    pub fn service(&self) -> &NoKvFs<M, O> {
        &self.service
    }

    fn remember_parent(&self, child: InodeId, parent: InodeId) {
        if let Ok(mut parents) = self.parents.write() {
            parents.insert(child.get(), parent.get());
        }
    }

    fn remember_name(&self, child: InodeId, name: &DentryName) {
        if let Ok(mut names) = self.names.write() {
            names.insert(child.get(), name.as_bytes().to_vec());
        }
    }

    fn remember_entry(&self, entry: &DentryWithAttr) {
        self.remember_parent(entry.attr.inode, entry.dentry.parent);
        self.remember_name(entry.attr.inode, &entry.dentry.name);
        if entry.attr.file_type == FileType::Directory {
            self.register_watch_scope(entry.attr.inode);
        }
    }

    fn forget_inode_cache(&self, inode: InodeId) {
        if inode == self.options.view.root() {
            return;
        }
        if let Ok(mut parents) = self.parents.write() {
            parents.remove(&inode.get());
        }
        if let Ok(mut names) = self.names.write() {
            names.remove(&inode.get());
        }
    }

    fn statfs_snapshot(&self) -> FuseStatfs {
        let blocks = STATFS_TOTAL_BYTES / u64::from(STATFS_BLOCK_SIZE);
        FuseStatfs {
            blocks,
            bfree: blocks,
            bavail: blocks,
            files: STATFS_TOTAL_FILES,
            ffree: STATFS_TOTAL_FILES,
            bsize: STATFS_BLOCK_SIZE,
            namelen: STATFS_NAME_MAX,
            frsize: STATFS_BLOCK_SIZE,
        }
    }

    fn register_watch_scope(&self, scope: InodeId) {
        if self.options.view != FuseView::Live {
            return;
        }
        if let Ok(cursor) = self.service.watch_subtree(scope) {
            self.invalidation.register_scope(scope, cursor);
        }
    }

    fn invalidation_registry(&self) -> Arc<InvalidationRegistry> {
        Arc::clone(&self.invalidation)
    }

    fn parent_of(&self, inode: InodeId) -> InodeId {
        if inode == self.options.view.root() {
            return inode;
        }
        let raw = self
            .parents
            .read()
            .ok()
            .and_then(|parents| parents.get(&inode.get()).copied())
            .unwrap_or(InodeId::ROOT_RAW);
        InodeId::new(raw).unwrap_or_else(|_| InodeId::root())
    }

    fn name_of(&self, inode: InodeId) -> Result<DentryName, Errno> {
        let raw = self
            .names
            .read()
            .ok()
            .and_then(|names| names.get(&inode.get()).cloned())
            .ok_or(Errno::EIO)?;
        DentryName::new(raw).map_err(|_| Errno::EIO)
    }

    fn metadata_inode(&self, ino: INodeNo) -> Result<InodeId, Errno> {
        if ino.0 == InodeId::ROOT_RAW {
            return Ok(self.options.view.root());
        }
        inode_id(ino)
    }

    fn fuse_ino(&self, inode: InodeId) -> INodeNo {
        if inode == self.options.view.root() {
            INodeNo(InodeId::ROOT_RAW)
        } else {
            INodeNo(inode.get())
        }
    }

    fn view_file_attr(&self, attr: &InodeAttr) -> FileAttr {
        let mut out = file_attr(attr);
        out.ino = self.fuse_ino(attr.inode);
        out
    }

    fn read_only(&self) -> bool {
        self.options.access.is_read_only() || self.options.view.is_read_only()
    }

    fn service_get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, MetadError> {
        match self.options.view {
            FuseView::Live => self.service.get_attr(inode),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.service.get_attr_at_snapshot(snapshot_id, inode)
            }
        }
    }

    fn service_lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        match self.options.view {
            FuseView::Live => self.service.lookup_plus(parent, name),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.service
                    .lookup_plus_at_snapshot(snapshot_id, parent, name)
            }
        }
    }

    fn service_read_dir_plus_page(
        &self,
        inode: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        match self.options.view {
            FuseView::Live => self.service.read_dir_plus_page(inode, after, limit),
            FuseView::Snapshot { snapshot_id, .. } => {
                let requested = limit.max(1);
                let rows = self.service.read_dir_plus_at_snapshot(snapshot_id, inode)?;
                let mut entries = rows
                    .into_iter()
                    .filter(|entry| {
                        after.is_none_or(|cursor| entry.dentry.name.as_bytes() > cursor.as_bytes())
                    })
                    .take(requested.saturating_add(1))
                    .collect::<Vec<_>>();
                let has_more = entries.len() > requested;
                entries.truncate(requested);
                let next_cursor = if has_more {
                    entries.last().map(|entry| entry.dentry.name.clone())
                } else {
                    None
                };
                Ok(ReadDirPlusPage {
                    entries,
                    next_cursor,
                })
            }
        }
    }

    fn rename_entry(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, MetadError> {
        if self.service.lookup_plus(new_parent, &new_name)?.is_some() {
            self.service
                .rename_replace(parent, name, new_parent, new_name)
        } else {
            self.service
                .rename(parent, name, new_parent, new_name)
                .map(|entry| RenameReplaceResult {
                    entry,
                    replaced: None,
                })
        }
    }

    fn service_read_file(
        &self,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        match self.options.view {
            FuseView::Live => self.service.read_file(inode, offset, len),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.service
                    .read_file_at_snapshot(snapshot_id, inode, offset, len)
            }
        }
    }

    fn service_read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, MetadError> {
        match self.options.view {
            FuseView::Live => self.service.read_symlink(inode),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.service.read_symlink_at_snapshot(snapshot_id, inode)
            }
        }
    }

    fn add_dirent(
        &self,
        reply: &mut ReplyDirectory,
        request_offset: u64,
        entry_offset: u64,
        inode: InodeId,
        kind: FuseFileType,
        name: &str,
    ) -> bool {
        request_offset < entry_offset && reply.add(self.fuse_ino(inode), entry_offset, kind, name)
    }

    fn add_dirent_plus(
        &self,
        reply: &mut ReplyDirectoryPlus,
        request_offset: u64,
        entry_offset: u64,
        name: &str,
        attr: &InodeAttr,
    ) -> bool {
        request_offset < entry_offset
            && reply.add(
                self.fuse_ino(attr.inode),
                entry_offset,
                name,
                &self.options.entry_ttl,
                &self.view_file_attr(attr),
                Generation(attr.generation),
            )
    }

    fn allocate_directory_handle(&self, inode: InodeId) -> Result<FileHandle, Errno> {
        let attr = self
            .service_get_attr(inode)
            .map_err(errno)?
            .ok_or(Errno::ENOENT)?;
        if attr.file_type != FileType::Directory {
            return Err(Errno::ENOTDIR);
        }
        let raw = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.directory_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .insert(
                raw,
                DirectoryHandle {
                    inode,
                    attr,
                    entries: Vec::new(),
                    next_cursor: None,
                    exhausted: false,
                },
            );
        Ok(FileHandle(raw))
    }

    fn directory_handle_attr(&self, fh: FileHandle) -> Result<InodeAttr, Errno> {
        self.directory_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .map(|handle| handle.attr.clone())
            .ok_or(Errno::EBADF)
    }

    fn directory_child(
        &self,
        fh: FileHandle,
        child_index: usize,
    ) -> Result<Option<DentryWithAttr>, Errno> {
        loop {
            let fetch = {
                let handles = self.directory_handles.read().map_err(|_| Errno::EIO)?;
                let handle = handles.get(&fh.0).ok_or(Errno::EBADF)?;
                if let Some(entry) = handle.entries.get(child_index) {
                    return Ok(Some(entry.clone()));
                }
                if handle.exhausted {
                    return Ok(None);
                }
                (handle.inode, handle.next_cursor.clone())
            };
            let page = self
                .service_read_dir_plus_page(fetch.0, fetch.1.as_ref(), FUSE_READDIR_PAGE_SIZE)
                .map_err(errno)?;
            for entry in &page.entries {
                self.remember_entry(entry);
            }
            let mut handles = self.directory_handles.write().map_err(|_| Errno::EIO)?;
            let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
            if handle.entries.get(child_index).is_some() || handle.next_cursor != fetch.1 {
                continue;
            }
            handle.entries.extend(page.entries);
            handle.next_cursor = page.next_cursor;
            handle.exhausted = handle.next_cursor.is_none();
        }
    }

    fn release_directory_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        self.directory_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .remove(&fh.0);
        Ok(())
    }

    fn sync_directory_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        self.directory_handle_attr(fh).map(|_| ())
    }

    fn allocate_handle(&self, handle: WriteHandle) -> Result<FileHandle, Errno> {
        let raw = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .insert(raw, handle);
        Ok(FileHandle(raw))
    }

    fn open_write_handle(&self, attr: &InodeAttr, parent: InodeId) -> Result<FileHandle, Errno> {
        let name = self.name_of(attr.inode)?;
        self.allocate_write_handle(attr, parent, name)
    }

    fn allocate_write_handle(
        &self,
        attr: &InodeAttr,
        parent: InodeId,
        name: DentryName,
    ) -> Result<FileHandle, Errno> {
        self.allocate_handle(WriteHandle {
            inode: attr.inode,
            parent,
            name,
            prepared: None,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            base_size: attr.size,
            size: attr.size,
            dirty_extents: Vec::new(),
            staged_chunks: Vec::new(),
            staged: StagedObjectSet::default(),
            next_block_index: 0,
            dirty: false,
        })
    }

    fn write_to_handle(&self, fh: FileHandle, offset: u64, data: &[u8]) -> Result<usize, Errno> {
        let end = offset
            .checked_add(u64::try_from(data.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
        let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
        if !data.is_empty() {
            if handle.prepared.is_none() {
                handle.prepared = Some(
                    self.service
                        .prepare_artifact_replace(handle.parent, handle.name.clone())
                        .map_err(errno)?,
                );
            }
            let prepared = handle.prepared.as_ref().ok_or(Errno::EIO)?;
            let written = self
                .service
                .stage_prepared_artifact_ranges(
                    prepared,
                    &fuse_manifest_id(handle.parent, handle.inode),
                    &[PublishArtifactRange {
                        offset,
                        bytes: data.to_vec(),
                    }],
                    handle.next_block_index,
                )
                .map_err(errno)?;
            handle.next_block_index = handle
                .next_block_index
                .saturating_add(written.object_puts as u64);
            let staged = written.staged_objects().map_err(|_| Errno::EIO)?;
            let staged_objects = handle
                .staged
                .objects()
                .iter()
                .cloned()
                .chain(staged.objects().iter().cloned())
                .collect();
            handle.staged = StagedObjectSet::new(staged_objects);
            handle.staged_chunks.extend(written.chunks.clone());
            handle.dirty_extents.push(DirtyExtent {
                chunks: written.chunks,
            });
            handle.size = handle.size.max(end);
            handle.dirty = true;
        }
        Ok(data.len())
    }

    fn truncate_handle(&self, fh: FileHandle, size: u64) -> Result<(), Errno> {
        let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
        let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
        if handle.prepared.is_none() {
            handle.prepared = Some(
                self.service
                    .prepare_artifact_replace(handle.parent, handle.name.clone())
                    .map_err(errno)?,
            );
        }
        handle.size = size;
        handle.dirty = true;
        Ok(())
    }

    fn update_handle_attrs(
        &self,
        fh: FileHandle,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<bool, Errno> {
        let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
        let Some(handle) = handles.get_mut(&fh.0) else {
            return Ok(false);
        };
        if let Some(mode) = mode {
            handle.mode = mode;
        }
        if let Some(uid) = uid {
            handle.uid = uid;
        }
        if let Some(gid) = gid {
            handle.gid = gid;
        }
        Ok(true)
    }

    fn read_from_handle(
        &self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Option<Vec<u8>>, Errno> {
        let Some(handle) = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .cloned()
        else {
            return Ok(None);
        };
        if offset >= handle.size {
            return Ok(Some(Vec::new()));
        }
        let output_len = u64::from(size)
            .min(handle.size.saturating_sub(offset))
            .try_into()
            .map_err(|_| Errno::EINVAL)?;
        let mut bytes = vec![0_u8; output_len];
        if offset < handle.base_size {
            let base_len = u64::try_from(output_len)
                .map_err(|_| Errno::EINVAL)?
                .min(handle.base_size - offset) as usize;
            let base = self
                .service_read_file(handle.inode, offset, base_len)
                .map_err(errno)?;
            bytes[..base.len()].copy_from_slice(&base);
        }
        self.overlay_dirty_extents(&mut bytes, offset, &handle.dirty_extents)?;
        Ok(Some(bytes))
    }

    fn overlay_dirty_extents(
        &self,
        output: &mut [u8],
        output_offset: u64,
        extents: &[DirtyExtent],
    ) -> Result<(), Errno> {
        if output.is_empty() {
            return Ok(());
        }
        let output_limit = output_offset
            .checked_add(u64::try_from(output.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        for extent in extents {
            for chunk in &extent.chunks {
                for block in &chunk.blocks {
                    let block_end = block
                        .logical_offset
                        .checked_add(block.len)
                        .ok_or(Errno::EINVAL)?;
                    let start = block.logical_offset.max(output_offset);
                    let end = block_end.min(output_limit);
                    if start >= end {
                        continue;
                    }
                    let object_offset = block
                        .object_offset
                        .checked_add(start - block.logical_offset)
                        .ok_or(Errno::EINVAL)?;
                    let len = usize::try_from(end - start).map_err(|_| Errno::EINVAL)?;
                    let bytes = self
                        .service
                        .read_session_object_blocks(
                            len,
                            &[ObjectReadBlock {
                                object_key: block.object_key.clone(),
                                object_offset,
                                len,
                                output_offset: 0,
                            }],
                        )
                        .map_err(errno)?;
                    let output_start =
                        usize::try_from(start - output_offset).map_err(|_| Errno::EINVAL)?;
                    let output_end = output_start.checked_add(bytes.len()).ok_or(Errno::EINVAL)?;
                    output[output_start..output_end].copy_from_slice(&bytes);
                }
            }
        }
        Ok(())
    }

    fn digest_handle_body(&self, fh: FileHandle, size: u64) -> Result<String, Errno> {
        const DIGEST_CHUNK_SIZE: u32 = 8 * 1024 * 1024;

        let mut hasher = Sha256::new();
        let mut offset = 0_u64;
        while offset < size {
            let requested = (size - offset).min(u64::from(DIGEST_CHUNK_SIZE)) as u32;
            let bytes = self
                .read_from_handle(fh, offset, requested)?
                .ok_or(Errno::EIO)?;
            if bytes.is_empty() {
                return Err(Errno::EIO);
            }
            hasher.update(&bytes);
            offset = offset
                .checked_add(u64::try_from(bytes.len()).map_err(|_| Errno::EINVAL)?)
                .ok_or(Errno::EINVAL)?;
        }
        let digest = hasher.finalize();
        Ok(format!("sha256:{digest:x}"))
    }

    fn publish_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        let Some(snapshot) = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .cloned()
        else {
            return Ok(());
        };
        if !snapshot.dirty {
            return Ok(());
        }
        let digest_uri = self.digest_handle_body(fh, snapshot.size)?;
        self.service
            .publish_prepared_artifact_staged_session(
                snapshot.prepared.ok_or(Errno::EIO)?,
                PublishArtifactStagedSession {
                    parent: snapshot.parent,
                    name: snapshot.name,
                    producer: "nokv-fuse".to_owned(),
                    digest_uri,
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: fuse_manifest_id(snapshot.parent, snapshot.inode),
                    size: snapshot.size,
                    chunks: snapshot.staged_chunks,
                    staged: snapshot.staged,
                    mode: snapshot.mode,
                    uid: snapshot.uid,
                    gid: snapshot.gid,
                },
            )
            .map_err(errno)?;
        if let Some(handle) = self
            .write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .get_mut(&fh.0)
        {
            handle.base_size = snapshot.size;
            handle.size = snapshot.size;
            handle.dirty_extents.clear();
            handle.staged_chunks.clear();
            handle.staged = StagedObjectSet::default();
            handle.prepared = None;
            handle.next_block_index = 0;
            handle.dirty = false;
        }
        Ok(())
    }

    fn release_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        self.publish_handle(fh)?;
        self.write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .remove(&fh.0);
        Ok(())
    }
}

pub fn mount<M, O>(
    service: NoKvFs<M, O>,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<()>
where
    M: MetadataStore + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    mount_shared(Arc::new(service), mountpoint, options)
}

pub fn mount_shared<M, O>(
    service: Arc<NoKvFs<M, O>>,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<()>
where
    M: MetadataStore + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    let mut config = Config::default();
    let mount_options = mount_options(&options);
    config.mount_options = mount_options;
    config.n_threads = Some(options.threads);
    let fuse = NoKvFuse::from_shared(Arc::clone(&service), options.clone());
    let registry = fuse.invalidation_registry();
    let session = fuser::spawn_mount2(fuse, mountpoint, &config)?;
    let _invalidation_worker = if options.view == FuseView::Live {
        Some(FuseInvalidationWorker::spawn(
            service,
            session.notifier(),
            registry,
            options.invalidation,
        ))
    } else {
        None
    };
    session.join()
}

fn mount_options(options: &FuseOptions) -> Vec<MountOption> {
    let mut mount_options = vec![MountOption::FSName(options.fs_name.clone())];
    if options.access.is_read_only() || options.view.is_read_only() {
        mount_options.push(MountOption::RO);
    } else {
        mount_options.push(MountOption::RW);
    }
    #[cfg(target_os = "macos")]
    {
        mount_options.push(MountOption::CUSTOM("fstypename=nokvfs".to_owned()));
        mount_options.push(MountOption::CUSTOM(format!("volname={}", options.fs_name)));
        // NoKV does not persist Finder/resource-fork metadata yet. Ask macFUSE
        // to reject Apple sidecars instead of creating visible ._ files.
        mount_options.push(MountOption::CUSTOM("noappledouble".to_owned()));
        mount_options.push(MountOption::CUSTOM("noapplexattr".to_owned()));
    }
    #[cfg(not(target_os = "macos"))]
    {
        mount_options.push(MountOption::Subtype("nokv-fs".to_owned()));
        mount_options.push(MountOption::NoAtime);
    }
    mount_options
}

impl FuseView {
    fn root(self) -> InodeId {
        match self {
            Self::Live => InodeId::root(),
            Self::Snapshot { root, .. } => root,
        }
    }

    fn is_read_only(self) -> bool {
        matches!(self, Self::Snapshot { .. })
    }
}

impl FuseAccessMode {
    fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FuseStatfs {
    blocks: u64,
    bfree: u64,
    bavail: u64,
    files: u64,
    ffree: u64,
    bsize: u32,
    namelen: u32,
    frsize: u32,
}

impl<M, O> Filesystem for NoKvFuse<M, O>
where
    M: MetadataStore + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    fn forget(&self, _req: &Request, ino: INodeNo, _nlookup: u64) {
        if let Ok(inode) = self.metadata_inode(ino) {
            self.forget_inode_cache(inode);
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service_lookup_plus(parent, &name) {
            Ok(Some(entry)) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.metadata_inode(ino).and_then(|inode| {
            self.service_get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(attr) => reply.attr(&self.options.attr_ttl, &self.view_file_attr(&attr)),
            Err(err) => reply.error(err),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let mut published_handle = false;
        if let Some(fh) = fh {
            if let Err(err) = self.update_handle_attrs(fh, mode, uid, gid) {
                reply.error(err);
                return;
            }
            if let Some(size) = size {
                if let Err(err) = self.truncate_handle(fh, size) {
                    reply.error(err);
                    return;
                }
            }
            if size.is_some() {
                if let Err(err) = self.publish_handle(fh) {
                    reply.error(err);
                    return;
                }
                published_handle = true;
            }
        }
        if published_handle && mtime.is_none() && ctime.is_none() {
            match self.service_get_attr(inode) {
                Ok(Some(attr)) => reply.attr(&self.options.attr_ttl, &self.view_file_attr(&attr)),
                Ok(None) => reply.error(Errno::ENOENT),
                Err(err) => reply.error(errno(err)),
            }
            return;
        }
        let changes = UpdateAttr {
            mode: (!published_handle).then_some(mode).flatten(),
            uid: (!published_handle).then_some(uid).flatten(),
            gid: (!published_handle).then_some(gid).flatten(),
            size: (!published_handle).then_some(size).flatten(),
            mtime_ms: mtime.map(time_or_now_ms),
            ctime_ms: ctime.map(system_time_ms),
        };
        if inode == self.options.view.root() {
            match self.service.update_root_attrs(changes) {
                Ok(attr) => reply.attr(&self.options.attr_ttl, &self.view_file_attr(&attr)),
                Err(err) => reply.error(errno(err)),
            }
            return;
        }
        let parent = self.parent_of(inode);
        let name = match self.name_of(inode) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service.update_attrs(parent, &name, changes) {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.attr(&self.options.attr_ttl, &self.view_file_attr(&entry.attr));
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service_read_symlink(inode) {
            Ok(target) => reply.data(&target),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        match self.metadata_inode(ino).and_then(|inode| {
            self.service_get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(attr) if attr.file_type == FileType::File => match flags.acc_mode() {
                OpenAccMode::O_RDONLY => reply.opened(FileHandle(0), FopenFlags::FOPEN_KEEP_CACHE),
                OpenAccMode::O_WRONLY | OpenAccMode::O_RDWR => {
                    if self.read_only() {
                        reply.error(Errno::EROFS);
                        return;
                    }
                    let parent = self.parent_of(attr.inode);
                    match self.open_write_handle(&attr, parent) {
                        Ok(fh) => reply.opened(fh, FopenFlags::empty()),
                        Err(err) => reply.error(err),
                    }
                }
            },
            Ok(_) => reply.error(Errno::EISDIR),
            Err(err) => reply.error(err),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        match self.read_from_handle(fh, offset, size) {
            Ok(Some(bytes)) => {
                reply.data(&bytes);
                return;
            }
            Ok(None) => {}
            Err(err) => {
                reply.error(err);
                return;
            }
        }
        match self.metadata_inode(ino).and_then(|inode| {
            self.service_read_file(inode, offset, size as usize)
                .map_err(errno)
        }) {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(err),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self
            .metadata_inode(ino)
            .and_then(|inode| self.allocate_directory_handle(inode))
        {
            Ok(handle) => reply.opened(handle, FopenFlags::FOPEN_KEEP_CACHE),
            Err(err) => reply.error(err),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if let Err(err) = self.directory_handle_attr(fh) {
            reply.error(err);
            return;
        }
        let parent = self.parent_of(inode);
        if self.add_dirent(
            &mut reply,
            offset,
            FUSE_DOT_OFFSET,
            inode,
            FuseFileType::Directory,
            ".",
        ) || self.add_dirent(
            &mut reply,
            offset,
            FUSE_DOT_DOT_OFFSET,
            parent,
            FuseFileType::Directory,
            "..",
        ) {
            reply.ok();
            return;
        }
        let Some(mut index) = child_index_from_offset(offset) else {
            reply.ok();
            return;
        };
        loop {
            let entry = match self.directory_child(fh, index) {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) => {
                    reply.error(err);
                    return;
                }
            };
            let entry_offset = child_offset(index);
            if offset >= entry_offset {
                index = index.saturating_add(1);
                continue;
            }
            if reply.add(
                self.fuse_ino(entry.attr.inode),
                entry_offset,
                fuse_file_type(entry.attr.file_type),
                OsStr::from_bytes(entry.dentry.name.as_bytes()),
            ) {
                break;
            }
            index = index.saturating_add(1);
        }
        reply.ok();
    }

    fn readdirplus(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let attr = match self.directory_handle_attr(fh) {
            Ok(attr) => attr,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if attr.file_type != FileType::Directory {
            reply.error(Errno::ENOTDIR);
            return;
        };
        let parent = self.parent_of(inode);
        let parent_attr = self
            .service_get_attr(parent)
            .ok()
            .flatten()
            .unwrap_or_else(|| attr.clone());
        if self.add_dirent_plus(&mut reply, offset, FUSE_DOT_OFFSET, ".", &attr)
            || self.add_dirent_plus(&mut reply, offset, FUSE_DOT_DOT_OFFSET, "..", &parent_attr)
        {
            reply.ok();
            return;
        }
        let Some(mut index) = child_index_from_offset(offset) else {
            reply.ok();
            return;
        };
        loop {
            let entry = match self.directory_child(fh, index) {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) => {
                    reply.error(err);
                    return;
                }
            };
            let entry_offset = child_offset(index);
            if offset >= entry_offset {
                index = index.saturating_add(1);
                continue;
            }
            if reply.add(
                self.fuse_ino(entry.attr.inode),
                entry_offset,
                OsStr::from_bytes(entry.dentry.name.as_bytes()),
                &self.options.entry_ttl,
                &self.view_file_attr(&entry.attr),
                Generation(entry.attr.generation),
            ) {
                break;
            }
            index = index.saturating_add(1);
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        match self.release_directory_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.sync_directory_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let stat = self.statfs_snapshot();
        reply.statfs(
            stat.blocks,
            stat.bfree,
            stat.bavail,
            stat.files,
            stat.ffree,
            stat.bsize,
            stat.namelen,
            stat.frsize,
        );
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        if position != 0 {
            reply.error(xattr_unsupported_error());
            return;
        }
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match xattr_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let mode = match xattr_set_mode(flags) {
            Ok(mode) => mode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service.set_xattr(inode, name, value.to_vec(), mode) {
            Ok(()) => reply.ok(),
            Err(MetadError::Metadata(nokvfs_meta::MetadataError::PredicateFailed))
                if mode == XattrSetMode::Create =>
            {
                reply.error(Errno::EEXIST)
            }
            Err(MetadError::Metadata(nokvfs_meta::MetadataError::PredicateFailed))
                if mode == XattrSetMode::Replace =>
            {
                reply.error(xattr_missing_error())
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match xattr_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service.get_xattr(inode, name) {
            Ok(Some(value)) => reply_xattr_data(value.as_slice(), size, reply),
            Ok(None) => reply.error(xattr_missing_error()),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service.list_xattr(inode) {
            Ok(names) => {
                let mut encoded = Vec::new();
                for name in names {
                    encoded.extend_from_slice(&name);
                    encoded.push(0);
                }
                reply_xattr_data(&encoded, size, reply);
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match xattr_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service.remove_xattr(inode, name) {
            Ok(()) => reply.ok(),
            Err(MetadError::Metadata(nokvfs_meta::MetadataError::PredicateFailed)) => {
                reply.error(xattr_missing_error())
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn access(&self, req: &Request, ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        if let Err(err) = validate_access_mask(mask) {
            reply.error(err);
            return;
        }
        match self.metadata_inode(ino).and_then(|inode| {
            self.service_get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(_) if self.read_only() && mask.contains(AccessFlags::W_OK) => {
                reply.error(Errno::EROFS)
            }
            Ok(attr) if access_allowed(&attr, req.uid(), req.gid(), mask) => reply.ok(),
            Ok(_) => reply.error(Errno::EACCES),
            Err(err) => reply.error(err),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self
            .service
            .create_dir(parent, name, mode & !umask, req.uid(), req.gid())
        {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn mknod(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(link_name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let target = target.as_os_str().as_bytes().to_vec();
        match self
            .service
            .create_symlink(parent, name, target, 0o777, req.uid(), req.gid())
        {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self
            .service
            .create_file(parent, name, mode & !umask, req.uid(), req.gid())
        {
            Ok(entry) => {
                match self.allocate_write_handle(&entry.attr, parent, entry.dentry.name.clone()) {
                    Ok(handle) => {
                        self.remember_entry(&entry);
                        reply.created(
                            &self.options.entry_ttl,
                            &self.view_file_attr(&entry.attr),
                            Generation(entry.attr.generation),
                            handle,
                            FopenFlags::empty(),
                        );
                    }
                    Err(err) => reply.error(err),
                }
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service.remove_file(parent, &name) {
            Ok(entry) => {
                if let Ok(mut parents) = self.parents.write() {
                    parents.remove(&entry.attr.inode.get());
                }
                if let Ok(mut names) = self.names.write() {
                    names.remove(&entry.attr.inode.get());
                }
                reply.ok();
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service.remove_empty_dir(parent, &name) {
            Ok(entry) => {
                if let Ok(mut parents) = self.parents.write() {
                    parents.remove(&entry.attr.inode.get());
                }
                if let Ok(mut names) = self.names.write() {
                    names.remove(&entry.attr.inode.get());
                }
                reply.ok();
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if !flags.is_empty() {
            reply.error(Errno::EINVAL);
            return;
        }
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let newparent = match self.metadata_inode(newparent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let newname = match dentry_name(newname) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.rename_entry(parent, &name, newparent, newname) {
            Ok(result) => {
                self.remember_entry(&result.entry);
                if let Some(replaced) = result.replaced {
                    if let Ok(mut parents) = self.parents.write() {
                        parents.remove(&replaced.attr.inode.get());
                    }
                    if let Ok(mut names) = self.names.write() {
                        names.remove(&replaced.attr.inode.get());
                    }
                }
                reply.ok();
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        match self.write_to_handle(fh, offset, data) {
            Ok(written) => reply.written(written as u32),
            Err(err) => reply.error(err),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.ok();
            return;
        }
        match self.publish_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.ok();
            return;
        }
        match self.release_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.ok();
            return;
        }
        match self.publish_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }
}

fn inode_id(ino: INodeNo) -> Result<InodeId, Errno> {
    InodeId::new(ino.0).map_err(|_| Errno::EINVAL)
}

fn dentry_name(name: &OsStr) -> Result<DentryName, Errno> {
    DentryName::new(name.as_bytes().to_vec()).map_err(|_| Errno::EINVAL)
}

fn time_or_now_ms(value: TimeOrNow) -> u64 {
    match value {
        TimeOrNow::SpecificTime(time) => system_time_ms(time),
        TimeOrNow::Now => system_time_ms(SystemTime::now()),
    }
}

fn system_time_ms(time: SystemTime) -> u64 {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

fn fuse_manifest_id(parent: InodeId, inode: InodeId) -> String {
    format!("fuse/{}/{}", parent.get(), inode.get())
}

fn child_index_from_offset(offset: u64) -> Option<usize> {
    let raw = offset.saturating_sub(FUSE_FIRST_CHILD_OFFSET);
    usize::try_from(raw).ok()
}

fn child_offset(index: usize) -> u64 {
    u64::try_from(index)
        .unwrap_or(u64::MAX.saturating_sub(FUSE_FIRST_CHILD_OFFSET))
        .saturating_add(FUSE_FIRST_CHILD_OFFSET)
}

fn xattr_unsupported_error() -> Errno {
    Errno::EOPNOTSUPP
}

fn xattr_missing_error() -> Errno {
    Errno::NO_XATTR
}

fn xattr_name(name: &OsStr) -> Result<&[u8], Errno> {
    let name = name.as_bytes();
    if name.is_empty() || name.contains(&0) {
        return Err(Errno::EINVAL);
    }
    Ok(name)
}

fn xattr_set_mode(flags: i32) -> Result<XattrSetMode, Errno> {
    if flags & !(XATTR_CREATE | XATTR_REPLACE) != 0 {
        return Err(Errno::EINVAL);
    }
    match (flags & XATTR_CREATE != 0, flags & XATTR_REPLACE != 0) {
        (false, false) => Ok(XattrSetMode::Any),
        (true, false) => Ok(XattrSetMode::Create),
        (false, true) => Ok(XattrSetMode::Replace),
        (true, true) => Err(Errno::EINVAL),
    }
}

fn reply_xattr_data(data: &[u8], size: u32, reply: ReplyXattr) {
    if size == 0 {
        reply.size(u32::try_from(data.len()).unwrap_or(u32::MAX));
        return;
    }
    let requested = usize::try_from(size).unwrap_or(usize::MAX);
    if requested < data.len() {
        reply.error(Errno::ERANGE);
    } else {
        reply.data(data);
    }
}

fn validate_access_mask(mask: AccessFlags) -> Result<(), Errno> {
    let supported = AccessFlags::R_OK | AccessFlags::W_OK | AccessFlags::X_OK;
    if mask.bits() & !supported.bits() == 0 {
        Ok(())
    } else {
        Err(Errno::EINVAL)
    }
}

fn access_allowed(attr: &InodeAttr, uid: u32, gid: u32, mask: AccessFlags) -> bool {
    if mask.is_empty() {
        return true;
    }
    if uid == 0 {
        return !mask.contains(AccessFlags::X_OK)
            || attr.file_type == FileType::Directory
            || attr.mode & 0o111 != 0;
    }
    let shift = if uid == attr.uid {
        6
    } else if gid == attr.gid {
        3
    } else {
        0
    };
    let perms = (attr.mode >> shift) & 0o7;
    (!mask.contains(AccessFlags::R_OK) || perms & 0o4 != 0)
        && (!mask.contains(AccessFlags::W_OK) || perms & 0o2 != 0)
        && (!mask.contains(AccessFlags::X_OK) || perms & 0o1 != 0)
}

fn errno(err: MetadError) -> Errno {
    match err {
        MetadError::Model(_) => Errno::EINVAL,
        MetadError::InvalidPath(_) => Errno::EINVAL,
        MetadError::NotFound => Errno::ENOENT,
        MetadError::NotFile => Errno::EISDIR,
        MetadError::NotDirectory => Errno::ENOTDIR,
        MetadError::DirectoryNotEmpty => Errno::ENOTEMPTY,
        MetadError::CannotRemoveRoot => Errno::EBUSY,
        MetadError::StaleBodyGeneration { .. } => Errno::ESTALE,
        MetadError::MissingBodyDescriptor
        | MetadError::Metadata(_)
        | MetadError::Object(_)
        | MetadError::PublishArtifactFailed { .. }
        | MetadError::Codec(_)
        | MetadError::BodySizeMismatch { .. }
        | MetadError::InvalidPreparedArtifact(_)
        | MetadError::AllocatorExhausted => Errno::EIO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_meta::holtstore::HoltMetadataStore;
    use nokvfs_meta::PublishArtifact;
    use nokvfs_object::MemoryObjectStore;
    use nokvfs_types::MountId;

    fn service() -> NoKvFs<HoltMetadataStore, MemoryObjectStore> {
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            MemoryObjectStore::new(),
        );
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        service
    }

    #[test]
    fn parent_cache_defaults_to_root_and_remembers_lookup_parent() {
        let service = service();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        let child = InodeId::new(99).unwrap();
        assert_eq!(fuse.parent_of(child), InodeId::root());
        fuse.remember_parent(child, InodeId::new(9).unwrap());
        assert_eq!(fuse.parent_of(child), InodeId::new(9).unwrap());
    }

    #[test]
    fn directory_handle_loads_live_entries_by_page() {
        let service = service();
        let total = FUSE_READDIR_PAGE_SIZE + 3;
        let names = (0..total)
            .map(|index| DentryName::new(format!("sample-{index:04}.bin").into_bytes()).unwrap())
            .collect::<Vec<_>>();
        service
            .create_files_in_dir(InodeId::root(), names, 0o644, 1000, 1000)
            .unwrap();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        let handle = fuse
            .allocate_directory_handle(InodeId::root())
            .expect("open directory handle");

        let before = fuse.service().metadata_service_stats();
        let first = fuse
            .directory_child(handle, 0)
            .expect("load first child")
            .expect("first child exists");
        let after_first = fuse.service().metadata_service_stats();
        assert_eq!(first.dentry.name.as_bytes(), b"sample-0000.bin");
        assert_eq!(
            after_first.read_dir_plus_entry_total - before.read_dir_plus_entry_total,
            FUSE_READDIR_PAGE_SIZE as u64
        );

        let second_page_first = fuse
            .directory_child(handle, FUSE_READDIR_PAGE_SIZE)
            .expect("load second page")
            .expect("second page has entries");
        let after_second = fuse.service().metadata_service_stats();
        assert_eq!(
            second_page_first.dentry.name.as_bytes(),
            format!("sample-{FUSE_READDIR_PAGE_SIZE:04}.bin").as_bytes()
        );
        assert_eq!(
            after_second.read_dir_plus_entry_total - after_first.read_dir_plus_entry_total,
            (total - FUSE_READDIR_PAGE_SIZE) as u64
        );
        assert!(fuse
            .directory_child(handle, total)
            .expect("load past end")
            .is_none());

        fuse.release_directory_handle(handle)
            .expect("release directory handle");
        assert_eq!(
            fuse.directory_child(handle, 0).unwrap_err().code(),
            Errno::EBADF.code()
        );
    }

    #[test]
    fn directory_fsync_requires_valid_directory_handle() {
        let service = service();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        let handle = fuse
            .allocate_directory_handle(InodeId::root())
            .expect("open directory handle");

        fuse.sync_directory_handle(handle)
            .expect("valid directory handle syncs");
        fuse.release_directory_handle(handle)
            .expect("release directory handle");
        assert_eq!(
            fuse.sync_directory_handle(handle).unwrap_err().code(),
            Errno::EBADF.code()
        );
    }

    #[test]
    fn statfs_snapshot_reports_nonzero_capacity_and_name_limit() {
        let service = service();
        let fuse = NoKvFuse::new(service, FuseOptions::default());

        let stat = fuse.statfs_snapshot();

        assert_eq!(stat.bsize, STATFS_BLOCK_SIZE);
        assert_eq!(stat.frsize, STATFS_BLOCK_SIZE);
        assert_eq!(stat.namelen, STATFS_NAME_MAX);
        assert!(stat.blocks > 0);
        assert!(stat.bfree <= stat.blocks);
        assert!(stat.bavail <= stat.bfree);
        assert!(stat.files > 0);
        assert!(stat.ffree <= stat.files);
    }

    #[test]
    fn forget_inode_cache_drops_fuse_parent_name_without_removing_metadata() {
        let service = service();
        let name = DentryName::new(b"cached.bin".to_vec()).unwrap();
        let entry = service
            .create_file(InodeId::root(), name.clone(), 0o644, 1000, 1000)
            .unwrap();
        let inode = entry.attr.inode;
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        fuse.remember_entry(&entry);

        assert_eq!(fuse.parent_of(inode), InodeId::root());
        assert_eq!(fuse.name_of(inode).unwrap(), name);

        fuse.forget_inode_cache(inode);

        assert_eq!(fuse.parent_of(inode), InodeId::root());
        assert_eq!(fuse.name_of(inode).unwrap_err().code(), Errno::EIO.code());
        assert!(fuse
            .service()
            .lookup_plus(InodeId::root(), &entry.dentry.name)
            .unwrap()
            .is_some());
    }

    #[test]
    fn xattr_errors_are_explicit_not_unimplemented() {
        assert_eq!(xattr_unsupported_error().code(), Errno::EOPNOTSUPP.code());
        assert_ne!(xattr_unsupported_error().code(), Errno::ENOSYS.code());
        assert_eq!(xattr_missing_error().code(), Errno::NO_XATTR.code());
        assert_ne!(xattr_missing_error().code(), Errno::ENOSYS.code());
        assert_eq!(
            xattr_name(OsStr::new("user.comment")).unwrap(),
            b"user.comment"
        );
        assert_eq!(
            xattr_name(OsStr::new("")).unwrap_err().code(),
            Errno::EINVAL.code()
        );
        assert_eq!(xattr_set_mode(0).unwrap(), XattrSetMode::Any);
        assert_eq!(xattr_set_mode(XATTR_CREATE).unwrap(), XattrSetMode::Create);
        assert_eq!(
            xattr_set_mode(XATTR_REPLACE).unwrap(),
            XattrSetMode::Replace
        );
        assert_eq!(
            xattr_set_mode(XATTR_CREATE | XATTR_REPLACE)
                .unwrap_err()
                .code(),
            Errno::EINVAL.code()
        );
    }

    #[test]
    fn access_helper_honors_owner_group_other_and_root_execute() {
        let attr = InodeAttr {
            inode: InodeId::new(42).unwrap(),
            file_type: FileType::File,
            mode: 0o640,
            uid: 1000,
            gid: 2000,
            size: 0,
            generation: 1,
            mtime_ms: 1,
            ctime_ms: 1,
        };
        assert!(access_allowed(&attr, 1000, 9, AccessFlags::R_OK));
        assert!(access_allowed(&attr, 1000, 9, AccessFlags::W_OK));
        assert!(access_allowed(&attr, 9, 2000, AccessFlags::R_OK));
        assert!(!access_allowed(&attr, 9, 2000, AccessFlags::W_OK));
        assert!(!access_allowed(&attr, 9, 9, AccessFlags::R_OK));
        assert!(!access_allowed(&attr, 0, 0, AccessFlags::X_OK));

        let executable_dir = InodeAttr {
            file_type: FileType::Directory,
            mode: 0o000,
            ..attr.clone()
        };
        assert!(access_allowed(&executable_dir, 0, 0, AccessFlags::X_OK));
        assert!(validate_access_mask(AccessFlags::from_bits_retain(0x4000)).is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_mount_options_disable_appledouble_sidecars() {
        let options = mount_options(&FuseOptions::default());
        let debug = format!("{options:?}");
        assert!(debug.contains("noappledouble"));
        assert!(debug.contains("noapplexattr"));
    }

    #[test]
    fn rename_entry_uses_plain_rename_for_missing_directory_target() {
        let service = service();
        let old_name = DentryName::new(b"old-dir".to_vec()).unwrap();
        let new_name = DentryName::new(b"new-dir".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), old_name.clone(), 0o755, 1000, 1000)
            .unwrap();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        fuse.remember_entry(&created);

        let renamed = fuse
            .rename_entry(
                InodeId::root(),
                &old_name,
                InodeId::root(),
                new_name.clone(),
            )
            .unwrap();

        assert!(renamed.replaced.is_none());
        assert_eq!(renamed.entry.attr.file_type, FileType::Directory);
        assert_eq!(renamed.entry.dentry.name, new_name);
        assert!(fuse
            .service()
            .lookup_plus(InodeId::root(), &old_name)
            .unwrap()
            .is_none());
        assert_eq!(
            fuse.service()
                .lookup_plus(InodeId::root(), &new_name)
                .unwrap()
                .unwrap()
                .attr
                .file_type,
            FileType::Directory
        );
    }

    #[test]
    fn fuse_service_reads_file_body_by_inode() {
        let service = service();
        let published = service
            .publish_artifact(PublishArtifact {
                parent: InodeId::root(),
                name: DentryName::new(b"checkpoint".to_vec()).unwrap(),
                producer: "fuse-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint".to_owned(),
                bytes: b"0123456789".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        let bytes = fuse
            .service()
            .read_file(published.attr.inode, 4, 3)
            .expect("read range through inode API");
        assert_eq!(bytes, b"456");
    }

    #[test]
    fn snapshot_view_maps_snapshot_root_and_reads_old_body() {
        let service = service();
        let runs = service
            .create_dir(
                InodeId::root(),
                DentryName::new(b"runs".to_vec()).unwrap(),
                0o755,
                1000,
                1000,
            )
            .unwrap();
        let name = DentryName::new(b"checkpoint".to_vec()).unwrap();
        let old = service
            .publish_artifact(PublishArtifact {
                parent: runs.attr.inode,
                name: name.clone(),
                producer: "fuse-test".to_owned(),
                digest_uri: "sha256:old".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint-old".to_owned(),
                bytes: b"old".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        let snapshot = service.snapshot_subtree(runs.attr.inode).unwrap();
        service
            .replace_artifact(PublishArtifact {
                parent: runs.attr.inode,
                name,
                producer: "fuse-test".to_owned(),
                digest_uri: "sha256:new".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint-new".to_owned(),
                bytes: b"new-body".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();

        let fuse = NoKvFuse::new(
            service,
            FuseOptions {
                view: FuseView::Snapshot {
                    snapshot_id: snapshot.snapshot_id,
                    root: snapshot.root,
                },
                ..FuseOptions::default()
            },
        );
        assert!(fuse.read_only());
        assert_eq!(
            fuse.metadata_inode(INodeNo(InodeId::ROOT_RAW)).unwrap(),
            runs.attr.inode
        );
        let root_attr = fuse
            .service_get_attr(runs.attr.inode)
            .unwrap()
            .expect("snapshot root attr");
        assert_eq!(
            fuse.view_file_attr(&root_attr).ino,
            INodeNo(InodeId::ROOT_RAW)
        );

        let handle = fuse
            .allocate_directory_handle(runs.attr.inode)
            .expect("open snapshot root directory");
        let entry = fuse
            .directory_child(handle, 0)
            .expect("read snapshot directory")
            .expect("snapshot entry exists");
        assert_eq!(entry, old);
        assert!(fuse
            .directory_child(handle, 1)
            .expect("read snapshot directory end")
            .is_none());
        assert_eq!(
            fuse.service_read_file(old.attr.inode, 0, old.attr.size as usize)
                .unwrap(),
            b"old"
        );
    }

    #[test]
    fn live_view_can_be_configured_read_only() {
        let service = service();
        let fuse = NoKvFuse::new(
            service,
            FuseOptions {
                access: FuseAccessMode::ReadOnly,
                ..FuseOptions::default()
            },
        );

        assert!(fuse.read_only());
    }

    #[test]
    fn write_handle_publish_updates_file_body() {
        let service = service();
        let name = DentryName::new(b"checkpoint".to_vec()).unwrap();
        let created = service
            .create_file(InodeId::root(), name, 0o644, 1000, 1000)
            .unwrap();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        fuse.remember_entry(&created);
        let handle = fuse
            .open_write_handle(&created.attr, InodeId::root())
            .expect("open write handle");

        assert_eq!(fuse.write_to_handle(handle, 0, b"0123").unwrap(), 4);
        assert_eq!(fuse.write_to_handle(handle, 6, b"89").unwrap(), 2);
        let expected = &[b'0', b'1', b'2', b'3', 0, 0, b'8', b'9'];
        assert_eq!(
            fuse.read_from_handle(handle, 0, 8).unwrap().unwrap(),
            expected
        );

        fuse.publish_handle(handle).expect("publish handle");
        assert_eq!(
            fuse.service()
                .read_file(created.attr.inode, 0, 8)
                .expect("read published body"),
            expected
        );
        let body = fuse
            .service()
            .body_descriptor(created.attr.inode)
            .expect("read body descriptor")
            .expect("body descriptor exists");
        assert_eq!(
            body.digest_uri,
            "sha256:7f35eb30d89706db2f03b9113f85c6509756d7ad5a7d19c80617d771bb9577db"
        );
        fuse.release_handle(handle).expect("release handle");
    }

    #[test]
    fn write_handle_open_does_not_eagerly_fetch_existing_body() {
        let service = service();
        let published = service
            .publish_artifact(PublishArtifact {
                parent: InodeId::root(),
                name: DentryName::new(b"checkpoint".to_vec()).unwrap(),
                producer: "fuse-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint".to_owned(),
                bytes: b"0123456789".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        fuse.remember_entry(&published);
        let before = fuse.service().object_stats();
        let handle = fuse
            .open_write_handle(&published.attr, InodeId::root())
            .expect("open write handle");
        let after_open = fuse.service().object_stats();
        assert_eq!(after_open.object_gets, before.object_gets);

        assert_eq!(
            fuse.read_from_handle(handle, 2, 4).unwrap().unwrap(),
            b"2345"
        );
        assert!(fuse.service().object_stats().object_gets > before.object_gets);
    }

    #[test]
    fn dirty_range_write_overlays_existing_body_until_publish() {
        let service = service();
        let published = service
            .publish_artifact(PublishArtifact {
                parent: InodeId::root(),
                name: DentryName::new(b"checkpoint".to_vec()).unwrap(),
                producer: "fuse-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint".to_owned(),
                bytes: b"abcdefghij".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        fuse.remember_entry(&published);
        let handle = fuse
            .open_write_handle(&published.attr, InodeId::root())
            .expect("open write handle");
        let before_write = fuse.service().object_stats();

        assert_eq!(fuse.write_to_handle(handle, 3, b"XYZ").unwrap(), 3);
        let after_write = fuse.service().object_stats();
        assert_eq!(after_write.object_puts, before_write.object_puts + 1);
        assert_eq!(
            fuse.read_from_handle(handle, 0, 10).unwrap().unwrap(),
            b"abcXYZghij"
        );

        let before_publish = fuse.service().object_stats();
        fuse.publish_handle(handle).expect("publish handle");
        let after_publish = fuse.service().object_stats();
        assert_eq!(after_publish.object_puts, before_publish.object_puts);
        assert_eq!(
            fuse.service()
                .read_file(published.attr.inode, 0, 10)
                .expect("read published body"),
            b"abcXYZghij"
        );
    }

    #[test]
    fn truncate_handle_republishes_manifest_without_reuploading_body() {
        let service = service();
        let published = service
            .publish_artifact(PublishArtifact {
                parent: InodeId::root(),
                name: DentryName::new(b"checkpoint".to_vec()).unwrap(),
                producer: "fuse-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint".to_owned(),
                bytes: b"abcdefghij".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        let fuse = NoKvFuse::new(service, FuseOptions::default());
        fuse.remember_entry(&published);
        let handle = fuse
            .open_write_handle(&published.attr, InodeId::root())
            .expect("open write handle");
        let before_truncate = fuse.service().object_stats();

        fuse.truncate_handle(handle, 3).expect("truncate handle");
        fuse.publish_handle(handle).expect("publish shrink");
        let after_shrink = fuse.service().object_stats();
        assert_eq!(after_shrink.object_puts, before_truncate.object_puts);
        assert_eq!(
            fuse.service()
                .read_file(published.attr.inode, 0, 16)
                .expect("read shrunk body"),
            b"abc"
        );

        fuse.truncate_handle(handle, 6).expect("extend handle");
        fuse.publish_handle(handle).expect("publish sparse extend");
        let after_extend = fuse.service().object_stats();
        assert_eq!(after_extend.object_puts, before_truncate.object_puts);
        assert_eq!(
            fuse.service()
                .read_file(published.attr.inode, 0, 16)
                .expect("read sparse body"),
            b"abc\0\0\0"
        );
    }
}
