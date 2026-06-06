use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use fuser::{
    AccessFlags, BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileAttr, FileHandle,
    FileType as FuseFileType, Filesystem, FopenFlags, Generation, INodeNo, OpenAccMode, OpenFlags,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty,
    ReplyEntry, ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request,
    TimeOrNow, WriteFlags,
};
use nokv_client::MetadataClient;
use nokv_meta::{
    DentryWithAttr, MetadError, PublishArtifactStagedSession, ReadDirPlusPage, RenameReplaceResult,
    UpdateAttr, XattrSetMode,
};
use nokv_object::{
    manifest_digest_uri, DirtyChunkExtent, FileReadPipeline, ObjectError, ObjectReadBlock,
    ObjectStore, PendingChunkedWrite,
};
use nokv_types::{AdvisoryLockRequest, DentryName, FileType, InodeAttr, InodeId, SpecialNodeSpec};

use crate::attr::{file_attr, fuse_file_type};
use crate::backend::{ClientFuseBackend, FuseBackend, FuseBackendError};
use crate::invalidation::{FuseInvalidationWorker, InvalidationRegistry};

mod directory;
mod errors;
mod locks;
mod options;
mod posix;
mod write_session;
mod xattr;

pub use options::{
    FuseAccessMode, FuseObjectPipelineStats, FuseOptions, FusePrefetchOptions, FuseView,
    FuseWritebackOptions,
};

use directory::{
    child_index_from_offset, child_offset, DirectoryHandle, FUSE_DOT_DOT_OFFSET, FUSE_DOT_OFFSET,
    FUSE_READDIR_PAGE_SIZE,
};
use errors::errno;
use locks::{
    advisory_lock_kind_from_fuse, advisory_lock_kind_to_fuse, fuse_rename_mode, FuseLockRequest,
    FuseRenameMode,
};
use options::{
    mount_options, FuseStatfs, STATFS_BLOCK_SIZE, STATFS_NAME_MAX, STATFS_TOTAL_BYTES,
    STATFS_TOTAL_FILES,
};
use posix::{
    access_allowed, copy_file_range_size, dentry_name, file_type_from_mknod_mode, inode_id,
    resolve_fallocate_size, resolve_lseek, system_time_ms, time_or_now_ms, validate_access_mask,
};
use write_session::{
    buffered_publish_ranges, buffered_ranges_block_count, cleanup_written_objects,
    fuse_manifest_id, push_buffered_write, take_buffered_upload_ranges, BufferedWriteRange,
    PendingBufferedUpload, SequentialDigest, WriteHandle, WriteStageReservation,
};
#[cfg(test)]
use write_session::{staged_range_block_count, FUSE_WRITEBACK_UPLOAD_THRESHOLD};
use xattr::{
    reply_xattr_data, xattr_missing_error, xattr_name, xattr_set_mode, xattr_unsupported_error,
};

pub(crate) struct NoKvFuse<B: FuseBackend> {
    backend: Arc<B>,
    options: FuseOptions,
    parents: RwLock<HashMap<u64, u64>>,
    names: RwLock<HashMap<u64, Vec<u8>>>,
    next_handle: AtomicU64,
    read_handles: RwLock<HashMap<u64, ReadHandle>>,
    write_handles: RwLock<HashMap<u64, WriteHandle<B::Prepared>>>,
    directory_handles: RwLock<HashMap<u64, DirectoryHandle>>,
    invalidation: Arc<InvalidationRegistry>,
}

pub struct FuseMount {
    session: fuser::BackgroundSession,
    stats: Arc<dyn Fn() -> io::Result<FuseObjectPipelineStats> + Send + Sync>,
    _invalidation_worker: Option<FuseInvalidationWorker>,
}

impl FuseMount {
    pub fn object_pipeline_stats(&self) -> io::Result<FuseObjectPipelineStats> {
        (self.stats)()
    }

    pub fn join(self) -> io::Result<()> {
        self.session.join()
    }
}

#[derive(Clone, Debug)]
struct ReadHandle {
    attr: InodeAttr,
    reader: FileReadPipeline,
}

impl<B> NoKvFuse<B>
where
    B: FuseBackend,
{
    #[cfg(test)]
    pub(crate) fn from_backend(backend: B, options: FuseOptions) -> Self {
        Self::from_shared_backend(Arc::new(backend), options)
    }

    pub(crate) fn from_shared_backend(backend: Arc<B>, options: FuseOptions) -> Self {
        let mut parents = HashMap::new();
        parents.insert(options.view.root().get(), options.view.root().get());
        let invalidation = Arc::new(InvalidationRegistry::default());
        let fuse = Self {
            backend,
            options,
            parents: RwLock::new(parents),
            names: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            read_handles: RwLock::new(HashMap::new()),
            write_handles: RwLock::new(HashMap::new()),
            directory_handles: RwLock::new(HashMap::new()),
            invalidation,
        };
        fuse.register_watch_scope(fuse.options.view.root());
        fuse
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
        if let Ok(Some(cursor)) = self.backend.watch_subtree(scope) {
            self.invalidation.register_scope(scope, cursor);
        }
    }

    #[cfg(test)]
    pub(crate) fn object_pipeline_stats(&self) -> Result<FuseObjectPipelineStats, Errno> {
        self.backend.object_pipeline_stats().map_err(errno)
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

    fn advisory_lock_request(
        &self,
        request: FuseLockRequest,
    ) -> Result<AdvisoryLockRequest, Errno> {
        Ok(AdvisoryLockRequest {
            inode: self.metadata_inode(request.ino)?,
            owner: request.owner.0,
            start: request.start,
            end: request.end,
            kind: advisory_lock_kind_from_fuse(request.typ)?,
            pid: request.pid,
            wait: request.wait,
        })
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

    fn read_open_flags(&self) -> FopenFlags {
        if self.options.direct_io {
            FopenFlags::FOPEN_DIRECT_IO
        } else if self.options.kernel_cache {
            FopenFlags::FOPEN_KEEP_CACHE
        } else {
            FopenFlags::empty()
        }
    }

    fn write_open_flags(&self) -> FopenFlags {
        if self.options.direct_io {
            FopenFlags::FOPEN_DIRECT_IO
        } else {
            FopenFlags::empty()
        }
    }

    fn read_only(&self) -> bool {
        self.options.access.is_read_only() || self.options.view.is_read_only()
    }

    fn service_get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.get_attr(inode),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.backend.get_attr_at_snapshot(snapshot_id, inode)
            }
        }
    }

    fn service_lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.lookup_plus(parent, name),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.backend
                    .lookup_plus_at_snapshot(snapshot_id, parent, name)
            }
        }
    }

    fn service_read_dir_plus_page(
        &self,
        inode: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.read_dir_plus_page(inode, after, limit),
            FuseView::Snapshot { snapshot_id, .. } => {
                let requested = limit.max(1);
                let rows = self.backend.read_dir_plus_at_snapshot(snapshot_id, inode)?;
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
    ) -> Result<RenameReplaceResult, FuseBackendError> {
        if self.backend.lookup_plus(new_parent, &new_name)?.is_some() {
            self.backend
                .rename_replace(parent, name, new_parent, new_name)
        } else {
            self.backend
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
    ) -> Result<Vec<u8>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.read_file(inode, offset, len),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.backend
                    .read_file_at_snapshot(snapshot_id, inode, offset, len)
            }
        }
    }

    fn service_read_file_with_known_attr_pipeline(
        &self,
        attr: &InodeAttr,
        offset: u64,
        len: usize,
        pipeline: &mut FileReadPipeline,
    ) -> Result<Vec<u8>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self
                .backend
                .read_file_with_known_attr_pipeline(attr, offset, len, pipeline),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.backend
                    .read_file_at_snapshot(snapshot_id, attr.inode, offset, len)
            }
        }
    }

    fn service_read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.read_symlink(inode),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.backend.read_symlink_at_snapshot(snapshot_id, inode)
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

    fn allocate_read_handle(&self, attr: InodeAttr) -> Result<FileHandle, Errno> {
        let raw = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.read_handles.write().map_err(|_| Errno::EIO)?.insert(
            raw,
            ReadHandle {
                attr,
                reader: FileReadPipeline::new(self.options.read_pipeline),
            },
        );
        Ok(FileHandle(raw))
    }

    fn read_from_read_handle(
        &self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Option<Vec<u8>>, Errno> {
        let Some((attr, mut reader)) = self
            .read_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .map(|handle| (handle.attr.clone(), handle.reader.clone()))
        else {
            return Ok(None);
        };
        let bytes = self
            .service_read_file_with_known_attr_pipeline(&attr, offset, size as usize, &mut reader)
            .map_err(errno)?;
        if let Some(handle) = self
            .read_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .get_mut(&fh.0)
        {
            handle.reader = reader;
        }
        Ok(Some(bytes))
    }

    fn read_for_copy(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, Errno> {
        if let Some(bytes) = self.read_from_handle(fh, offset, size)? {
            return Ok(bytes);
        }
        if let Some(bytes) = self.read_from_read_handle(fh, offset, size)? {
            return Ok(bytes);
        }
        let inode = self.metadata_inode(ino)?;
        self.service_read_file(inode, offset, size as usize)
            .map_err(errno)
    }

    fn release_read_handle(&self, fh: FileHandle) -> Result<bool, Errno> {
        Ok(self
            .read_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .remove(&fh.0)
            .is_some())
    }

    fn lseek_file_size(&self, ino: INodeNo, fh: FileHandle) -> Result<u64, Errno> {
        if let Some(size) = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .map(|handle| handle.size)
        {
            return Ok(size);
        }
        if let Some(size) = self
            .read_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .map(|handle| handle.attr.size)
        {
            return Ok(size);
        }
        let inode = self.metadata_inode(ino)?;
        match self.service_get_attr(inode).map_err(errno)? {
            Some(attr) if attr.file_type == FileType::File => Ok(attr.size),
            Some(_) => Err(Errno::EINVAL),
            None => Err(Errno::ENOENT),
        }
    }

    fn ensure_fallocated_range(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
    ) -> Result<(), Errno> {
        let current_size = self.lseek_file_size(ino, fh)?;
        let size = resolve_fallocate_size(current_size, offset, length, mode)?;
        let has_write_handle = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .contains_key(&fh.0);
        if !has_write_handle {
            return Err(Errno::EBADF);
        }
        let Some(size) = size else {
            return Ok(());
        };
        if size <= current_size {
            return Ok(());
        }
        self.truncate_handle(fh, size)?;
        self.publish_handle(fh)
    }

    fn allocate_handle(&self, handle: WriteHandle<B::Prepared>) -> Result<FileHandle, Errno> {
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
        self.allocate_write_handle_with_session(attr, parent, name, None)
    }

    fn allocate_write_handle_with_session(
        &self,
        attr: &InodeAttr,
        parent: InodeId,
        name: DentryName,
        prepared: Option<B::Prepared>,
    ) -> Result<FileHandle, Errno> {
        let writer = prepared
            .as_ref()
            .map(|prepared| {
                self.backend
                    .new_write_pipeline(prepared, &fuse_manifest_id(parent, attr.inode))
                    .map_err(errno)
            })
            .transpose()?;
        self.allocate_handle(WriteHandle {
            inode: attr.inode,
            parent,
            name,
            prepared,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            base_size: attr.size,
            size: attr.size,
            writer,
            buffered: Vec::new(),
            pending_uploads: Vec::new(),
            sequential_digest: Some(SequentialDigest::new()),
            dirty: false,
        })
    }

    fn write_to_handle(&self, fh: FileHandle, offset: u64, data: &[u8]) -> Result<usize, Errno> {
        let end = offset
            .checked_add(u64::try_from(data.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        if data.is_empty() {
            return Ok(0);
        }
        {
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
            if handle.prepared.is_none() {
                handle.prepared = Some(
                    self.backend
                        .prepare_artifact_replace(handle.parent, handle.name.clone())
                        .map_err(errno)?,
                );
            }
            let prepared = handle.prepared.clone().ok_or(Errno::EIO)?;
            if handle.writer.is_none() {
                handle.writer = Some(
                    self.backend
                        .new_write_pipeline(
                            &prepared,
                            &fuse_manifest_id(handle.parent, handle.inode),
                        )
                        .map_err(errno)?,
                );
            }
            if let Some(digest) = handle.sequential_digest.as_mut() {
                if !digest.append(offset, data) {
                    handle.sequential_digest = None;
                }
            }
            push_buffered_write(&mut handle.buffered, offset, data);
            handle.size = handle.size.max(end);
            handle.dirty = true;
        }
        self.flush_handle_buffers(fh, false)?;
        Ok(data.len())
    }

    fn truncate_handle(&self, fh: FileHandle, size: u64) -> Result<(), Errno> {
        let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
        let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
        if handle.prepared.is_none() {
            handle.prepared = Some(
                self.backend
                    .prepare_artifact_replace(handle.parent, handle.name.clone())
                    .map_err(errno)?,
            );
        }
        if handle.writer.is_none() {
            let prepared = handle.prepared.as_ref().ok_or(Errno::EIO)?;
            handle.writer = Some(
                self.backend
                    .new_write_pipeline(prepared, &fuse_manifest_id(handle.parent, handle.inode))
                    .map_err(errno)?,
            );
        }
        handle.size = size;
        match handle.sequential_digest.as_ref().map(|digest| digest.len) {
            Some(len) if len == size => {}
            Some(_) if size == 0 => handle.sequential_digest = Some(SequentialDigest::new()),
            Some(_) => handle.sequential_digest = None,
            None => {}
        }
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
        if let Some(writer) = &handle.writer {
            self.overlay_dirty_extents(&mut bytes, offset, writer.dirty_extents())?;
        }
        for upload in &handle.pending_uploads {
            self.overlay_buffered_ranges(&mut bytes, offset, &upload.ranges)?;
        }
        self.overlay_buffered_ranges(&mut bytes, offset, &handle.buffered)?;
        Ok(Some(bytes))
    }

    fn cleanup_completed_pending(&self, pending: PendingChunkedWrite) -> Result<(), Errno> {
        match pending.wait() {
            Ok(written) => cleanup_written_objects(&*self.backend, &written),
            Err(err) => {
                let _ = pending.discard_writeback_cache();
                Err(self.cleanup_object_error(err))
            }
        }
    }

    fn cleanup_object_error(&self, err: ObjectError) -> Errno {
        if let ObjectError::StagedWriteFailed { staged, .. } = &err {
            let _ = self.backend.cleanup_staged_objects(staged);
        }
        errno(err)
    }

    fn drain_handle_uploads(&self, fh: FileHandle) -> Result<(), Errno> {
        loop {
            let uploads = {
                let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
                let Some(handle) = handles.get_mut(&fh.0) else {
                    return Err(Errno::EBADF);
                };
                std::mem::take(&mut handle.pending_uploads)
            };
            if uploads.is_empty() {
                return Ok(());
            }
            let mut completed = Vec::with_capacity(uploads.len());
            let mut failed_ranges = Vec::new();
            let mut first_error = None;
            for upload in uploads {
                match upload.pending.wait() {
                    Ok(written) => completed.push(written),
                    Err(err) => {
                        let errno = self.cleanup_object_error(err);
                        let _ = upload.pending.discard_writeback_cache();
                        if first_error.is_none() {
                            first_error = Some(errno);
                        }
                        failed_ranges.extend(upload.ranges);
                    }
                }
            }
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let Some(handle) = handles.get_mut(&fh.0) else {
                drop(handles);
                for written in completed {
                    cleanup_written_objects(&*self.backend, &written)?;
                }
                return Err(Errno::EBADF);
            };
            let Some(writer) = handle.writer.as_mut() else {
                drop(handles);
                for written in completed {
                    cleanup_written_objects(&*self.backend, &written)?;
                }
                return Err(Errno::EIO);
            };
            for written in completed {
                writer.record_write(written).map_err(errno)?;
            }
            if !failed_ranges.is_empty() {
                let current = std::mem::take(&mut handle.buffered);
                handle.buffered = failed_ranges.into_iter().chain(current).collect();
            }
            if let Some(err) = first_error {
                return Err(err);
            }
        }
    }

    fn flush_handle_buffers(&self, fh: FileHandle, force: bool) -> Result<(), Errno> {
        loop {
            let reservation = {
                let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
                let Some(handle) = handles.get_mut(&fh.0) else {
                    return Err(Errno::EBADF);
                };
                let ranges = take_buffered_upload_ranges(&mut handle.buffered, force)?;
                if ranges.is_empty() {
                    return Ok(());
                }
                let prepared = handle.prepared.clone().ok_or(Errno::EIO)?;
                let writer = handle.writer.as_mut().ok_or(Errno::EIO)?;
                let block_count = buffered_ranges_block_count(&ranges)?;
                let block_index_base = writer.reserve_blocks(block_count);
                WriteStageReservation {
                    prepared,
                    manifest_id: writer.options().manifest_id.clone(),
                    block_index_base,
                    ranges,
                }
            };
            let publish_ranges = buffered_publish_ranges(&reservation.ranges);
            let pending = match self.backend.stage_prepared_artifact_ranges_async(
                &reservation.prepared,
                &reservation.manifest_id,
                &publish_ranges,
                reservation.block_index_base,
            ) {
                Ok(pending) => pending,
                Err(err) => {
                    self.restore_buffered_ranges(fh, reservation.ranges);
                    return Err(errno(err));
                }
            };
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let Some(handle) = handles.get_mut(&fh.0) else {
                drop(handles);
                self.cleanup_completed_pending(pending)?;
                return Err(Errno::EBADF);
            };
            if handle
                .prepared
                .as_ref()
                .map(|prepared| self.backend.prepared_generation(prepared))
                != Some(self.backend.prepared_generation(&reservation.prepared))
            {
                drop(handles);
                self.cleanup_completed_pending(pending)?;
                return Err(Errno::ESTALE);
            }
            handle.pending_uploads.push(PendingBufferedUpload {
                pending,
                ranges: reservation.ranges,
            });
            if !force {
                return Ok(());
            }
        }
    }

    fn restore_buffered_ranges(&self, fh: FileHandle, ranges: Vec<BufferedWriteRange>) {
        let Ok(mut handles) = self.write_handles.write() else {
            return;
        };
        let Some(handle) = handles.get_mut(&fh.0) else {
            return;
        };
        let current = std::mem::take(&mut handle.buffered);
        handle.buffered = ranges.into_iter().chain(current).collect();
    }

    fn overlay_dirty_extents(
        &self,
        output: &mut [u8],
        output_offset: u64,
        extents: &[DirtyChunkExtent],
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
                        .backend
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

    fn overlay_buffered_ranges(
        &self,
        output: &mut [u8],
        output_offset: u64,
        ranges: &[BufferedWriteRange],
    ) -> Result<(), Errno> {
        if output.is_empty() {
            return Ok(());
        }
        let output_limit = output_offset
            .checked_add(u64::try_from(output.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        for range in ranges {
            let range_end = range
                .offset
                .checked_add(u64::try_from(range.bytes.len()).map_err(|_| Errno::EINVAL)?)
                .ok_or(Errno::EINVAL)?;
            let start = range.offset.max(output_offset);
            let end = range_end.min(output_limit);
            if start >= end {
                continue;
            }
            let source_start = usize::try_from(start - range.offset).map_err(|_| Errno::EINVAL)?;
            let source_end = usize::try_from(end - range.offset).map_err(|_| Errno::EINVAL)?;
            let output_start = usize::try_from(start - output_offset).map_err(|_| Errno::EINVAL)?;
            let output_end = usize::try_from(end - output_offset).map_err(|_| Errno::EINVAL)?;
            output[output_start..output_end]
                .copy_from_slice(&range.bytes[source_start..source_end]);
        }
        Ok(())
    }

    fn publish_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        let has_write_handle = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .contains_key(&fh.0);
        if !has_write_handle {
            return Ok(());
        }
        self.flush_handle_buffers(fh, true)?;
        self.drain_handle_uploads(fh)?;
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
        let writer = snapshot.writer.as_ref().ok_or(Errno::EIO)?;
        let prepared = snapshot.prepared.as_ref().ok_or(Errno::EIO)?;
        let digest_uri = snapshot
            .sequential_digest
            .as_ref()
            .and_then(|digest| digest.digest_uri_for_size(snapshot.size))
            .unwrap_or_else(|| {
                manifest_digest_uri(
                    snapshot.size,
                    self.backend.prepared_generation(prepared),
                    writer.staged_chunks(),
                )
            });
        self.backend
            .publish_prepared_artifact_staged_session(
                prepared.clone(),
                PublishArtifactStagedSession {
                    parent: snapshot.parent,
                    name: snapshot.name,
                    producer: "nokv-fuse".to_owned(),
                    digest_uri,
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: fuse_manifest_id(snapshot.parent, snapshot.inode),
                    size: snapshot.size,
                    chunks: writer.staged_chunks().to_vec(),
                    staged: writer.staged_objects().clone(),
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
            handle.prepared = None;
            handle.writer = None;
            handle.sequential_digest = Some(SequentialDigest::new());
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

pub fn mount_client<O>(
    metadata: MetadataClient,
    objects: O,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<()>
where
    O: ObjectStore + Send + Sync + 'static,
{
    spawn_mount_client(metadata, objects, mountpoint, options).and_then(FuseMount::join)
}

pub fn spawn_mount_client<O>(
    metadata: MetadataClient,
    objects: O,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<FuseMount>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let backend = ClientFuseBackend::new(metadata, objects, &options)
        .map_err(|err| io::Error::other(err.to_string()))?;
    spawn_mount_backend(backend, mountpoint, options)
}

fn spawn_mount_backend<B>(
    backend: B,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<FuseMount>
where
    B: FuseBackend,
{
    let mut config = Config::default();
    let mount_options = mount_options(&options);
    config.mount_options = mount_options;
    config.n_threads = Some(options.threads);
    let backend = Arc::new(backend);
    let stats_backend = Arc::clone(&backend);
    let fuse = NoKvFuse::from_shared_backend(Arc::clone(&backend), options.clone());
    let registry = fuse.invalidation_registry();
    let session = fuser::spawn_mount2(fuse, mountpoint, &config)?;
    let invalidation_worker = if options.view == FuseView::Live {
        Some(FuseInvalidationWorker::spawn(
            backend,
            session.notifier(),
            registry,
            options.invalidation,
        ))
    } else {
        None
    };
    Ok(FuseMount {
        session,
        stats: Arc::new(move || {
            stats_backend
                .object_pipeline_stats()
                .map_err(|err| io::Error::other(err.to_string()))
        }),
        _invalidation_worker: invalidation_worker,
    })
}

impl<B> Filesystem for NoKvFuse<B>
where
    B: FuseBackend,
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
            match self.backend.update_root_attrs(changes) {
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
        match self.backend.update_attrs(parent, &name, changes) {
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
                OpenAccMode::O_RDONLY => match self.allocate_read_handle(attr) {
                    Ok(fh) => reply.opened(fh, self.read_open_flags()),
                    Err(err) => reply.error(err),
                },
                OpenAccMode::O_WRONLY | OpenAccMode::O_RDWR => {
                    if self.read_only() {
                        reply.error(Errno::EROFS);
                        return;
                    }
                    let parent = self.parent_of(attr.inode);
                    match self.open_write_handle(&attr, parent) {
                        Ok(fh) => reply.opened(fh, self.write_open_flags()),
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
        match self.read_from_read_handle(fh, offset, size) {
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
            Ok(handle) => reply.opened(handle, self.read_open_flags()),
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
        match self.backend.set_xattr(inode, name, value.to_vec(), mode) {
            Ok(()) => reply.ok(),
            Err(FuseBackendError::Metadata(MetadError::Metadata(
                nokv_meta::MetadataError::PredicateFailed,
            ))) if mode == XattrSetMode::Create => reply.error(Errno::EEXIST),
            Err(FuseBackendError::Metadata(MetadError::Metadata(
                nokv_meta::MetadataError::PredicateFailed,
            ))) if mode == XattrSetMode::Replace => reply.error(xattr_missing_error()),
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
        match self.backend.get_xattr(inode, name) {
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
        match self.backend.list_xattr(inode) {
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
        match self.backend.remove_xattr(inode, name) {
            Ok(()) => reply.ok(),
            Err(FuseBackendError::Metadata(MetadError::Metadata(
                nokv_meta::MetadataError::PredicateFailed,
            ))) => reply.error(xattr_missing_error()),
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
            .backend
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
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
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
        let file_type = match file_type_from_mknod_mode(mode) {
            Ok(file_type) => file_type,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let mode = mode & !umask & 0o7777;
        let result = if file_type == FileType::File {
            self.backend
                .create_file(parent, name, mode, req.uid(), req.gid())
        } else {
            self.backend.create_special_node(
                parent,
                name,
                SpecialNodeSpec {
                    file_type,
                    mode,
                    rdev,
                    uid: req.uid(),
                    gid: req.gid(),
                },
            )
        };
        match result {
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
            .backend
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
            .backend
            .create_file_prepared(parent, name, mode & !umask, req.uid(), req.gid())
        {
            Ok((entry, prepared)) => {
                match self.allocate_write_handle_with_session(
                    &entry.attr,
                    parent,
                    entry.dentry.name.clone(),
                    Some(prepared),
                ) {
                    Ok(handle) => {
                        self.remember_entry(&entry);
                        reply.created(
                            &self.options.entry_ttl,
                            &self.view_file_attr(&entry.attr),
                            Generation(entry.attr.generation),
                            handle,
                            self.write_open_flags(),
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
        match self.backend.remove_file(parent, &name) {
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
        match self.backend.remove_empty_dir(parent, &name) {
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

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
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
        let new_parent = match self.metadata_inode(newparent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let new_name = match dentry_name(newname) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.link(inode, new_parent, new_name) {
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
        let mode = match fuse_rename_mode(flags) {
            Ok(mode) => mode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
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
        let result = match mode {
            FuseRenameMode::ReplaceIfTargetExists => {
                self.rename_entry(parent, &name, newparent, newname)
            }
            #[cfg(target_os = "linux")]
            FuseRenameMode::NoReplace => self
                .backend
                .rename(parent, &name, newparent, newname)
                .map(|entry| RenameReplaceResult {
                    entry,
                    replaced: None,
                }),
        };
        match result {
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
            match self.release_read_handle(fh) {
                Ok(_) => reply.ok(),
                Err(err) => reply.error(err),
            }
            return;
        }
        match self.release_read_handle(fh) {
            Ok(true) => {
                reply.ok();
                return;
            }
            Ok(false) => {}
            Err(err) => {
                reply.error(err);
                return;
            }
        }
        match self.release_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn getlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let request = match self.advisory_lock_request(FuseLockRequest {
            ino,
            owner: lock_owner,
            start,
            end,
            typ,
            pid,
            wait: false,
        }) {
            Ok(request) => request,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.get_advisory_lock(request) {
            Ok(Some(lock)) => match advisory_lock_kind_to_fuse(lock.kind) {
                Ok(typ) => reply.locked(lock.start, lock.end, typ, lock.pid),
                Err(err) => reply.error(err),
            },
            Ok(None) => reply.locked(start, end, i32::from(libc::F_UNLCK), 0),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn setlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let request = match self.advisory_lock_request(FuseLockRequest {
            ino,
            owner: lock_owner,
            start,
            end,
            typ,
            pid,
            wait: sleep,
        }) {
            Ok(request) => request,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.set_advisory_lock(request) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno(err)),
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

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        match self.ensure_fallocated_range(ino, fh, offset, length, mode) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        fh_in: FileHandle,
        offset_in: u64,
        _ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        if !flags.is_empty() {
            reply.error(Errno::EOPNOTSUPP);
            return;
        }
        let size = copy_file_range_size(len);
        if size == 0 {
            reply.written(0);
            return;
        }
        let bytes = match self.read_for_copy(ino_in, fh_in, offset_in, size) {
            Ok(bytes) => bytes,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if bytes.is_empty() {
            reply.written(0);
            return;
        }
        match self.write_to_handle(fh_out, offset_out, &bytes) {
            Ok(written) => reply.written(written as u32),
            Err(err) => reply.error(err),
        }
    }

    fn lseek(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let size = match self.lseek_file_size(ino, fh) {
            Ok(size) => size,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match resolve_lseek(size, offset, whence) {
            Ok(offset) => reply.offset(offset),
            Err(err) => reply.error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::FuseBackendResult;
    use nokv_meta::PublishArtifactRange;
    use nokv_object::{
        BlockCacheStats, FileWritePipeline, ObjectPrefetchStats, ObjectWritebackStats,
        WritebackCacheStats, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
    };
    use nokv_types::{WatchCursor, WatchRecord};

    #[test]
    fn parent_cache_defaults_to_root_and_remembers_lookup_parent() {
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend, FuseOptions::default());
        let child = InodeId::new(99).unwrap();
        assert_eq!(fuse.parent_of(child), InodeId::root());
        fuse.remember_parent(child, InodeId::new(9).unwrap());
        assert_eq!(fuse.parent_of(child), InodeId::new(9).unwrap());
    }

    #[test]
    fn statfs_snapshot_reports_nonzero_capacity_and_name_limit() {
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend, FuseOptions::default());

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
    fn open_flags_follow_fuse_cache_options() {
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend, FuseOptions::default());
        assert_eq!(fuse.read_open_flags(), FopenFlags::FOPEN_KEEP_CACHE);
        assert_eq!(fuse.write_open_flags(), FopenFlags::empty());

        let fuse = NoKvFuse::from_backend(
            UnsupportedTestBackend,
            FuseOptions {
                kernel_cache: false,
                ..FuseOptions::default()
            },
        );
        assert_eq!(fuse.read_open_flags(), FopenFlags::empty());
        assert_eq!(fuse.write_open_flags(), FopenFlags::empty());

        let fuse = NoKvFuse::from_backend(
            UnsupportedTestBackend,
            FuseOptions {
                direct_io: true,
                ..FuseOptions::default()
            },
        );
        assert_eq!(fuse.read_open_flags(), FopenFlags::FOPEN_DIRECT_IO);
        assert_eq!(fuse.write_open_flags(), FopenFlags::FOPEN_DIRECT_IO);
    }

    #[test]
    fn object_pipeline_stats_default_for_backend_without_object_pipeline() {
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend, FuseOptions::default());
        assert_eq!(
            fuse.object_pipeline_stats().unwrap(),
            FuseObjectPipelineStats::default()
        );
    }

    #[test]
    fn object_pipeline_stats_convert_to_object_transfer_stats() {
        let stats = FuseObjectPipelineStats {
            block_cache: Some(BlockCacheStats {
                hits: 2,
                hit_bytes: 20,
                ..BlockCacheStats::default()
            }),
            prefetch: Some(ObjectPrefetchStats {
                enqueued: 3,
                dropped: 1,
                completed: 2,
                failed: 1,
                object_gets: 4,
                object_get_bytes: 40,
                cache_hits: 5,
                cache_hit_bytes: 50,
            }),
            writeback_cache: Some(WritebackCacheStats {
                staged: 6,
                staged_bytes: 60,
                ..WritebackCacheStats::default()
            }),
            writeback: Some(ObjectWritebackStats {
                enqueued: 7,
                inline: 1,
                fallback: 2,
                completed: 3,
                failed: 4,
                staged_bytes: 70,
                uploaded_bytes: 80,
                queue_wait_ns: 90,
                queue_max_wait_ns: 91,
                upload_ns: 100,
                upload_max_ns: 101,
            }),
            read_plan_cache_hits: 8,
            read_plan_cache_misses: 9,
        };

        let object = stats.object_transfer_stats();

        assert_eq!(object.cache_hits, 7);
        assert_eq!(object.cache_hit_bytes, 70);
        assert_eq!(object.prefetch_enqueued, 3);
        assert_eq!(object.prefetch_object_gets, 4);
        assert_eq!(object.read_plan_cache_hits, 8);
        assert_eq!(object.read_plan_cache_misses, 9);
        assert_eq!(object.object_writeback_enqueued, 7);
        assert_eq!(object.object_writeback_inline, 1);
        assert_eq!(object.object_writeback_fallback, 2);
        assert_eq!(object.object_writeback_completed, 3);
        assert_eq!(object.object_writeback_failed, 4);
        assert_eq!(object.object_writeback_staged_bytes, 70);
        assert_eq!(object.object_writeback_uploaded_bytes, 80);
        assert_eq!(object.object_writeback_queue_wait_ns, 90);
        assert_eq!(object.object_writeback_queue_max_wait_ns, 91);
        assert_eq!(object.object_writeback_upload_ns, 100);
        assert_eq!(object.object_writeback_upload_max_ns, 101);
    }

    #[test]
    fn staged_range_block_count_matches_chunked_object_boundaries() {
        assert_eq!(staged_range_block_count(0, 0).unwrap(), 0);
        assert_eq!(staged_range_block_count(0, 1).unwrap(), 1);
        assert_eq!(
            staged_range_block_count(0, DEFAULT_BLOCK_SIZE + 1).unwrap(),
            2
        );
        assert_eq!(
            staged_range_block_count(DEFAULT_CHUNK_SIZE - 1, 2).unwrap(),
            2
        );
    }

    #[test]
    fn manifest_digest_is_stable_and_manifest_scoped() {
        let chunks = vec![nokv_object::StoredChunk {
            chunk_index: 0,
            logical_offset: 0,
            len: 4,
            blocks: vec![nokv_object::StoredBlock {
                object_key: "blocks/1/2/3/0/0".to_owned(),
                logical_offset: 0,
                object_offset: 0,
                len: 4,
                digest_uri: "sha256:block".to_owned(),
            }],
        }];

        let first = manifest_digest_uri(4, 3, &chunks);
        let second = manifest_digest_uri(4, 3, &chunks);
        assert_eq!(first, second);
        assert!(first.starts_with("manifest-sha256:"));
        assert_ne!(first, manifest_digest_uri(4, 4, &chunks));

        let mut changed = chunks.clone();
        changed[0].blocks[0].digest_uri = "sha256:changed".to_owned();
        assert_ne!(first, manifest_digest_uri(4, 3, &changed));
    }

    #[test]
    fn buffered_writes_coalesce_until_upload_threshold_is_ready() {
        let mut buffered = Vec::new();
        push_buffered_write(
            &mut buffered,
            0,
            &vec![1; FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2],
        );
        assert!(take_buffered_upload_ranges(&mut buffered, false)
            .unwrap()
            .is_empty());

        push_buffered_write(
            &mut buffered,
            (FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2) as u64,
            &vec![2; FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2],
        );
        let upload = take_buffered_upload_ranges(&mut buffered, false).unwrap();

        assert!(buffered.is_empty());
        assert_eq!(upload.len(), 1);
        assert_eq!(upload[0].offset, 0);
        assert_eq!(upload[0].bytes.len(), FUSE_WRITEBACK_UPLOAD_THRESHOLD);
        assert_eq!(upload[0].bytes[0], 1);
        assert_eq!(upload[0].bytes[FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2], 2);
    }

    #[test]
    fn buffered_upload_keeps_tail_until_flush() {
        let mut buffered = Vec::new();
        push_buffered_write(
            &mut buffered,
            0,
            &vec![7; FUSE_WRITEBACK_UPLOAD_THRESHOLD + 17],
        );

        let upload = take_buffered_upload_ranges(&mut buffered, false).unwrap();
        assert_eq!(upload.len(), 1);
        assert_eq!(upload[0].bytes.len(), FUSE_WRITEBACK_UPLOAD_THRESHOLD);
        assert_eq!(buffered.len(), 1);
        assert_eq!(buffered[0].offset, FUSE_WRITEBACK_UPLOAD_THRESHOLD as u64);
        assert_eq!(buffered[0].bytes.len(), 17);

        let tail = take_buffered_upload_ranges(&mut buffered, true).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].offset, FUSE_WRITEBACK_UPLOAD_THRESHOLD as u64);
        assert_eq!(tail[0].bytes.len(), 17);
        assert!(buffered.is_empty());
    }

    #[test]
    fn failed_pending_writeback_restores_dirty_range_for_retry() {
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend, FuseOptions::default());
        let inode = InodeId::new(7).unwrap();
        let fh = fuse
            .allocate_write_handle(
                &InodeAttr {
                    inode,
                    file_type: FileType::File,
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    nlink: 1,
                    size: 0,
                    generation: 1,
                    mtime_ms: 1,
                    ctime_ms: 1,
                },
                InodeId::root(),
                DentryName::new("checkpoint.bin").unwrap(),
            )
            .unwrap();

        {
            let mut handles = fuse.write_handles.write().unwrap();
            let handle = handles.get_mut(&fh.0).unwrap();
            handle.prepared = Some(());
            handle.size = 10;
            handle.dirty = true;
            handle.writer = Some(
                FileWritePipeline::new(nokv_object::ChunkWriteOptions {
                    manifest_id: "fuse/1/7".to_owned(),
                    mount: 1,
                    inode: inode.get(),
                    generation: 1,
                    chunk_size: DEFAULT_CHUNK_SIZE,
                    block_size: DEFAULT_BLOCK_SIZE,
                })
                .unwrap(),
            );
            handle.pending_uploads.push(PendingBufferedUpload {
                pending: PendingChunkedWrite::ready(Err(ObjectError::Backend(
                    "temporary object outage".to_owned(),
                ))),
                ranges: vec![BufferedWriteRange {
                    offset: 0,
                    bytes: b"checkpoint".to_vec(),
                }],
            });
        }

        assert_eq!(
            fuse.drain_handle_uploads(fh).unwrap_err().code(),
            Errno::EIO.code()
        );
        let handles = fuse.write_handles.read().unwrap();
        let handle = handles.get(&fh.0).unwrap();
        assert!(handle.pending_uploads.is_empty());
        assert_eq!(handle.buffered.len(), 1);
        assert_eq!(handle.buffered[0].offset, 0);
        assert_eq!(handle.buffered[0].bytes, b"checkpoint");
        assert!(handle.writer.as_ref().unwrap().staged_chunks().is_empty());
        drop(handles);

        fuse.publish_handle(fh).unwrap();
        let handles = fuse.write_handles.read().unwrap();
        let handle = handles.get(&fh.0).unwrap();
        assert!(handle.buffered.is_empty());
        assert!(handle.pending_uploads.is_empty());
        assert!(handle.writer.is_none());
        assert!(!handle.dirty);
        assert_eq!(handle.base_size, 10);
    }

    #[test]
    fn read_from_write_handle_overlays_pending_upload_without_waiting() {
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend, FuseOptions::default());
        let inode = InodeId::new(7).unwrap();
        let fh = fuse
            .allocate_write_handle(
                &InodeAttr {
                    inode,
                    file_type: FileType::File,
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    nlink: 1,
                    size: 0,
                    generation: 1,
                    mtime_ms: 1,
                    ctime_ms: 1,
                },
                InodeId::root(),
                DentryName::new("checkpoint.bin").unwrap(),
            )
            .unwrap();

        {
            let mut handles = fuse.write_handles.write().unwrap();
            let handle = handles.get_mut(&fh.0).unwrap();
            handle.size = 10;
            handle.dirty = true;
            handle.pending_uploads.push(PendingBufferedUpload {
                pending: PendingChunkedWrite::ready(Err(ObjectError::Backend(
                    "upload should not be awaited by read".to_owned(),
                ))),
                ranges: vec![BufferedWriteRange {
                    offset: 0,
                    bytes: b"checkpoint".to_vec(),
                }],
            });
        }

        assert_eq!(
            fuse.read_from_handle(fh, 0, 10).unwrap().unwrap(),
            b"checkpoint"
        );
        let handles = fuse.write_handles.read().unwrap();
        assert_eq!(handles.get(&fh.0).unwrap().pending_uploads.len(), 1);
    }

    #[test]
    fn read_handle_uses_pipeline_and_is_released() {
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend, FuseOptions::default());
        let inode = InodeId::new(7).unwrap();
        let fh = fuse
            .allocate_read_handle(InodeAttr {
                inode,
                file_type: FileType::File,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                nlink: 1,
                size: 16,
                generation: 1,
                mtime_ms: 1,
                ctime_ms: 1,
            })
            .unwrap();

        let first = fuse.read_from_read_handle(fh, 0, 4).unwrap().unwrap();
        let second = fuse.read_from_read_handle(fh, 4, 4).unwrap().unwrap();

        assert_eq!(first, b"abcd");
        assert_eq!(second, b"efgh");
        assert!(fuse.release_read_handle(fh).unwrap());
        assert!(!fuse.release_read_handle(fh).unwrap());
        assert!(fuse.read_from_read_handle(fh, 0, 4).unwrap().is_none());
    }

    #[test]
    fn rename_flags_select_supported_modes() {
        assert_eq!(
            fuse_rename_mode(RenameFlags::empty()).unwrap(),
            FuseRenameMode::ReplaceIfTargetExists
        );

        #[cfg(target_os = "linux")]
        {
            assert_eq!(
                fuse_rename_mode(RenameFlags::RENAME_NOREPLACE).unwrap(),
                FuseRenameMode::NoReplace
            );
            assert_eq!(
                fuse_rename_mode(RenameFlags::RENAME_EXCHANGE)
                    .unwrap_err()
                    .code(),
                Errno::EINVAL.code()
            );
            assert_eq!(
                fuse_rename_mode(RenameFlags::RENAME_NOREPLACE | RenameFlags::RENAME_EXCHANGE)
                    .unwrap_err()
                    .code(),
                Errno::EINVAL.code()
            );
        }

        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(
                fuse_rename_mode(RenameFlags::from_bits_retain(1))
                    .unwrap_err()
                    .code(),
                Errno::EINVAL.code()
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_mount_options_disable_appledouble_sidecars() {
        let options = mount_options(&FuseOptions::default());
        let debug = format!("{options:?}");
        assert!(debug.contains("noappledouble"));
        assert!(debug.contains("noapplexattr"));
    }

    struct UnsupportedTestBackend;

    impl FuseBackend for UnsupportedTestBackend {
        type Prepared = ();

        fn prepared_generation(&self, _prepared: &Self::Prepared) -> u64 {
            1
        }

        fn watch_subtree(&self, _scope: InodeId) -> FuseBackendResult<Option<WatchCursor>> {
            unsupported()
        }

        fn replay_watch(
            &self,
            _scope: InodeId,
            _cursor: WatchCursor,
            _limit: usize,
        ) -> FuseBackendResult<Vec<WatchRecord>> {
            unsupported()
        }

        fn get_attr(&self, _inode: InodeId) -> FuseBackendResult<Option<InodeAttr>> {
            unsupported()
        }

        fn get_attr_at_snapshot(
            &self,
            _snapshot_id: u64,
            _inode: InodeId,
        ) -> FuseBackendResult<Option<InodeAttr>> {
            unsupported()
        }

        fn lookup_plus(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<Option<DentryWithAttr>> {
            unsupported()
        }

        fn lookup_plus_at_snapshot(
            &self,
            _snapshot_id: u64,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<Option<DentryWithAttr>> {
            unsupported()
        }

        fn read_dir_plus_page(
            &self,
            _inode: InodeId,
            _after: Option<&DentryName>,
            _limit: usize,
        ) -> FuseBackendResult<ReadDirPlusPage> {
            unsupported()
        }

        fn read_dir_plus_at_snapshot(
            &self,
            _snapshot_id: u64,
            _inode: InodeId,
        ) -> FuseBackendResult<Vec<DentryWithAttr>> {
            unsupported()
        }

        fn rename(
            &self,
            _parent: InodeId,
            _name: &DentryName,
            _new_parent: InodeId,
            _new_name: DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn rename_replace(
            &self,
            _parent: InodeId,
            _name: &DentryName,
            _new_parent: InodeId,
            _new_name: DentryName,
        ) -> FuseBackendResult<RenameReplaceResult> {
            unsupported()
        }

        fn read_file(
            &self,
            _inode: InodeId,
            _offset: u64,
            _len: usize,
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn read_file_with_pipeline(
            &self,
            _inode: InodeId,
            offset: u64,
            len: usize,
            pipeline: &mut FileReadPipeline,
        ) -> FuseBackendResult<Vec<u8>> {
            let store = nokv_object::MemoryObjectStore::new();
            let key = nokv_object::ObjectKey::new("blocks/test/read").unwrap();
            store.put(&key, b"abcdefghijklmnop").unwrap();
            let outcome = pipeline
                .read_blocks::<_, nokv_object::MemoryBlockCache>(
                    &store,
                    None,
                    16,
                    offset,
                    len,
                    &[ObjectReadBlock {
                        object_key: key.as_str().to_owned(),
                        object_offset: offset,
                        len,
                        output_offset: 0,
                    }],
                )
                .unwrap();
            Ok(outcome.blocks.bytes)
        }

        fn read_file_at_snapshot(
            &self,
            _snapshot_id: u64,
            _inode: InodeId,
            _offset: u64,
            _len: usize,
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn read_symlink(&self, _inode: InodeId) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn read_symlink_at_snapshot(
            &self,
            _snapshot_id: u64,
            _inode: InodeId,
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn update_root_attrs(&self, _changes: UpdateAttr) -> FuseBackendResult<InodeAttr> {
            unsupported()
        }

        fn update_attrs(
            &self,
            _parent: InodeId,
            _name: &DentryName,
            _changes: UpdateAttr,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn set_xattr(
            &self,
            _inode: InodeId,
            _name: &[u8],
            _value: Vec<u8>,
            _mode: XattrSetMode,
        ) -> FuseBackendResult<()> {
            unsupported()
        }

        fn get_xattr(&self, _inode: InodeId, _name: &[u8]) -> FuseBackendResult<Option<Vec<u8>>> {
            unsupported()
        }

        fn list_xattr(&self, _inode: InodeId) -> FuseBackendResult<Vec<Vec<u8>>> {
            unsupported()
        }

        fn remove_xattr(&self, _inode: InodeId, _name: &[u8]) -> FuseBackendResult<()> {
            unsupported()
        }

        fn get_advisory_lock(
            &self,
            _request: AdvisoryLockRequest,
        ) -> FuseBackendResult<Option<nokv_types::AdvisoryLock>> {
            unsupported()
        }

        fn set_advisory_lock(&self, _request: AdvisoryLockRequest) -> FuseBackendResult<()> {
            unsupported()
        }

        fn create_dir(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn create_file(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn create_file_prepared(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<(DentryWithAttr, Self::Prepared)> {
            unsupported()
        }

        fn create_symlink(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _target: Vec<u8>,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn create_special_node(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _spec: SpecialNodeSpec,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn link(
            &self,
            _inode: InodeId,
            _new_parent: InodeId,
            _new_name: DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn remove_file(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn remove_empty_dir(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn prepare_artifact_replace(
            &self,
            _parent: InodeId,
            _name: DentryName,
        ) -> FuseBackendResult<Self::Prepared> {
            unsupported()
        }

        fn new_write_pipeline(
            &self,
            _prepared: &Self::Prepared,
            manifest_id: &str,
        ) -> FuseBackendResult<FileWritePipeline> {
            FileWritePipeline::new(nokv_object::ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: 1,
                inode: 7,
                generation: 1,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            })
            .map_err(FuseBackendError::Object)
        }

        fn stage_prepared_artifact_ranges(
            &self,
            _prepared: &Self::Prepared,
            manifest_id: &str,
            ranges: &[PublishArtifactRange],
            block_index_base: u64,
        ) -> FuseBackendResult<nokv_object::ChunkedWrite> {
            let mut blocks = Vec::new();
            let mut size = 0_u64;
            for (index, range) in ranges.iter().enumerate() {
                if range.bytes.is_empty() {
                    continue;
                }
                let len = range.bytes.len() as u64;
                size = size.max(range.offset.saturating_add(len));
                blocks.push(nokv_object::StoredBlock {
                    object_key: format!(
                        "blocks/test/{}",
                        block_index_base.saturating_add(index as u64)
                    ),
                    logical_offset: range.offset,
                    object_offset: 0,
                    len,
                    digest_uri: format!("sha256:test-{index}"),
                });
            }
            Ok(nokv_object::ChunkedWrite {
                manifest_id: manifest_id.to_owned(),
                size,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
                chunks: vec![nokv_object::StoredChunk {
                    chunk_index: 0,
                    logical_offset: 0,
                    len: size,
                    blocks,
                }],
                object_puts: ranges.len(),
                object_put_bytes: ranges.iter().map(|range| range.bytes.len() as u64).sum(),
            })
        }

        fn cleanup_staged_objects(
            &self,
            _staged: &nokv_object::StagedObjectSet,
        ) -> FuseBackendResult<()> {
            Ok(())
        }

        fn read_session_object_blocks(
            &self,
            _output_len: usize,
            _blocks: &[ObjectReadBlock],
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn publish_prepared_artifact_staged_session(
            &self,
            _prepared: Self::Prepared,
            request: PublishArtifactStagedSession,
        ) -> FuseBackendResult<RenameReplaceResult> {
            let inode = InodeId::new(7).unwrap();
            let attr = InodeAttr {
                inode,
                file_type: FileType::File,
                mode: request.mode,
                uid: request.uid,
                gid: request.gid,
                rdev: 0,
                nlink: 1,
                size: request.size,
                generation: 1,
                mtime_ms: 1,
                ctime_ms: 1,
            };
            Ok(RenameReplaceResult {
                entry: DentryWithAttr {
                    dentry: nokv_types::DentryRecord {
                        parent: request.parent,
                        name: request.name,
                        child: inode,
                        child_type: FileType::File,
                        attr_generation: attr.generation,
                    },
                    attr,
                    body: None,
                },
                replaced: None,
            })
        }
    }

    fn unsupported<T>() -> FuseBackendResult<T> {
        Err(FuseBackendError::Metadata(MetadError::Codec(
            "test backend".to_owned(),
        )))
    }
}
