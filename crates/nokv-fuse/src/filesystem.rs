use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    AccessFlags, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType as FuseFileType,
    Filesystem, FopenFlags, Generation, INodeNo, MountOption, OpenAccMode, OpenFlags, RenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow,
    WriteFlags,
};
use nokv_client::MetadataClient;
use nokv_meta::{
    DentryWithAttr, MetadError, PublishArtifactRange, PublishArtifactStagedSession,
    ReadDirPlusPage, RenameReplaceResult, UpdateAttr, XattrSetMode,
};
use nokv_object::{
    manifest_digest_uri, BlockCachePolicy, BlockCacheStats, ChunkedWrite, DirtyChunkExtent,
    FileReadPipeline, FileReadPipelineOptions, FileWritePipeline, ObjectError,
    ObjectPrefetchOptions, ObjectPrefetchStats, ObjectReadBlock, ObjectStore, ObjectWritebackStats,
    PendingChunkedWrite, WritebackCacheStats, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
    DEFAULT_S3_MULTIPART_CONCURRENCY,
};
use nokv_types::{
    AdvisoryLockKind, AdvisoryLockRequest, DentryName, FileType, InodeAttr, InodeId,
    SpecialNodeSpec,
};
use sha2::{Digest, Sha256};

use crate::attr::{file_attr, fuse_file_type};
use crate::backend::{ClientFuseBackend, FuseBackend, FuseBackendError};
use crate::invalidation::{FuseInvalidationOptions, FuseInvalidationWorker, InvalidationRegistry};

#[derive(Clone, Debug)]
pub struct FuseOptions {
    pub entry_ttl: Duration,
    pub attr_ttl: Duration,
    pub fs_name: String,
    pub threads: usize,
    pub kernel_cache: bool,
    pub direct_io: bool,
    pub block_cache: BlockCachePolicy,
    pub prefetch: FusePrefetchOptions,
    pub read_pipeline: FileReadPipelineOptions,
    pub writeback: FuseWritebackOptions,
    pub view: FuseView,
    pub access: FuseAccessMode,
    pub invalidation: FuseInvalidationOptions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FusePrefetchOptions {
    pub enabled: bool,
    pub queue_capacity: usize,
    pub workers: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FuseWritebackOptions {
    pub enabled: bool,
    pub root: PathBuf,
    pub max_bytes: u64,
    pub max_items: usize,
    pub queue_capacity: usize,
    pub workers: usize,
    pub upload_workers_per_request: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FuseObjectPipelineStats {
    pub block_cache: Option<BlockCacheStats>,
    pub prefetch: Option<ObjectPrefetchStats>,
    pub writeback_cache: Option<WritebackCacheStats>,
    pub writeback: Option<ObjectWritebackStats>,
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

#[derive(Clone, Copy, Debug)]
struct FuseLockRequest {
    ino: INodeNo,
    owner: fuser::LockOwner,
    start: u64,
    end: u64,
    typ: i32,
    pid: u32,
    wait: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FuseRenameMode {
    ReplaceIfTargetExists,
    #[cfg(target_os = "linux")]
    NoReplace,
}

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

#[derive(Clone, Debug)]
struct ReadHandle {
    attr: InodeAttr,
    reader: FileReadPipeline,
}

#[derive(Clone, Debug)]
struct WriteHandle<P> {
    inode: InodeId,
    parent: InodeId,
    name: DentryName,
    prepared: Option<P>,
    mode: u32,
    uid: u32,
    gid: u32,
    base_size: u64,
    size: u64,
    writer: Option<FileWritePipeline>,
    buffered: Vec<BufferedWriteRange>,
    pending_uploads: Vec<PendingBufferedUpload>,
    sequential_digest: Option<SequentialDigest>,
    dirty: bool,
}

#[derive(Clone, Debug)]
struct BufferedWriteRange {
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct PendingBufferedUpload {
    pending: PendingChunkedWrite,
    ranges: Vec<BufferedWriteRange>,
}

#[derive(Clone)]
struct SequentialDigest {
    hasher: Sha256,
    len: u64,
}

impl SequentialDigest {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            len: 0,
        }
    }

    fn append(&mut self, offset: u64, data: &[u8]) -> bool {
        if offset != self.len {
            return false;
        }
        self.hasher.update(data);
        self.len = self
            .len
            .saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
        true
    }

    fn digest_uri_for_size(&self, size: u64) -> Option<String> {
        if self.len != size {
            return None;
        }
        let digest = self.hasher.clone().finalize();
        Some(format!("sha256:{digest:x}"))
    }
}

impl fmt::Debug for SequentialDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequentialDigest")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct WriteStageReservation<P> {
    prepared: P,
    manifest_id: String,
    block_index_base: u64,
    ranges: Vec<BufferedWriteRange>,
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
const MODE_TYPE_MASK: u32 = 0o170000;
const MODE_NAMED_PIPE: u32 = 0o010000;
const MODE_CHAR_DEVICE: u32 = 0o020000;
const MODE_DIRECTORY: u32 = 0o040000;
const MODE_BLOCK_DEVICE: u32 = 0o060000;
const MODE_REGULAR_FILE: u32 = 0o100000;
const MODE_SYMLINK: u32 = 0o120000;
const MODE_SOCKET: u32 = 0o140000;
const FUSE_WRITEBACK_UPLOAD_THRESHOLD: usize = 1024 * 1024;
const FALLOC_FL_KEEP_SIZE: i32 = 0x01;
const DEFAULT_WRITEBACK_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_WRITEBACK_CACHE_ITEMS: usize = 16 * 1024;
const DEFAULT_WRITEBACK_QUEUE_CAPACITY: usize = 256;
const DEFAULT_WRITEBACK_WORKERS: usize = DEFAULT_S3_MULTIPART_CONCURRENCY;

impl Default for FuseOptions {
    fn default() -> Self {
        Self {
            entry_ttl: Duration::from_secs(1),
            attr_ttl: Duration::from_secs(1),
            fs_name: "nokv".to_owned(),
            threads: default_threads(),
            kernel_cache: true,
            direct_io: false,
            block_cache: BlockCachePolicy::default(),
            prefetch: FusePrefetchOptions::default(),
            read_pipeline: FileReadPipelineOptions::default(),
            writeback: FuseWritebackOptions::default(),
            view: FuseView::Live,
            access: FuseAccessMode::ReadWrite,
            invalidation: FuseInvalidationOptions::default(),
        }
    }
}

impl Default for FusePrefetchOptions {
    fn default() -> Self {
        let defaults = ObjectPrefetchOptions::default();
        Self {
            enabled: true,
            queue_capacity: defaults.queue_capacity,
            workers: defaults.workers,
        }
    }
}

impl From<FusePrefetchOptions> for ObjectPrefetchOptions {
    fn from(options: FusePrefetchOptions) -> Self {
        Self {
            queue_capacity: options.queue_capacity,
            workers: options.workers,
        }
    }
}

impl Default for FuseWritebackOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            root: std::env::temp_dir().join(format!("nokv-writeback-{}", std::process::id())),
            max_bytes: DEFAULT_WRITEBACK_CACHE_BYTES,
            max_items: DEFAULT_WRITEBACK_CACHE_ITEMS,
            queue_capacity: DEFAULT_WRITEBACK_QUEUE_CAPACITY,
            workers: DEFAULT_WRITEBACK_WORKERS,
            upload_workers_per_request: DEFAULT_WRITEBACK_WORKERS,
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
            writer: None,
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
    let backend = ClientFuseBackend::new(metadata, objects, &options)
        .map_err(|err| io::Error::other(err.to_string()))?;
    mount_backend(backend, mountpoint, options)
}

fn mount_backend<B>(
    backend: B,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<()>
where
    B: FuseBackend,
{
    let mut config = Config::default();
    let mount_options = mount_options(&options);
    config.mount_options = mount_options;
    config.n_threads = Some(options.threads);
    let backend = Arc::new(backend);
    let fuse = NoKvFuse::from_shared_backend(Arc::clone(&backend), options.clone());
    let registry = fuse.invalidation_registry();
    let session = fuser::spawn_mount2(fuse, mountpoint, &config)?;
    let _invalidation_worker = if options.view == FuseView::Live {
        Some(FuseInvalidationWorker::spawn(
            backend,
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
        mount_options.push(MountOption::CUSTOM("fstypename=nokv".to_owned()));
        mount_options.push(MountOption::CUSTOM(format!("volname={}", options.fs_name)));
        // NoKV does not persist Finder/resource-fork metadata yet. Ask macFUSE
        // to reject Apple sidecars instead of creating visible ._ files.
        mount_options.push(MountOption::CUSTOM("noappledouble".to_owned()));
        mount_options.push(MountOption::CUSTOM("noapplexattr".to_owned()));
    }
    #[cfg(not(target_os = "macos"))]
    {
        mount_options.push(MountOption::Subtype("nokv".to_owned()));
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

fn inode_id(ino: INodeNo) -> Result<InodeId, Errno> {
    InodeId::new(ino.0).map_err(|_| Errno::EINVAL)
}

fn file_type_from_mknod_mode(mode: u32) -> Result<FileType, Errno> {
    match mode & MODE_TYPE_MASK {
        0 | MODE_REGULAR_FILE => Ok(FileType::File),
        MODE_NAMED_PIPE => Ok(FileType::NamedPipe),
        MODE_CHAR_DEVICE => Ok(FileType::CharDevice),
        MODE_BLOCK_DEVICE => Ok(FileType::BlockDevice),
        MODE_SOCKET => Ok(FileType::Socket),
        MODE_DIRECTORY | MODE_SYMLINK => Err(Errno::EINVAL),
        _ => Err(Errno::EINVAL),
    }
}

fn advisory_lock_kind_from_fuse(typ: i32) -> Result<AdvisoryLockKind, Errno> {
    if typ == i32::from(libc::F_RDLCK) {
        Ok(AdvisoryLockKind::Read)
    } else if typ == i32::from(libc::F_WRLCK) {
        Ok(AdvisoryLockKind::Write)
    } else if typ == i32::from(libc::F_UNLCK) {
        Ok(AdvisoryLockKind::Unlock)
    } else {
        Err(Errno::EINVAL)
    }
}

fn advisory_lock_kind_to_fuse(kind: AdvisoryLockKind) -> Result<i32, Errno> {
    match kind {
        AdvisoryLockKind::Read => Ok(i32::from(libc::F_RDLCK)),
        AdvisoryLockKind::Write => Ok(i32::from(libc::F_WRLCK)),
        AdvisoryLockKind::Unlock => Ok(i32::from(libc::F_UNLCK)),
    }
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

fn staged_range_block_count(offset: u64, len: usize) -> Result<u64, Errno> {
    let mut range_offset = 0_usize;
    let mut count = 0_u64;
    while range_offset < len {
        let logical_offset = offset
            .checked_add(u64::try_from(range_offset).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        let chunk_start = (logical_offset / DEFAULT_CHUNK_SIZE).saturating_mul(DEFAULT_CHUNK_SIZE);
        let next_chunk = chunk_start
            .checked_add(DEFAULT_CHUNK_SIZE)
            .ok_or(Errno::EINVAL)?;
        let remaining_in_chunk =
            usize::try_from(next_chunk - logical_offset).map_err(|_| Errno::EINVAL)?;
        let write_len = DEFAULT_BLOCK_SIZE
            .min(remaining_in_chunk)
            .min(len - range_offset);
        if write_len == 0 {
            return Err(Errno::EINVAL);
        }
        count = count.saturating_add(1);
        range_offset += write_len;
    }
    Ok(count)
}

fn push_buffered_write(ranges: &mut Vec<BufferedWriteRange>, offset: u64, data: &[u8]) {
    if let Some(last) = ranges.last_mut() {
        let last_end = last.offset.saturating_add(last.bytes.len() as u64);
        if last_end == offset {
            last.bytes.extend_from_slice(data);
            return;
        }
    }
    ranges.push(BufferedWriteRange {
        offset,
        bytes: data.to_vec(),
    });
}

fn take_buffered_upload_ranges(
    ranges: &mut Vec<BufferedWriteRange>,
    force: bool,
) -> Result<Vec<BufferedWriteRange>, Errno> {
    let mut upload = Vec::new();
    let mut retained = Vec::new();
    for mut range in ranges.drain(..) {
        if range.bytes.is_empty() {
            continue;
        }
        let upload_len = if force {
            range.bytes.len()
        } else {
            (range.bytes.len() / FUSE_WRITEBACK_UPLOAD_THRESHOLD) * FUSE_WRITEBACK_UPLOAD_THRESHOLD
        };
        if upload_len == 0 {
            retained.push(range);
            continue;
        }
        if upload_len == range.bytes.len() {
            upload.push(range);
            continue;
        }
        let tail = range.bytes.split_off(upload_len);
        let tail_offset = range
            .offset
            .checked_add(u64::try_from(upload_len).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        upload.push(range);
        retained.push(BufferedWriteRange {
            offset: tail_offset,
            bytes: tail,
        });
    }
    *ranges = retained;
    Ok(upload)
}

fn buffered_ranges_block_count(ranges: &[BufferedWriteRange]) -> Result<u64, Errno> {
    ranges.iter().try_fold(0_u64, |count, range| {
        staged_range_block_count(range.offset, range.bytes.len())
            .map(|next| count.saturating_add(next))
    })
}

fn buffered_publish_ranges(ranges: &[BufferedWriteRange]) -> Vec<PublishArtifactRange> {
    ranges
        .iter()
        .map(|range| PublishArtifactRange {
            offset: range.offset,
            bytes: range.bytes.clone(),
        })
        .collect()
}

fn cleanup_written_objects<B: FuseBackend>(
    backend: &B,
    written: &ChunkedWrite,
) -> Result<(), Errno> {
    let staged = written.staged_objects().map_err(errno)?;
    backend.cleanup_staged_objects(&staged).map_err(errno)
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

fn resolve_fallocate_size(
    current_size: u64,
    offset: u64,
    length: u64,
    mode: i32,
) -> Result<Option<u64>, Errno> {
    if length == 0 {
        return Err(Errno::EINVAL);
    }
    let end = offset.checked_add(length).ok_or(Errno::EINVAL)?;
    match mode {
        0 => Ok(Some(current_size.max(end))),
        FALLOC_FL_KEEP_SIZE => Ok(None),
        _ => Err(Errno::EOPNOTSUPP),
    }
}

fn resolve_lseek(size: u64, offset: i64, whence: i32) -> Result<i64, Errno> {
    match whence {
        libc::SEEK_SET => {
            u64::try_from(offset).map_err(|_| Errno::EINVAL)?;
            Ok(offset)
        }
        libc::SEEK_END => {
            let size = i128::from(size);
            let next = size + i128::from(offset);
            if !(0..=i128::from(i64::MAX)).contains(&next) {
                return Err(Errno::EINVAL);
            }
            Ok(next as i64)
        }
        libc::SEEK_DATA => {
            let offset = u64::try_from(offset).map_err(|_| Errno::EINVAL)?;
            if offset >= size {
                return Err(Errno::ENXIO);
            }
            i64::try_from(offset).map_err(|_| Errno::EINVAL)
        }
        libc::SEEK_HOLE => {
            let offset = u64::try_from(offset).map_err(|_| Errno::EINVAL)?;
            if offset > size {
                return Err(Errno::ENXIO);
            }
            i64::try_from(size).map_err(|_| Errno::EINVAL)
        }
        // FUSE does not pass the kernel's current file offset, so SEEK_CUR
        // cannot be answered accurately at the filesystem boundary.
        libc::SEEK_CUR => Err(Errno::EINVAL),
        _ => Err(Errno::EINVAL),
    }
}

fn fuse_rename_mode(flags: RenameFlags) -> Result<FuseRenameMode, Errno> {
    if flags.is_empty() {
        return Ok(FuseRenameMode::ReplaceIfTargetExists);
    }
    #[cfg(target_os = "linux")]
    {
        if flags == RenameFlags::RENAME_NOREPLACE {
            return Ok(FuseRenameMode::NoReplace);
        }
    }
    Err(Errno::EINVAL)
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

fn errno(err: impl Into<FuseBackendError>) -> Errno {
    match err.into() {
        FuseBackendError::Metadata(err) => metadata_errno(err),
        FuseBackendError::Client(nokv_client::ClientError::Metadata(err)) => metadata_errno(err),
        FuseBackendError::Client(nokv_client::ClientError::NotFound(_)) => Errno::ENOENT,
        FuseBackendError::Client(nokv_client::ClientError::NotDirectory(_)) => Errno::ENOTDIR,
        FuseBackendError::Client(nokv_client::ClientError::ForwardToLeader { .. }) => Errno::EAGAIN,
        FuseBackendError::Client(nokv_client::ClientError::LockConflict(_)) => Errno::EAGAIN,
        FuseBackendError::Client(nokv_client::ClientError::Object(_))
        | FuseBackendError::Client(nokv_client::ClientError::Io(_))
        | FuseBackendError::Client(nokv_client::ClientError::Protocol(_))
        | FuseBackendError::Client(nokv_client::ClientError::ReadNotFresh { .. })
        | FuseBackendError::Client(nokv_client::ClientError::EmptyPath)
        | FuseBackendError::Client(nokv_client::ClientError::RelativePath)
        | FuseBackendError::Client(nokv_client::ClientError::ParentTraversal)
        | FuseBackendError::Client(nokv_client::ClientError::InvalidArtifactPath(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsDirectory(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsFile(_))
        | FuseBackendError::Client(nokv_client::ClientError::InvalidName(_))
        | FuseBackendError::Client(nokv_client::ClientError::RootHasNoParent)
        | FuseBackendError::Object(_) => Errno::EIO,
    }
}

fn metadata_errno(err: MetadError) -> Errno {
    match err {
        MetadError::Model(_) => Errno::EINVAL,
        MetadError::InvalidPath(_) => Errno::EINVAL,
        MetadError::NotFound => Errno::ENOENT,
        MetadError::NotFile => Errno::EISDIR,
        MetadError::NotDirectory => Errno::ENOTDIR,
        MetadError::DirectoryNotEmpty => Errno::ENOTEMPTY,
        MetadError::CannotRemoveRoot => Errno::EBUSY,
        MetadError::StaleBodyGeneration { .. } => Errno::ESTALE,
        MetadError::LockConflict(_) => Errno::EAGAIN,
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
    use crate::backend::FuseBackendResult;
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
    fn lseek_resolves_end_data_and_hole_offsets() {
        assert_eq!(resolve_lseek(100, 5, libc::SEEK_SET).unwrap(), 5);
        assert_eq!(resolve_lseek(100, -10, libc::SEEK_END).unwrap(), 90);
        assert_eq!(resolve_lseek(100, 10, libc::SEEK_DATA).unwrap(), 10);
        assert_eq!(resolve_lseek(100, 10, libc::SEEK_HOLE).unwrap(), 100);
        assert_eq!(resolve_lseek(100, 100, libc::SEEK_HOLE).unwrap(), 100);
    }

    #[test]
    fn fallocate_size_resolves_sparse_extension() {
        assert_eq!(resolve_fallocate_size(10, 3, 4, 0).unwrap(), Some(10));
        assert_eq!(resolve_fallocate_size(10, 8, 5, 0).unwrap(), Some(13));
        assert_eq!(
            resolve_fallocate_size(10, 8, 5, FALLOC_FL_KEEP_SIZE).unwrap(),
            None
        );
    }

    #[test]
    fn fallocate_rejects_empty_overflow_or_unsupported_mode() {
        assert_eq!(
            resolve_fallocate_size(10, 0, 0, 0).unwrap_err().code(),
            Errno::EINVAL.code()
        );
        assert_eq!(
            resolve_fallocate_size(10, u64::MAX, 1, 0)
                .unwrap_err()
                .code(),
            Errno::EINVAL.code()
        );
        assert_eq!(
            resolve_fallocate_size(10, 0, 1, 0x02).unwrap_err().code(),
            Errno::EOPNOTSUPP.code()
        );
    }

    #[test]
    fn lseek_rejects_invalid_or_unanswerable_offsets() {
        assert_eq!(
            resolve_lseek(100, -1, libc::SEEK_SET).unwrap_err().code(),
            Errno::EINVAL.code()
        );
        assert_eq!(
            resolve_lseek(100, 100, libc::SEEK_DATA).unwrap_err().code(),
            Errno::ENXIO.code()
        );
        assert_eq!(
            resolve_lseek(100, 101, libc::SEEK_HOLE).unwrap_err().code(),
            Errno::ENXIO.code()
        );
        assert_eq!(
            resolve_lseek(100, 0, libc::SEEK_CUR).unwrap_err().code(),
            Errno::EINVAL.code()
        );
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

    #[test]
    fn access_helper_honors_owner_group_other_and_root_execute() {
        let attr = InodeAttr {
            inode: InodeId::new(42).unwrap(),
            file_type: FileType::File,
            mode: 0o640,
            uid: 1000,
            gid: 2000,
            rdev: 0,
            nlink: 1,
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
