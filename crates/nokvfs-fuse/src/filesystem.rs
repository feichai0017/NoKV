use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType as FuseFileType, Filesystem,
    FopenFlags, Generation, INodeNo, MountOption, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite, Request, TimeOrNow, WriteFlags,
};
use nokvfs_meta::command::MetadataStore;
use nokvfs_meta::{
    DentryWithAttr, MetadError, NoKvFs, PreparedArtifact, PublishArtifactRange,
    PublishArtifactStagedSession, UpdateAttr,
};
use nokvfs_object::{ObjectReadBlock, ObjectStore, StagedObjectSet, StoredChunk};
use nokvfs_types::{DentryName, FileType, InodeAttr, InodeId};

use crate::attr::{file_attr, fuse_file_type};
use crate::invalidation::{FuseInvalidationOptions, FuseInvalidationWorker, InvalidationRegistry};

#[derive(Clone, Debug)]
pub struct FuseOptions {
    pub entry_ttl: Duration,
    pub attr_ttl: Duration,
    pub fs_name: String,
    pub threads: usize,
    pub view: FuseView,
    pub invalidation: FuseInvalidationOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuseView {
    Live,
    Snapshot { snapshot_id: u64, root: InodeId },
}

pub struct NoKvFuse<M, O> {
    service: Arc<NoKvFs<M, O>>,
    options: FuseOptions,
    parents: RwLock<HashMap<u64, u64>>,
    names: RwLock<HashMap<u64, Vec<u8>>>,
    next_handle: AtomicU64,
    write_handles: RwLock<HashMap<u64, WriteHandle>>,
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

impl Default for FuseOptions {
    fn default() -> Self {
        Self {
            entry_ttl: Duration::from_secs(1),
            attr_ttl: Duration::from_secs(1),
            fs_name: "nokv-fs".to_owned(),
            threads: default_threads(),
            view: FuseView::Live,
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
        self.options.view.is_read_only()
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

    fn service_read_dir_plus(&self, inode: InodeId) -> Result<Vec<DentryWithAttr>, MetadError> {
        match self.options.view {
            FuseView::Live => self.service.read_dir_plus(inode),
            FuseView::Snapshot { snapshot_id, .. } => {
                self.service.read_dir_plus_at_snapshot(snapshot_id, inode)
            }
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

    fn directory_entries(&self, inode: InodeId) -> Result<(InodeAttr, Vec<DentryWithAttr>), Errno> {
        let attr = self
            .service_get_attr(inode)
            .map_err(errno)?
            .ok_or(Errno::ENOENT)?;
        if attr.file_type != FileType::Directory {
            return Err(Errno::ENOTDIR);
        }
        let entries = self.service_read_dir_plus(inode).map_err(errno)?;
        for entry in &entries {
            self.remember_entry(entry);
        }
        Ok((attr, entries))
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
        self.service
            .publish_prepared_artifact_staged_session(
                snapshot.prepared.ok_or(Errno::EIO)?,
                PublishArtifactStagedSession {
                    parent: snapshot.parent,
                    name: snapshot.name,
                    producer: "nokv-fuse".to_owned(),
                    digest_uri: "unknown".to_owned(),
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
    let mut mount_options = vec![MountOption::FSName(options.fs_name.clone())];
    if options.view.is_read_only() {
        mount_options.push(MountOption::RO);
    } else {
        mount_options.push(MountOption::RW);
    }
    #[cfg(target_os = "macos")]
    {
        mount_options.push(MountOption::CUSTOM("fstypename=nokvfs".to_owned()));
        mount_options.push(MountOption::CUSTOM(format!("volname={}", options.fs_name)));
    }
    #[cfg(not(target_os = "macos"))]
    {
        mount_options.push(MountOption::Subtype("nokv-fs".to_owned()));
        mount_options.push(MountOption::NoAtime);
    }
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

impl<M, O> Filesystem for NoKvFuse<M, O>
where
    M: MetadataStore + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
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
            .and_then(|inode| self.directory_entries(inode).map(|_| ()))
        {
            Ok(()) => reply.opened(FileHandle(0), FopenFlags::FOPEN_KEEP_CACHE),
            Err(err) => reply.error(err),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
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
        let Ok((_attr, entries)) = self.directory_entries(inode) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let parent = self.parent_of(inode);
        if self.add_dirent(&mut reply, offset, 1, inode, FuseFileType::Directory, ".")
            || self.add_dirent(&mut reply, offset, 2, parent, FuseFileType::Directory, "..")
        {
            reply.ok();
            return;
        }
        for (index, entry) in entries.iter().enumerate() {
            let entry_offset = index as u64 + 3;
            if offset >= entry_offset {
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
        }
        reply.ok();
    }

    fn readdirplus(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
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
        let Ok((attr, entries)) = self.directory_entries(inode) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let parent = self.parent_of(inode);
        let parent_attr = self
            .service_get_attr(parent)
            .ok()
            .flatten()
            .unwrap_or_else(|| attr.clone());
        if self.add_dirent_plus(&mut reply, offset, 1, ".", &attr)
            || self.add_dirent_plus(&mut reply, offset, 2, "..", &parent_attr)
        {
            reply.ok();
            return;
        }
        for (index, entry) in entries.iter().enumerate() {
            let entry_offset = index as u64 + 3;
            if offset >= entry_offset {
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
        }
        reply.ok();
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
        match self
            .service
            .rename_replace(parent, &name, newparent, newname)
        {
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

        let (_attr, entries) = fuse.directory_entries(runs.attr.inode).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], old);
        assert_eq!(
            fuse.service_read_file(old.attr.inode, 0, old.attr.size as usize)
                .unwrap(),
            b"old"
        );
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
