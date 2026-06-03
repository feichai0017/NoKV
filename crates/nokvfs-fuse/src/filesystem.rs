use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use fuser::{
    Config, Errno, FileHandle, FileType as FuseFileType, Filesystem, FopenFlags, Generation,
    INodeNo, MountOption, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
    WriteFlags,
};
use nokvfs_meta::command::MetadataStore;
use nokvfs_meta::{DentryWithAttr, MetadError, NoKvFs, PublishArtifact};
use nokvfs_object::ObjectStore;
use nokvfs_types::{DentryName, FileType, InodeAttr, InodeId};

use crate::attr::{file_attr, fuse_file_type};

#[derive(Clone, Debug)]
pub struct FuseOptions {
    pub entry_ttl: Duration,
    pub attr_ttl: Duration,
    pub fs_name: String,
    pub threads: usize,
}

pub struct NoKvFuse<M, O> {
    service: Arc<NoKvFs<M, O>>,
    options: FuseOptions,
    parents: RwLock<HashMap<u64, u64>>,
    names: RwLock<HashMap<u64, Vec<u8>>>,
    next_handle: AtomicU64,
    write_handles: RwLock<HashMap<u64, WriteHandle>>,
}

#[derive(Clone, Debug)]
struct WriteHandle {
    inode: InodeId,
    parent: InodeId,
    name: DentryName,
    mode: u32,
    uid: u32,
    gid: u32,
    bytes: Vec<u8>,
    dirty: bool,
}

impl Default for FuseOptions {
    fn default() -> Self {
        Self {
            entry_ttl: Duration::from_secs(1),
            attr_ttl: Duration::from_secs(1),
            fs_name: "nokv-fs".to_owned(),
            threads: default_threads(),
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
        let mut parents = HashMap::new();
        parents.insert(InodeId::ROOT_RAW, InodeId::ROOT_RAW);
        Self {
            service: Arc::new(service),
            options,
            parents: RwLock::new(parents),
            names: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            write_handles: RwLock::new(HashMap::new()),
        }
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
    }

    fn parent_of(&self, inode: InodeId) -> InodeId {
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

    fn directory_entries(&self, inode: InodeId) -> Result<(InodeAttr, Vec<DentryWithAttr>), Errno> {
        let attr = self
            .service
            .get_attr(inode)
            .map_err(errno)?
            .ok_or(Errno::ENOENT)?;
        if attr.file_type != FileType::Directory {
            return Err(Errno::ENOTDIR);
        }
        let entries = self.service.read_dir_plus(inode).map_err(errno)?;
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
        let bytes = if attr.size == 0 {
            Vec::new()
        } else {
            self.service
                .read_file(attr.inode, 0, attr.size as usize)
                .map_err(errno)?
        };
        self.allocate_handle(WriteHandle {
            inode: attr.inode,
            parent,
            name,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            bytes,
            dirty: false,
        })
    }

    fn write_to_handle(&self, fh: FileHandle, offset: u64, data: &[u8]) -> Result<usize, Errno> {
        let offset = usize::try_from(offset).map_err(|_| Errno::EINVAL)?;
        let end = offset.checked_add(data.len()).ok_or(Errno::EINVAL)?;
        let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
        let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
        if end > handle.bytes.len() {
            handle.bytes.resize(end, 0);
        }
        handle.bytes[offset..end].copy_from_slice(data);
        handle.dirty = true;
        Ok(data.len())
    }

    fn read_from_handle(
        &self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Option<Vec<u8>>, Errno> {
        let handles = self.write_handles.read().map_err(|_| Errno::EIO)?;
        let Some(handle) = handles.get(&fh.0) else {
            return Ok(None);
        };
        let offset = usize::try_from(offset).map_err(|_| Errno::EINVAL)?;
        if offset >= handle.bytes.len() {
            return Ok(Some(Vec::new()));
        }
        let end = offset
            .checked_add(size as usize)
            .ok_or(Errno::EINVAL)?
            .min(handle.bytes.len());
        Ok(Some(handle.bytes[offset..end].to_vec()))
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
            .replace_artifact(PublishArtifact {
                parent: snapshot.parent,
                name: snapshot.name,
                producer: "nokv-fuse".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: fuse_manifest_id(snapshot.parent, snapshot.inode),
                bytes: snapshot.bytes.clone(),
                mode: snapshot.mode,
                uid: snapshot.uid,
                gid: snapshot.gid,
            })
            .map_err(errno)?;
        if let Some(handle) = self
            .write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .get_mut(&fh.0)
        {
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
    let mut config = Config::default();
    let mut mount_options = vec![MountOption::FSName(options.fs_name.clone())];
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
    fuser::mount2(NoKvFuse::new(service, options), mountpoint, &config)
}

impl<M, O> Filesystem for NoKvFuse<M, O>
where
    M: MetadataStore + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = match inode_id(parent) {
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
        match self.service.lookup_plus(parent, &name) {
            Ok(Some(entry)) => {
                self.remember_parent(entry.attr.inode, parent);
                reply.entry(
                    &self.options.entry_ttl,
                    &file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match inode_id(ino).and_then(|inode| {
            self.service
                .get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(attr) => reply.attr(&self.options.attr_ttl, &file_attr(&attr)),
            Err(err) => reply.error(err),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        match inode_id(ino).and_then(|inode| {
            self.service
                .get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(attr) if attr.file_type == FileType::File => match flags.acc_mode() {
                OpenAccMode::O_RDONLY => reply.opened(FileHandle(0), FopenFlags::FOPEN_KEEP_CACHE),
                OpenAccMode::O_WRONLY | OpenAccMode::O_RDWR => {
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
        match inode_id(ino).and_then(|inode| {
            self.service
                .read_file(inode, offset, size as usize)
                .map_err(errno)
        }) {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(err),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match inode_id(ino).and_then(|inode| self.directory_entries(inode).map(|_| ())) {
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
        let inode = match inode_id(ino) {
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
        if add_dirent(&mut reply, offset, 1, inode, FuseFileType::Directory, ".")
            || add_dirent(&mut reply, offset, 2, parent, FuseFileType::Directory, "..")
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
                INodeNo(entry.attr.inode.get()),
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
        let inode = match inode_id(ino) {
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
            .service
            .get_attr(parent)
            .ok()
            .flatten()
            .unwrap_or_else(|| attr.clone());
        if add_dirent_plus(&mut reply, &self.options, offset, 1, ".", &attr)
            || add_dirent_plus(&mut reply, &self.options, offset, 2, "..", &parent_attr)
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
                INodeNo(entry.attr.inode.get()),
                entry_offset,
                OsStr::from_bytes(entry.dentry.name.as_bytes()),
                &self.options.entry_ttl,
                &file_attr(&entry.attr),
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
        let parent = match inode_id(parent) {
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
                    &file_attr(&entry.attr),
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
        let parent = match inode_id(parent) {
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
            Ok(entry) => match self.allocate_handle(WriteHandle {
                inode: entry.attr.inode,
                parent,
                name: entry.dentry.name.clone(),
                mode: entry.attr.mode,
                uid: entry.attr.uid,
                gid: entry.attr.gid,
                bytes: Vec::new(),
                dirty: false,
            }) {
                Ok(handle) => {
                    self.remember_entry(&entry);
                    reply.created(
                        &self.options.entry_ttl,
                        &file_attr(&entry.attr),
                        Generation(entry.attr.generation),
                        handle,
                        FopenFlags::empty(),
                    );
                }
                Err(err) => reply.error(err),
            },
            Err(err) => reply.error(errno(err)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent = match inode_id(parent) {
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
        let parent = match inode_id(parent) {
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
        let parent = match inode_id(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let newparent = match inode_id(newparent) {
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

fn fuse_manifest_id(parent: InodeId, inode: InodeId) -> String {
    format!("fuse/{}/{}", parent.get(), inode.get())
}

fn add_dirent(
    reply: &mut ReplyDirectory,
    request_offset: u64,
    entry_offset: u64,
    inode: InodeId,
    kind: FuseFileType,
    name: &str,
) -> bool {
    request_offset < entry_offset && reply.add(INodeNo(inode.get()), entry_offset, kind, name)
}

fn add_dirent_plus(
    reply: &mut ReplyDirectoryPlus,
    options: &FuseOptions,
    request_offset: u64,
    entry_offset: u64,
    name: &str,
    attr: &InodeAttr,
) -> bool {
    request_offset < entry_offset
        && reply.add(
            INodeNo(attr.inode.get()),
            entry_offset,
            name,
            &options.entry_ttl,
            &file_attr(attr),
            Generation(attr.generation),
        )
}

fn errno(err: MetadError) -> Errno {
    match err {
        MetadError::Model(_) => Errno::EINVAL,
        MetadError::NotFound => Errno::ENOENT,
        MetadError::NotFile => Errno::EISDIR,
        MetadError::NotDirectory => Errno::ENOTDIR,
        MetadError::DirectoryNotEmpty => Errno::ENOTEMPTY,
        MetadError::CannotRemoveRoot => Errno::EBUSY,
        MetadError::MissingBodyDescriptor
        | MetadError::Metadata(_)
        | MetadError::Object(_)
        | MetadError::PublishArtifactFailed { .. }
        | MetadError::Codec(_)
        | MetadError::BodySizeMismatch { .. }
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
}
