use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use fuser::{
    Config, Errno, FileHandle, FileType as FuseFileType, Filesystem, FopenFlags, Generation,
    INodeNo, MountOption, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, WriteFlags,
};
use nokvfs_meta::command::MetadataStore;
use nokvfs_meta::{DentryWithAttr, MetadError, NoKvFs};
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

    fn parent_of(&self, inode: InodeId) -> InodeId {
        let raw = self
            .parents
            .read()
            .ok()
            .and_then(|parents| parents.get(&inode.get()).copied())
            .unwrap_or(InodeId::ROOT_RAW);
        InodeId::new(raw).unwrap_or_else(|_| InodeId::root())
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
            self.remember_parent(entry.attr.inode, inode);
        }
        Ok((attr, entries))
    }
}

pub fn mount_read_only<M, O>(
    service: NoKvFs<M, O>,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<()>
where
    M: MetadataStore + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    let mut config = Config::default();
    let mut mount_options = vec![
        MountOption::FSName(options.fs_name.clone()),
        MountOption::RO,
    ];
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

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match inode_id(ino).and_then(|inode| {
            self.service
                .get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(attr) if attr.file_type == FileType::File => {
                reply.opened(FileHandle(0), FopenFlags::FOPEN_KEEP_CACHE);
            }
            Ok(_) => reply.error(Errno::EISDIR),
            Err(err) => reply.error(err),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
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
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
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
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(Errno::EROFS);
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        reply.error(Errno::EROFS);
    }
}

fn inode_id(ino: INodeNo) -> Result<InodeId, Errno> {
    InodeId::new(ino.0).map_err(|_| Errno::EINVAL)
}

fn dentry_name(name: &OsStr) -> Result<DentryName, Errno> {
    DentryName::new(name.as_bytes().to_vec()).map_err(|_| Errno::EINVAL)
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
    use nokvfs_types::{BodyDescriptor, MountId};

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
                body: BodyDescriptor {
                    producer: "fuse-test".to_owned(),
                    digest_uri: "sha256:test".to_owned(),
                    size: 10,
                    content_type: "application/octet-stream".to_owned(),
                    object_ref: "checkpoint".to_owned(),
                    generation: 1,
                },
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
}
