//! In-process NoKV-FS metadata service.
//!
//! This crate owns the first Rust-native service semantics over the
//! storage-neutral metadata command contract. It compiles namespace operations
//! into `MetadataCommand`s and stores file bodies through an object-store
//! boundary. It does not own Holt trees, Raft replication, FUSE, or protobuf.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use nokv_fs_layout::{
    chunk_manifest_key, decode_inode_attr, dentry_key, dentry_prefix, encode_body_descriptor,
    encode_dentry_projection, encode_inode_attr, inode_key, inode_prefix,
};
use nokv_fs_metastore::{
    CommandKind, MetadataCommand, MetadataError, MetadataStore, Mutation, MutationOp, Predicate,
    PredicateRef, ReadPurpose, ScanRequest, Value, Version,
};
use nokv_fs_model::{
    BodyDescriptor, DentryName, DentryProjection, DentryRecord, FileType, InodeAttr, InodeId,
    ModelError, MountId, RecordFamily,
};
use nokv_fs_object::{ObjectError, ObjectKey, ObjectStore};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryWithAttr {
    pub dentry: DentryRecord,
    pub attr: InodeAttr,
    pub body: Option<BodyDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifact {
    pub parent: InodeId,
    pub name: DentryName,
    pub body: BodyDescriptor,
    pub bytes: Vec<u8>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug)]
pub enum MetadError {
    Model(ModelError),
    Metadata(MetadataError),
    Object(ObjectError),
    Codec(String),
    BodySizeMismatch { descriptor: u64, bytes: u64 },
    AllocatorExhausted,
    NotFound,
    NotFile,
    MissingBodyDescriptor,
}

pub struct NoKvFs<M, O> {
    mount: MountId,
    metadata: M,
    objects: O,
    clock: AtomicU64,
    next_inode: AtomicU64,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn new(mount: MountId, metadata: M, objects: O) -> Self {
        Self {
            mount,
            metadata,
            objects,
            clock: AtomicU64::new(1),
            next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
        }
    }

    pub fn open_existing(mount: MountId, metadata: M, objects: O) -> Result<Self, MetadError> {
        let mut max_version = 1_u64;
        let mut max_inode = InodeId::ROOT_RAW;
        let rows = metadata.scan(ScanRequest {
            family: RecordFamily::Inode,
            prefix: inode_prefix(mount),
            version: Version::new(u64::MAX)?,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        for row in rows {
            let attr = decode_inode_attr(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            max_version = max_version
                .max(row.version.get())
                .max(attr.generation)
                .max(attr.mtime_ms)
                .max(attr.ctime_ms);
            max_inode = max_inode.max(attr.inode.get());
        }
        let next_inode = max_inode
            .checked_add(1)
            .ok_or(MetadError::AllocatorExhausted)?;
        Ok(Self {
            mount,
            metadata,
            objects,
            clock: AtomicU64::new(max_version),
            next_inode: AtomicU64::new(next_inode),
        })
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<InodeAttr, MetadError> {
        let version = self.next_version()?;
        let root = directory_attr(InodeId::root(), mode, uid, gid, version.get());
        let command = MetadataCommand {
            request_id: request_id(b"bootstrap-root", self.mount, InodeId::root(), version),
            kind: CommandKind::CreateDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: inode_key(self.mount, InodeId::root()),
            predicates: vec![PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, InodeId::root()),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, InodeId::root()),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&root))),
            }],
            watch: Vec::new(),
        };
        match self.metadata.commit_metadata(command) {
            Ok(_) | Err(MetadataError::PredicateFailed) => Ok(root),
            Err(err) => Err(err.into()),
        }
    }

    pub fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let attr = directory_attr(inode, mode, uid, gid, version.get());
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateDir, &projection, version)?;
        Ok(projection.into())
    }

    pub fn publish_artifact(&self, request: PublishArtifact) -> Result<DentryWithAttr, MetadError> {
        if request.body.size != request.bytes.len() as u64 {
            return Err(MetadError::BodySizeMismatch {
                descriptor: request.body.size,
                bytes: request.bytes.len() as u64,
            });
        }
        let object_key = ObjectKey::new(request.body.object_ref.clone())?;
        self.objects.put(&object_key, &request.bytes)?;

        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            size: request.body.size,
            generation: version.get(),
            mtime_ms: version.get(),
            ctime_ms: version.get(),
        };
        let projection = projection(request.parent, request.name, attr, Some(request.body));
        self.commit_create_projection(CommandKind::PublishArtifact, &projection, version)?;
        Ok(projection.into())
    }

    pub fn get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, MetadError> {
        let version = self.read_version()?;
        let Some(value) = self.metadata.get(
            RecordFamily::Inode,
            &inode_key(self.mount, inode),
            version,
            ReadPurpose::UserStrong,
        )?
        else {
            return Ok(None);
        };
        decode_inode_attr(&value.0)
            .map(Some)
            .map_err(|err| MetadError::Codec(err.to_string()))
    }

    pub fn lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let version = self.read_version()?;
        let key = dentry_key(self.mount, parent, name);
        let Some(value) =
            self.metadata
                .get(RecordFamily::Dentry, &key, version, ReadPurpose::UserStrong)?
        else {
            return Ok(None);
        };
        Ok(Some(
            nokv_fs_layout::decode_dentry_projection(&value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?
                .into(),
        ))
    }

    pub fn read_dir_plus(&self, parent: InodeId) -> Result<Vec<DentryWithAttr>, MetadError> {
        let version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(self.mount, parent),
            version,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        rows.into_iter()
            .map(|item| {
                nokv_fs_layout::decode_dentry_projection(&item.value.0)
                    .map(Into::into)
                    .map_err(|err| MetadError::Codec(err.to_string()))
            })
            .collect()
    }

    pub fn read_artifact(&self, parent: InodeId, name: &DentryName) -> Result<Vec<u8>, MetadError> {
        let entry = self
            .lookup_plus(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        let key = ObjectKey::new(body.object_ref)?;
        self.objects.get(&key, None).map_err(Into::into)
    }

    fn commit_create_projection(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        version: Version,
    ) -> Result<(), MetadError> {
        let inode = projection.attr.inode;
        let dentry = dentry_key(
            self.mount,
            projection.dentry.parent,
            &projection.dentry.name,
        );
        let mut mutations = vec![
            Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&projection.attr))),
            },
            Mutation {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(projection))),
            },
        ];
        if let Some(body) = &projection.body {
            mutations.push(Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(self.mount, inode, body.generation, 0),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            });
        }
        self.metadata.commit_metadata(MetadataCommand {
            request_id: request_id(kind_name(kind), self.mount, inode, version),
            kind,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, projection.dentry.parent),
                    predicate: Predicate::Exists,
                },
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry,
                    predicate: Predicate::NotExists,
                },
            ],
            mutations,
            watch: Vec::new(),
        })?;
        Ok(())
    }

    fn next_version(&self) -> Result<Version, MetadError> {
        Version::new(self.clock.fetch_add(1, Ordering::Relaxed) + 1).map_err(Into::into)
    }

    fn read_version(&self) -> Result<Version, MetadError> {
        Version::new(self.clock.load(Ordering::Relaxed)).map_err(Into::into)
    }

    fn next_inode(&self) -> Result<InodeId, MetadError> {
        InodeId::new(self.next_inode.fetch_add(1, Ordering::Relaxed)).map_err(Into::into)
    }
}

fn projection(
    parent: InodeId,
    name: DentryName,
    attr: InodeAttr,
    body: Option<BodyDescriptor>,
) -> DentryProjection {
    DentryProjection {
        dentry: DentryRecord {
            parent,
            name,
            child: attr.inode,
            child_type: attr.file_type,
            attr_generation: attr.generation,
        },
        attr,
        body,
    }
}

fn directory_attr(inode: InodeId, mode: u32, uid: u32, gid: u32, version: u64) -> InodeAttr {
    InodeAttr {
        inode,
        file_type: FileType::Directory,
        mode,
        uid,
        gid,
        size: 0,
        generation: version,
        mtime_ms: version,
        ctime_ms: version,
    }
}

fn predecessor(version: Version) -> Result<Version, MetadataError> {
    Version::new(version.get().saturating_sub(1))
}

fn request_id(prefix: &[u8], mount: MountId, inode: InodeId, version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 24);
    out.extend_from_slice(prefix);
    out.extend_from_slice(&mount.get().to_be_bytes());
    out.extend_from_slice(&inode.get().to_be_bytes());
    out.extend_from_slice(&version.get().to_be_bytes());
    out
}

fn kind_name(kind: CommandKind) -> &'static [u8] {
    match kind {
        CommandKind::CreateDir => b"create-dir",
        CommandKind::PublishArtifact => b"publish-artifact",
        _ => b"metadata-command",
    }
}

impl From<DentryProjection> for DentryWithAttr {
    fn from(projection: DentryProjection) -> Self {
        Self {
            dentry: projection.dentry,
            attr: projection.attr,
            body: projection.body,
        }
    }
}

impl From<MetadataError> for MetadError {
    fn from(err: MetadataError) -> Self {
        Self::Metadata(err)
    }
}

impl From<ModelError> for MetadError {
    fn from(err: ModelError) -> Self {
        Self::Model(err)
    }
}

impl From<ObjectError> for MetadError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for MetadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(err) => write!(f, "model error: {err}"),
            Self::Metadata(err) => write!(f, "metadata error: {err}"),
            Self::Object(err) => write!(f, "object error: {err}"),
            Self::Codec(err) => write!(f, "codec error: {err}"),
            Self::BodySizeMismatch { descriptor, bytes } => write!(
                f,
                "body descriptor size {descriptor} does not match uploaded bytes {bytes}"
            ),
            Self::AllocatorExhausted => write!(f, "inode allocator is exhausted"),
            Self::NotFound => write!(f, "metadata entry not found"),
            Self::NotFile => write!(f, "metadata entry is not a file"),
            Self::MissingBodyDescriptor => write!(f, "file is missing body descriptor"),
        }
    }
}

impl std::error::Error for MetadError {}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_fs_holtstore::HoltMetadataStore;
    use nokv_fs_object::LocalObjectStore;

    fn service() -> NoKvFs<HoltMetadataStore, LocalObjectStore> {
        let dir = tempfile::tempdir().unwrap();
        let objects = LocalObjectStore::new(dir.path().join("objects")).unwrap();
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects,
        );
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        service
    }

    #[test]
    fn create_dir_then_lookup_and_readdir_use_dentry_projection() {
        let service = service();
        let name = DentryName::new(b"runs".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
            .unwrap();

        let lookup = service
            .lookup_plus(InodeId::root(), &name)
            .unwrap()
            .unwrap();
        assert_eq!(lookup, created);

        let entries = service.read_dir_plus(InodeId::root()).unwrap();
        assert_eq!(entries, vec![created]);
    }

    #[test]
    fn publish_artifact_stores_body_then_publishes_metadata() {
        let service = service();
        let name = DentryName::new(b"checkpoint.json".to_vec()).unwrap();
        let published = service
            .publish_artifact(PublishArtifact {
                parent: InodeId::root(),
                name: name.clone(),
                body: BodyDescriptor {
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:test".to_owned(),
                    size: 7,
                    content_type: "application/json".to_owned(),
                    object_ref: "runs/1/checkpoint.json".to_owned(),
                    generation: 1,
                },
                bytes: b"{\"x\":1}".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();

        let lookup = service
            .lookup_plus(InodeId::root(), &name)
            .unwrap()
            .unwrap();
        assert_eq!(lookup, published);
        assert_eq!(lookup.attr.size, 7);
        assert_eq!(
            lookup.body.as_ref().unwrap().object_ref,
            "runs/1/checkpoint.json"
        );

        let bytes = service
            .read_artifact(InodeId::root(), &name)
            .expect("read artifact body");
        assert_eq!(bytes, b"{\"x\":1}");
    }

    #[test]
    fn get_attr_reads_root_inode() {
        let service = service();
        let root = service.get_attr(InodeId::root()).unwrap().unwrap();
        assert_eq!(root.inode, InodeId::root());
        assert_eq!(root.file_type, FileType::Directory);
    }

    #[test]
    fn open_existing_recovers_inode_and_version_allocators() {
        let dir = tempfile::tempdir().unwrap();
        let objects = LocalObjectStore::new(dir.path().join("objects")).unwrap();
        let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let first = service
            .create_dir(
                InodeId::root(),
                DentryName::new(b"first".to_vec()).unwrap(),
                0o755,
                1000,
                1000,
            )
            .unwrap();
        drop(service);

        let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
        let second = reopened
            .create_dir(
                InodeId::root(),
                DentryName::new(b"second".to_vec()).unwrap(),
                0o755,
                1000,
                1000,
            )
            .unwrap();
        assert!(second.attr.inode > first.attr.inode);
        assert!(second.attr.generation > first.attr.generation);
    }
}
