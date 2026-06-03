//! In-process NoKV-FS metadata service.
//!
//! This crate owns the first Rust-native service semantics over the
//! storage-neutral metadata command contract. It compiles namespace operations
//! into `MetadataCommand`s and stores file bodies through an object-store
//! boundary. It does not own Holt trees, Raft replication, FUSE, or protobuf.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::command::{
    CommandKind, CommitResult, MetadataCommand, MetadataError, MetadataStore, Mutation, MutationOp,
    Predicate, PredicateRef, ReadPurpose, ScanRequest, Value, Version,
};
use crate::layout::{
    allocator_key, chunk_manifest_key, chunk_manifest_prefix, decode_allocator_state,
    decode_body_descriptor, decode_chunk_manifest, decode_dentry_projection, decode_inode_attr,
    dentry_key, dentry_prefix, encode_allocator_state, encode_body_descriptor,
    encode_chunk_manifest, encode_dentry_projection, encode_inode_attr, inode_key,
};
use nokvfs_object::{
    delete_staged_objects, put_chunked_object, read_object_blocks, ChunkWriteOptions,
    MemoryBlockCache, ObjectCleanupOutcome, ObjectError, ObjectReadBlock, ObjectStore,
    StagedObjectSet, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokvfs_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, DentryProjection, DentryRecord,
    FileType, InodeAttr, InodeId, ModelError, MountId, RecordFamily,
};

const BODY_SUMMARY_CHUNK_INDEX: u64 = u64::MAX;

const ALLOCATOR_RECOVERY_FAMILIES: [RecordFamily; 11] = [
    RecordFamily::System,
    RecordFamily::Mount,
    RecordFamily::Inode,
    RecordFamily::Dentry,
    RecordFamily::Parent,
    RecordFamily::ChunkManifest,
    RecordFamily::Session,
    RecordFamily::PathIndex,
    RecordFamily::Watch,
    RecordFamily::Snapshot,
    RecordFamily::CommandDedupe,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AllocatorState {
    last_commit_version: u64,
    next_inode: u64,
}

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
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub bytes: Vec<u8>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectTransferStats {
    pub object_puts: u64,
    pub object_gets: u64,
    pub cache_hits: u64,
    pub manifest_chunks: u64,
    pub manifest_blocks: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameReplaceResult {
    pub entry: DentryWithAttr,
    pub replaced: Option<DentryWithAttr>,
}

#[derive(Debug)]
pub enum MetadError {
    Model(ModelError),
    Metadata(MetadataError),
    Object(ObjectError),
    PublishArtifactFailed {
        source: Box<MetadError>,
        staged: StagedObjectSet,
    },
    Codec(String),
    BodySizeMismatch {
        descriptor: u64,
        bytes: u64,
    },
    AllocatorExhausted,
    NotFound,
    NotFile,
    NotDirectory,
    DirectoryNotEmpty,
    CannotRemoveRoot,
    MissingBodyDescriptor,
}

pub struct NoKvFs<M, O> {
    mount: MountId,
    metadata: M,
    objects: O,
    commit_gate: Mutex<()>,
    clock: AtomicU64,
    next_inode: AtomicU64,
    block_cache: MemoryBlockCache,
    block_cache_enabled: AtomicBool,
    object_puts: AtomicU64,
    object_gets: AtomicU64,
    cache_hits: AtomicU64,
    manifest_chunks: AtomicU64,
    manifest_blocks: AtomicU64,
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
            commit_gate: Mutex::new(()),
            clock: AtomicU64::new(1),
            next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            object_puts: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
        }
    }

    pub fn open_existing(mount: MountId, metadata: M, objects: O) -> Result<Self, MetadError> {
        let allocator = recover_allocator_state(&metadata, mount)?;
        Ok(Self {
            mount,
            metadata,
            objects,
            commit_gate: Mutex::new(()),
            clock: AtomicU64::new(allocator.last_commit_version),
            next_inode: AtomicU64::new(allocator.next_inode),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            object_puts: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
        })
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        ObjectTransferStats {
            object_puts: self.object_puts.load(Ordering::Relaxed),
            object_gets: self.object_gets.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            manifest_chunks: self.manifest_chunks.load(Ordering::Relaxed),
            manifest_blocks: self.manifest_blocks.load(Ordering::Relaxed),
        }
    }

    pub fn set_block_cache_enabled(&self, enabled: bool) {
        self.block_cache_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn block_cache_enabled(&self) -> bool {
        self.block_cache_enabled.load(Ordering::Relaxed)
    }

    pub fn cleanup_staged_objects(
        &self,
        staged: &StagedObjectSet,
    ) -> Result<ObjectCleanupOutcome, MetadError> {
        delete_staged_objects(&self.objects, staged).map_err(Into::into)
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
        match self.commit_metadata(command) {
            Ok(_) | Err(MetadError::Metadata(MetadataError::PredicateFailed)) => Ok(root),
            Err(err) => Err(err),
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

    pub fn create_file(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            size: 0,
            generation: version.get(),
            mtime_ms: version.get(),
            ctime_ms: version.get(),
        };
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateFile, &projection, version)?;
        Ok(projection.into())
    }

    pub fn publish_artifact(&self, request: PublishArtifact) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        self.reserve_allocator_state(version, self.next_inode.load(Ordering::Relaxed))?;
        let written = put_chunked_object(
            &self.objects,
            &request.bytes,
            ChunkWriteOptions {
                manifest_id: request.manifest_id,
                mount: self.mount.get(),
                inode: inode.get(),
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        )?;
        let staged = written.staged_objects()?;
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.manifest_chunks
            .fetch_add(written.chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            written
                .chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        let chunks: Vec<ChunkManifest> = written
            .chunks
            .into_iter()
            .map(|chunk| ChunkManifest {
                chunk_index: chunk.chunk_index,
                logical_offset: chunk.logical_offset,
                len: chunk.len,
                blocks: chunk
                    .blocks
                    .into_iter()
                    .map(|block| BlockDescriptor {
                        object_key: block.object_key,
                        logical_offset: block.logical_offset,
                        object_offset: block.object_offset,
                        len: block.len,
                        digest_uri: block.digest_uri,
                    })
                    .collect(),
            })
            .collect();
        let body = BodyDescriptor {
            producer: request.producer,
            digest_uri: request.digest_uri,
            size: written.size,
            content_type: request.content_type,
            manifest_id: written.manifest_id,
            generation: version.get(),
            chunk_size: written.chunk_size,
            block_size: written.block_size,
        };
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            size: body.size,
            generation: version.get(),
            mtime_ms: version.get(),
            ctime_ms: version.get(),
        };
        let projection = projection(request.parent, request.name, attr, Some(body));
        if let Err(err) = self.commit_create_projection_with_chunks(
            CommandKind::PublishArtifact,
            &projection,
            &chunks,
            version,
        ) {
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(err),
                staged,
            });
        }
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
        self.lookup_plus_versioned(parent, name)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    fn lookup_plus_versioned(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let version = self.read_version()?;
        let key = dentry_key(self.mount, parent, name);
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::Dentry,
            &key,
            version,
            ReadPurpose::UserStrong,
        )?
        else {
            return Ok(None);
        };
        Ok(Some((
            crate::layout::decode_dentry_projection(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?
                .into(),
            item.version,
        )))
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
                crate::layout::decode_dentry_projection(&item.value.0)
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
        self.read_file(entry.attr.inode, 0, body.size as usize)
    }

    pub fn body_descriptor(&self, inode: InodeId) -> Result<Option<BodyDescriptor>, MetadError> {
        let Some(attr) = self.get_attr(inode)? else {
            return Ok(None);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let version = self.read_version()?;
        let summary_key =
            chunk_manifest_key(self.mount, inode, attr.generation, BODY_SUMMARY_CHUNK_INDEX);
        let Some(value) = self.metadata.get(
            RecordFamily::ChunkManifest,
            &summary_key,
            version,
            ReadPurpose::UserStrong,
        )?
        else {
            return Err(MetadError::MissingBodyDescriptor);
        };
        decode_body_descriptor(&value.0)
            .map(Some)
            .map_err(|err| MetadError::Codec(err.to_string()))
    }

    pub fn read_file(
        &self,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let Some(attr) = self.get_attr(inode)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= attr.size {
            return Ok(Vec::new());
        }
        let body = self.body_descriptor(inode)?.ok_or(MetadError::NotFound)?;
        let len = len.min((body.size - offset) as usize);
        let plan = self.read_plan(inode, &body, offset, len)?;
        let cache = if self.block_cache_enabled() {
            Some(&self.block_cache)
        } else {
            None
        };
        let outcome = read_object_blocks(&self.objects, cache, len, &plan)?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    pub fn remove_file(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_versioned(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let version = self.next_version()?;
        let key = dentry_key(self.mount, parent, name);
        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, key.clone()),
            delete_mutation(RecordFamily::Inode, inode_key(self.mount, entry.attr.inode)),
        ];
        if let Some(body) = &entry.body {
            mutations
                .extend(self.chunk_manifest_delete_mutations(entry.attr.inode, body.generation)?);
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"remove-file", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveFile,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key,
                    predicate: Predicate::VersionEquals(dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, entry.attr.inode),
                    predicate: Predicate::Exists,
                },
            ],
            mutations,
            watch: Vec::new(),
        })?;
        Ok(entry)
    }

    pub fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_versioned(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        if entry.attr.inode == InodeId::root() {
            return Err(MetadError::CannotRemoveRoot);
        }
        let version = self.next_version()?;
        let source_key = dentry_key(self.mount, parent, name);
        let child_prefix = dentry_prefix(self.mount, entry.attr.inode);
        match self.commit_metadata(MetadataCommand {
            request_id: request_id(b"remove-empty-dir", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveEmptyDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: source_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: source_key.clone(),
                    predicate: Predicate::VersionEquals(dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: child_prefix,
                    predicate: Predicate::PrefixEmpty,
                },
            ],
            mutations: vec![
                delete_mutation(RecordFamily::Dentry, source_key),
                delete_mutation(RecordFamily::Inode, inode_key(self.mount, entry.attr.inode)),
            ],
            watch: Vec::new(),
        }) {
            Ok(_) => Ok(entry),
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                Err(MetadError::DirectoryNotEmpty)
            }
            Err(err) => Err(err),
        }
    }

    pub fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, false)
            .map(|outcome| outcome.entry)
    }

    pub fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, true)
    }

    fn rename_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
        replace: bool,
    ) -> Result<RenameReplaceResult, MetadError> {
        let (source, source_version) = self
            .lookup_plus_versioned(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if parent == new_parent && *name == new_name {
            return Ok(RenameReplaceResult {
                entry: source,
                replaced: None,
            });
        }
        let destination = self.lookup_plus_versioned(new_parent, &new_name)?;
        if !replace && destination.is_some() {
            return Err(MetadataError::PredicateFailed.into());
        }
        if replace {
            if source.attr.file_type != FileType::File {
                return Err(MetadError::NotFile);
            }
            if let Some((entry, _)) = &destination {
                if entry.attr.file_type != FileType::File {
                    return Err(MetadError::NotFile);
                }
            }
        }

        let version = self.next_version()?;
        let source_key = dentry_key(self.mount, parent, name);
        let destination_key = dentry_key(self.mount, new_parent, &new_name);
        let projection = projection(
            new_parent,
            new_name,
            source.attr.clone(),
            source.body.clone(),
        );
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, new_parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: source_key.clone(),
                predicate: Predicate::VersionEquals(source_version),
            },
        ];
        let replaced = if let Some((entry, destination_version)) = destination {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::VersionEquals(destination_version),
            });
            Some(entry)
        } else {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::NotExists,
            });
            None
        };

        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, source_key),
            Mutation {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(&projection))),
            },
        ];
        if let Some(replaced) = &replaced {
            mutations.push(delete_mutation(
                RecordFamily::Inode,
                inode_key(self.mount, replaced.attr.inode),
            ));
            if let Some(body) = &replaced.body {
                mutations.extend(
                    self.chunk_manifest_delete_mutations(replaced.attr.inode, body.generation)?,
                );
            }
        }

        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                if replace {
                    b"rename-replace"
                } else {
                    b"rename"
                },
                self.mount,
                source.attr.inode,
                version,
            ),
            kind: if replace {
                CommandKind::RenameReplace
            } else {
                CommandKind::Rename
            },
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: destination_key,
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        Ok(RenameReplaceResult {
            entry: projection.into(),
            replaced,
        })
    }

    fn commit_create_projection(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        version: Version,
    ) -> Result<(), MetadError> {
        self.commit_create_projection_with_chunks(kind, projection, &[], version)
    }

    fn commit_create_projection_with_chunks(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
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
                key: chunk_manifest_key(
                    self.mount,
                    inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            });
            for chunk in chunks {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(self.mount, inode, body.generation, chunk.chunk_index),
                    op: MutationOp::Put,
                    value: Some(Value(encode_chunk_manifest(chunk))),
                });
            }
        }
        self.commit_metadata(MetadataCommand {
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

    fn read_plan(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<Vec<ObjectReadBlock>, MetadError> {
        if body.chunk_size == 0 || body.block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout.into());
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or(ObjectError::InvalidRange)?
            .min(body.size);
        if end <= offset {
            return Ok(Vec::new());
        }

        let start_chunk = offset / body.chunk_size;
        let end_chunk = (end - 1) / body.chunk_size;
        let version = self.read_version()?;
        let mut plan = Vec::new();
        for chunk_index in start_chunk..=end_chunk {
            let key = chunk_manifest_key(self.mount, inode, body.generation, chunk_index);
            let Some(value) = self.metadata.get(
                RecordFamily::ChunkManifest,
                &key,
                version,
                ReadPurpose::UserStrong,
            )?
            else {
                return Err(MetadError::MissingBodyDescriptor);
            };
            let manifest = decode_chunk_manifest(&value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            for block in manifest.blocks {
                let block_start = block.logical_offset;
                let block_end = block_start
                    .checked_add(block.len)
                    .ok_or(ObjectError::InvalidRange)?;
                let overlap_start = block_start.max(offset);
                let overlap_end = block_end.min(end);
                if overlap_start >= overlap_end {
                    continue;
                }
                let object_offset = block
                    .object_offset
                    .checked_add(overlap_start - block_start)
                    .ok_or(ObjectError::InvalidRange)?;
                plan.push(ObjectReadBlock {
                    object_key: block.object_key,
                    object_offset,
                    len: (overlap_end - overlap_start) as usize,
                    output_offset: (overlap_start - offset) as usize,
                });
            }
        }
        Ok(plan)
    }

    fn chunk_manifest_delete_mutations(
        &self,
        inode: InodeId,
        generation: u64,
    ) -> Result<Vec<Mutation>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::ChunkManifest,
            prefix: chunk_manifest_prefix(self.mount, inode, generation),
            version: self.read_version()?,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        Ok(rows
            .into_iter()
            .map(|row| delete_mutation(RecordFamily::ChunkManifest, row.key))
            .collect())
    }

    fn commit_metadata(&self, mut command: MetadataCommand) -> Result<CommitResult, MetadError> {
        let _guard = self.commit_gate.lock().map_err(|err| {
            MetadataError::Backend(format!("metadata commit gate poisoned: {err}"))
        })?;
        command.mutations.push(Mutation {
            family: RecordFamily::System,
            key: allocator_key(self.mount),
            op: MutationOp::Put,
            value: Some(Value(encode_allocator_state(
                command.commit_version.get(),
                self.next_inode.load(Ordering::Relaxed),
            ))),
        });
        self.metadata.commit_metadata(command).map_err(Into::into)
    }

    fn reserve_allocator_state(
        &self,
        version: Version,
        next_inode: u64,
    ) -> Result<CommitResult, MetadError> {
        let _guard = self.commit_gate.lock().map_err(|err| {
            MetadataError::Backend(format!("metadata commit gate poisoned: {err}"))
        })?;
        let key = allocator_key(self.mount);
        self.metadata
            .commit_metadata(MetadataCommand {
                request_id: request_id(b"reserve-allocator", self.mount, InodeId::root(), version),
                kind: CommandKind::ReserveAllocator,
                read_version: predecessor(version)?,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: key.clone(),
                predicates: Vec::new(),
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_allocator_state(version.get(), next_inode))),
                }],
                watch: Vec::new(),
            })
            .map_err(Into::into)
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

fn recover_allocator_state<M: MetadataStore>(
    metadata: &M,
    mount: MountId,
) -> Result<AllocatorState, MetadError> {
    let max_read = Version::new(u64::MAX)?;
    if let Some(value) = metadata.get(
        RecordFamily::System,
        &allocator_key(mount),
        max_read,
        ReadPurpose::UserStrong,
    )? {
        let (last_commit_version, next_inode) =
            decode_allocator_state(&value.0).map_err(|err| MetadError::Codec(err.to_string()))?;
        Version::new(last_commit_version)?;
        InodeId::new(next_inode)?;
        return Ok(AllocatorState {
            last_commit_version,
            next_inode,
        });
    }

    let mut last_commit_version = 1_u64;
    let mut max_inode = InodeId::ROOT_RAW;
    for family in ALLOCATOR_RECOVERY_FAMILIES {
        let rows = metadata.scan(ScanRequest {
            family,
            prefix: Vec::new(),
            version: max_read,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        for row in rows {
            last_commit_version = last_commit_version.max(row.version.get());
            match family {
                RecordFamily::Inode => {
                    let attr = decode_inode_attr(&row.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?;
                    last_commit_version = last_commit_version
                        .max(attr.generation)
                        .max(attr.mtime_ms)
                        .max(attr.ctime_ms);
                    max_inode = max_inode.max(attr.inode.get());
                }
                RecordFamily::Dentry => {
                    let projection = decode_dentry_projection(&row.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?;
                    last_commit_version = last_commit_version
                        .max(projection.attr.generation)
                        .max(projection.dentry.attr_generation);
                    max_inode = max_inode
                        .max(projection.attr.inode.get())
                        .max(projection.dentry.child.get());
                }
                _ => {}
            }
        }
    }

    let next_inode = max_inode
        .checked_add(1)
        .ok_or(MetadError::AllocatorExhausted)?;
    Ok(AllocatorState {
        last_commit_version,
        next_inode,
    })
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

fn delete_mutation(family: RecordFamily, key: Vec<u8>) -> Mutation {
    Mutation {
        family,
        key,
        op: MutationOp::Delete,
        value: None,
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
        CommandKind::ReserveAllocator => b"reserve-allocator",
        CommandKind::CreateFile => b"create-file",
        CommandKind::CreateDir => b"create-dir",
        CommandKind::Rename => b"rename",
        CommandKind::RenameReplace => b"rename-replace",
        CommandKind::RemoveFile => b"remove-file",
        CommandKind::RemoveEmptyDir => b"remove-empty-dir",
        CommandKind::PublishArtifact => b"publish-artifact",
        CommandKind::SnapshotSubtree => b"snapshot-subtree",
        CommandKind::WatchSubtree => b"watch-subtree",
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

impl MetadError {
    pub fn staged_objects(&self) -> Option<&StagedObjectSet> {
        match self {
            Self::PublishArtifactFailed { staged, .. } => Some(staged),
            Self::Object(ObjectError::StagedWriteFailed { staged, .. }) => Some(staged),
            _ => None,
        }
    }
}

impl fmt::Display for MetadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(err) => write!(f, "model error: {err}"),
            Self::Metadata(err) => write!(f, "metadata error: {err}"),
            Self::Object(err) => write!(f, "object error: {err}"),
            Self::PublishArtifactFailed { source, staged } => write!(
                f,
                "artifact publish failed after staging {} objects: {source}",
                staged.len()
            ),
            Self::Codec(err) => write!(f, "codec error: {err}"),
            Self::BodySizeMismatch { descriptor, bytes } => write!(
                f,
                "body descriptor size {descriptor} does not match uploaded bytes {bytes}"
            ),
            Self::AllocatorExhausted => write!(f, "inode allocator is exhausted"),
            Self::NotFound => write!(f, "metadata entry not found"),
            Self::NotFile => write!(f, "metadata entry is not a file"),
            Self::NotDirectory => write!(f, "metadata entry is not a directory"),
            Self::DirectoryNotEmpty => write!(f, "directory is not empty"),
            Self::CannotRemoveRoot => write!(f, "root directory cannot be removed"),
            Self::MissingBodyDescriptor => write!(f, "file is missing body descriptor"),
        }
    }
}

impl std::error::Error for MetadError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holtstore::HoltMetadataStore;
    use nokvfs_object::MemoryObjectStore;

    fn service() -> NoKvFs<HoltMetadataStore, MemoryObjectStore> {
        let objects = MemoryObjectStore::new();
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects,
        );
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        service
    }

    fn artifact_request(name: DentryName, manifest_id: &str, bytes: &[u8]) -> PublishArtifact {
        PublishArtifact {
            parent: InodeId::root(),
            name,
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: manifest_id.to_owned(),
            bytes: bytes.to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        }
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
    fn create_file_publishes_metadata_without_body_descriptor() {
        let service = service();
        let name = DentryName::new(b"empty.txt".to_vec()).unwrap();
        let created = service
            .create_file(InodeId::root(), name.clone(), 0o644, 1000, 1000)
            .unwrap();
        assert_eq!(created.attr.file_type, FileType::File);
        assert_eq!(created.attr.size, 0);
        assert!(created.body.is_none());
        assert_eq!(
            service.lookup_plus(InodeId::root(), &name).unwrap(),
            Some(created)
        );
    }

    #[test]
    fn publish_artifact_stores_body_then_publishes_metadata() {
        let service = service();
        let name = DentryName::new(b"checkpoint.json".to_vec()).unwrap();
        let published = service
            .publish_artifact(PublishArtifact {
                content_type: "application/json".to_owned(),
                ..artifact_request(name.clone(), "runs/1/checkpoint.json", b"{\"x\":1}")
            })
            .unwrap();

        let lookup = service
            .lookup_plus(InodeId::root(), &name)
            .unwrap()
            .unwrap();
        assert_eq!(lookup, published);
        assert_eq!(lookup.attr.size, 7);
        assert_eq!(
            lookup.body.as_ref().unwrap().manifest_id,
            "runs/1/checkpoint.json"
        );

        let bytes = service
            .read_artifact(InodeId::root(), &name)
            .expect("read artifact body");
        assert_eq!(bytes, b"{\"x\":1}");

        let body = service
            .body_descriptor(published.attr.inode)
            .expect("read body descriptor")
            .expect("body descriptor exists");
        assert_eq!(body.manifest_id, "runs/1/checkpoint.json");
        assert_eq!(body.generation, published.attr.generation);
        let range = service
            .read_file(published.attr.inode, 2, 3)
            .expect("read file range");
        assert_eq!(range, b"x\":");
        let before_cache = service.object_stats();
        let cached = service
            .read_file(published.attr.inode, 2, 3)
            .expect("read cached file range");
        assert_eq!(cached, b"x\":");
        assert!(service.object_stats().cache_hits > before_cache.cache_hits);
    }

    #[test]
    fn get_attr_reads_root_inode() {
        let service = service();
        let root = service.get_attr(InodeId::root()).unwrap().unwrap();
        assert_eq!(root.inode, InodeId::root());
        assert_eq!(root.file_type, FileType::Directory);
    }

    #[test]
    fn remove_file_deletes_namespace_and_returns_old_body() {
        let service = service();
        let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
        let published = service
            .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
            .unwrap();

        let removed = service.remove_file(InodeId::root(), &name).unwrap();
        assert_eq!(removed, published);
        assert_eq!(removed.body.as_ref().unwrap().manifest_id, "artifact.bin");
        assert!(service
            .lookup_plus(InodeId::root(), &name)
            .unwrap()
            .is_none());
        assert!(service.get_attr(removed.attr.inode).unwrap().is_none());
    }

    #[test]
    fn remove_empty_dir_rejects_non_empty_directory() {
        let service = service();
        let dir = DentryName::new(b"runs".to_vec()).unwrap();
        let child = DentryName::new(b"1".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
            .unwrap();
        service
            .create_dir(created.attr.inode, child, 0o755, 1000, 1000)
            .unwrap();

        let err = service.remove_empty_dir(InodeId::root(), &dir).unwrap_err();
        assert!(matches!(err, MetadError::DirectoryNotEmpty));
        assert!(service
            .lookup_plus(InodeId::root(), &dir)
            .unwrap()
            .is_some());
    }

    #[test]
    fn remove_empty_dir_deletes_empty_directory() {
        let service = service();
        let dir = DentryName::new(b"runs".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
            .unwrap();

        let removed = service.remove_empty_dir(InodeId::root(), &dir).unwrap();
        assert_eq!(removed, created);
        assert!(service
            .lookup_plus(InodeId::root(), &dir)
            .unwrap()
            .is_none());
        assert!(service.get_attr(created.attr.inode).unwrap().is_none());
    }

    #[test]
    fn rename_moves_dentry_without_changing_inode() {
        let service = service();
        let old_name = DentryName::new(b"old".to_vec()).unwrap();
        let new_name = DentryName::new(b"new".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), old_name.clone(), 0o755, 1000, 1000)
            .unwrap();

        let renamed = service
            .rename(
                InodeId::root(),
                &old_name,
                InodeId::root(),
                new_name.clone(),
            )
            .unwrap();
        assert_eq!(renamed.attr.inode, created.attr.inode);
        assert!(service
            .lookup_plus(InodeId::root(), &old_name)
            .unwrap()
            .is_none());
        assert_eq!(
            service.lookup_plus(InodeId::root(), &new_name).unwrap(),
            Some(renamed)
        );
    }

    #[test]
    fn rename_replace_returns_replaced_file_body() {
        let service = service();
        let source_name = DentryName::new(b"stage".to_vec()).unwrap();
        let final_name = DentryName::new(b"final".to_vec()).unwrap();
        let source = service
            .publish_artifact(artifact_request(source_name.clone(), "stage", b"new"))
            .unwrap();
        let old = service
            .publish_artifact(artifact_request(final_name.clone(), "final-old", b"old"))
            .unwrap();

        let result = service
            .rename_replace(
                InodeId::root(),
                &source_name,
                InodeId::root(),
                final_name.clone(),
            )
            .unwrap();
        assert_eq!(result.entry.attr.inode, source.attr.inode);
        assert_eq!(result.replaced, Some(old.clone()));
        assert!(service
            .lookup_plus(InodeId::root(), &source_name)
            .unwrap()
            .is_none());
        assert_eq!(
            service.lookup_plus(InodeId::root(), &final_name).unwrap(),
            Some(result.entry)
        );
        assert!(service.get_attr(old.attr.inode).unwrap().is_none());
    }

    #[test]
    fn open_existing_recovers_inode_and_version_allocators() {
        let dir = tempfile::tempdir().unwrap();
        let objects = MemoryObjectStore::new();
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

    #[test]
    fn open_existing_recovers_after_dentry_only_rename() {
        let dir = tempfile::tempdir().unwrap();
        let objects = MemoryObjectStore::new();
        let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let old_name = DentryName::new(b"old".to_vec()).unwrap();
        let new_name = DentryName::new(b"new".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), old_name.clone(), 0o755, 1000, 1000)
            .unwrap();
        let renamed = service
            .rename(
                InodeId::root(),
                &old_name,
                InodeId::root(),
                new_name.clone(),
            )
            .unwrap();
        assert_eq!(renamed.attr.inode, created.attr.inode);
        drop(service);

        let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
        assert!(reopened
            .lookup_plus(InodeId::root(), &old_name)
            .unwrap()
            .is_none());
        assert_eq!(
            reopened.lookup_plus(InodeId::root(), &new_name).unwrap(),
            Some(renamed)
        );
        assert_eq!(reopened.read_dir_plus(InodeId::root()).unwrap().len(), 1);
    }

    #[test]
    fn open_existing_does_not_reuse_removed_inode() {
        let dir = tempfile::tempdir().unwrap();
        let objects = MemoryObjectStore::new();
        let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let first_name = DentryName::new(b"first".to_vec()).unwrap();
        let second_name = DentryName::new(b"second".to_vec()).unwrap();
        let first = service
            .create_file(InodeId::root(), first_name.clone(), 0o644, 1000, 1000)
            .unwrap();
        service.remove_file(InodeId::root(), &first_name).unwrap();
        drop(service);

        let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
        let second = reopened
            .create_file(InodeId::root(), second_name.clone(), 0o644, 1000, 1000)
            .unwrap();
        assert!(second.attr.inode > first.attr.inode);
        assert!(second.attr.generation > first.attr.generation);
        assert!(reopened
            .lookup_plus(InodeId::root(), &first_name)
            .unwrap()
            .is_none());
        assert_eq!(
            reopened.lookup_plus(InodeId::root(), &second_name).unwrap(),
            Some(second)
        );
    }

    #[test]
    fn failed_publish_returns_staged_objects_for_cleanup_and_does_not_reuse_identity() {
        let dir = tempfile::tempdir().unwrap();
        let objects = MemoryObjectStore::new();
        let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
        let first = service
            .publish_artifact(artifact_request(name.clone(), "first", b"first"))
            .unwrap();
        let err = service
            .publish_artifact(artifact_request(name.clone(), "duplicate", b"duplicate"))
            .unwrap_err();
        let staged = match err {
            MetadError::PublishArtifactFailed { source, staged } => {
                assert!(matches!(
                    *source,
                    MetadError::Metadata(MetadataError::PredicateFailed)
                ));
                staged
            }
            err => panic!("unexpected publish error: {err:?}"),
        };
        assert_eq!(staged.len(), 1);
        for object in staged.objects() {
            assert!(objects.head(&object.key).unwrap().is_some());
        }
        assert_eq!(
            service.lookup_plus(InodeId::root(), &name).unwrap(),
            Some(first.clone())
        );

        let cleanup = service.cleanup_staged_objects(&staged).unwrap();
        assert_eq!(cleanup.attempted, staged.len());
        assert_eq!(cleanup.deleted, staged.len());
        assert_eq!(cleanup.missing, 0);
        for object in staged.objects() {
            assert!(objects.head(&object.key).unwrap().is_none());
        }
        drop(service);

        let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
        let next_name = DentryName::new(b"next.bin".to_vec()).unwrap();
        let next = reopened
            .publish_artifact(artifact_request(next_name, "next", b"next"))
            .unwrap();

        assert!(next.attr.inode.get() > first.attr.inode.get() + 1);
        assert!(next.attr.generation > first.attr.generation + 1);
    }
}
