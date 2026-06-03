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
    Predicate, PredicateRef, ReadPurpose, ScanRequest, Value, Version, WatchProjection,
};
use crate::layout::{
    allocator_key, chunk_manifest_key, chunk_manifest_prefix, decode_allocator_state,
    decode_body_descriptor, decode_chunk_manifest, decode_dentry_projection, decode_inode_attr,
    decode_object_gc_record, decode_snapshot_pin, decode_watch_event, dentry_key, dentry_prefix,
    encode_allocator_state, encode_body_descriptor, encode_chunk_manifest,
    encode_dentry_projection, encode_inode_attr, encode_object_gc_record, encode_snapshot_pin,
    encode_watch_event, gc_object_key, gc_queue_prefix, inode_key, snapshot_pin_key,
    snapshot_pin_prefix, watch_log_prefix,
};
use nokvfs_object::{
    delete_staged_objects, put_chunked_object, read_object_blocks, ChunkWriteOptions,
    MemoryBlockCache, ObjectCleanupOutcome, ObjectError, ObjectKey, ObjectReadBlock, ObjectStore,
    StagedObjectSet, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokvfs_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, DentryProjection, DentryRecord,
    FileType, InodeAttr, InodeId, ModelError, MountId, ObjectGcRecord, RecordFamily, SnapshotPin,
    WatchCursor, WatchEvent, WatchEventKind, WatchRecord,
};

const BODY_SUMMARY_CHUNK_INDEX: u64 = u64::MAX;

const ALLOCATOR_RECOVERY_FAMILIES: [RecordFamily; 12] = [
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
    RecordFamily::Gc,
    RecordFamily::CommandDedupe,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AllocatorState {
    last_commit_version: u64,
    next_inode: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StagedArtifactBody {
    body: BodyDescriptor,
    chunks: Vec<ChunkManifest>,
    staged: StagedObjectSet,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PendingObjectCleanupOutcome {
    pub scanned: usize,
    pub blocked_by_snapshots: usize,
    pub attempted: usize,
    pub deleted: usize,
    pub missing: usize,
    pub records_removed: usize,
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

    pub fn cleanup_pending_objects(
        &self,
        limit: usize,
    ) -> Result<PendingObjectCleanupOutcome, MetadError> {
        let version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(self.mount),
            version,
            limit,
            purpose: ReadPurpose::UserStrong,
        })?;
        if rows.is_empty() {
            return Ok(PendingObjectCleanupOutcome::default());
        }
        if self.has_active_snapshot_pins()? {
            return Ok(PendingObjectCleanupOutcome {
                scanned: rows.len(),
                blocked_by_snapshots: rows.len(),
                ..PendingObjectCleanupOutcome::default()
            });
        }

        let mut outcome = PendingObjectCleanupOutcome {
            scanned: rows.len(),
            blocked_by_snapshots: 0,
            attempted: 0,
            deleted: 0,
            missing: 0,
            records_removed: 0,
        };
        let mut cleaned_keys = Vec::with_capacity(rows.len());
        for row in rows {
            let record = decode_object_gc_record(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            let key = ObjectKey::new(record.object_key)?;
            outcome.attempted += 1;
            if self.objects.delete(&key)? {
                outcome.deleted += 1;
            } else {
                outcome.missing += 1;
            }
            cleaned_keys.push(row.key);
        }

        let commit_version = self.next_version()?;
        let records_removed = cleaned_keys.len();
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"cleanup-objects",
                self.mount,
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Gc,
            primary_key: gc_queue_prefix(self.mount),
            predicates: Vec::new(),
            mutations: cleaned_keys
                .into_iter()
                .map(|key| delete_mutation(RecordFamily::Gc, key))
                .collect(),
            watch: Vec::new(),
        })?;
        outcome.records_removed = records_removed;
        Ok(outcome)
    }

    pub fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, MetadError> {
        let Some(attr) = self.get_attr(root)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        let created_version = self.next_version()?;
        let read_version = predecessor(created_version)?;
        let pin = SnapshotPin {
            snapshot_id: created_version.get(),
            root,
            read_version: read_version.get(),
            created_version: created_version.get(),
        };
        let key = snapshot_pin_key(self.mount, pin.snapshot_id);
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"snapshot-subtree", self.mount, root, created_version),
            kind: CommandKind::SnapshotSubtree,
            read_version,
            commit_version: created_version,
            primary_family: RecordFamily::Snapshot,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Snapshot,
                key: key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Snapshot,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_snapshot_pin(&pin))),
            }],
            watch: Vec::new(),
        })?;
        Ok(pin)
    }

    pub fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, MetadError> {
        let key = snapshot_pin_key(self.mount, snapshot_id);
        if self.snapshot_pin(snapshot_id)?.is_none() {
            return Ok(false);
        }
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"retire-snapshot", self.mount, InodeId::root(), version),
            kind: CommandKind::RetireSnapshot,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Snapshot,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Snapshot,
                key: key.clone(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![delete_mutation(RecordFamily::Snapshot, key)],
            watch: Vec::new(),
        })?;
        Ok(true)
    }

    pub fn snapshot_pin(&self, snapshot_id: u64) -> Result<Option<SnapshotPin>, MetadError> {
        let value = self.metadata.get(
            RecordFamily::Snapshot,
            &snapshot_pin_key(self.mount, snapshot_id),
            self.read_version()?,
            ReadPurpose::UserStrong,
        )?;
        value
            .map(|value| {
                decode_snapshot_pin(&value.0).map_err(|err| MetadError::Codec(err.to_string()))
            })
            .transpose()
    }

    pub fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Option<InodeAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.get_attr_at_version(inode, version)
    }

    pub fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.lookup_plus_at_version(parent, name, version)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.read_dir_plus_at_version(parent, version)
    }

    pub fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let version = self.snapshot_read_version(snapshot_id)?;
        let Some(attr) = self.get_attr_at_version(inode, version)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= attr.size {
            return Ok(Vec::new());
        }
        let body = self
            .body_descriptor_at_version(inode, attr.generation, version)?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(inode, &body, offset, len, version)
    }

    pub fn read_artifact_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Vec<u8>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        let entry = self
            .lookup_plus_at_version(parent, name, version)?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(entry.attr.inode, &body, 0, body.size as usize, version)
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
        let StagedArtifactBody {
            body,
            chunks,
            staged,
        } = self.stage_artifact_body(&request, inode, version)?;
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

    pub fn replace_artifact(
        &self,
        request: PublishArtifact,
    ) -> Result<RenameReplaceResult, MetadError> {
        let (existing, dentry_version) = self
            .lookup_plus_versioned(request.parent, &request.name)?
            .ok_or(MetadError::NotFound)?;
        if existing.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let version = self.next_version()?;
        self.reserve_allocator_state(version, self.next_inode.load(Ordering::Relaxed))?;
        let StagedArtifactBody {
            body,
            chunks,
            staged,
        } = self.stage_artifact_body(&request, existing.attr.inode, version)?;
        let attr = InodeAttr {
            inode: existing.attr.inode,
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
        let old_generation = existing.body.as_ref().map(|body| body.generation);
        if let Err(err) = self.commit_replace_projection_with_chunks(
            &projection,
            &chunks,
            dentry_version,
            old_generation,
            version,
        ) {
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(err),
                staged,
            });
        }
        Ok(RenameReplaceResult {
            entry: projection.into(),
            replaced: Some(existing),
        })
    }

    pub fn get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, MetadError> {
        let version = self.read_version()?;
        self.get_attr_at_version(inode, version)
    }

    fn get_attr_at_version(
        &self,
        inode: InodeId,
        version: Version,
    ) -> Result<Option<InodeAttr>, MetadError> {
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
        self.lookup_plus_at_version(parent, name, version)
    }

    fn lookup_plus_at_version(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
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
        self.read_dir_plus_at_version(parent, version)
    }

    fn read_dir_plus_at_version(
        &self,
        parent: InodeId,
        version: Version,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
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

    pub fn watch_subtree(&self, scope: InodeId) -> Result<WatchCursor, MetadError> {
        let Some(attr) = self.get_attr(scope)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        Ok(WatchCursor {
            version: self.read_version()?.get(),
            event_id: u64::MAX,
        })
    }

    pub fn replay_watch(
        &self,
        scope: InodeId,
        after: WatchCursor,
        limit: usize,
    ) -> Result<Vec<WatchRecord>, MetadError> {
        let version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Watch,
            prefix: watch_log_prefix(self.mount, scope),
            version,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        let mut out = Vec::new();
        for row in rows {
            let cursor = watch_cursor_from_key(&row.key)?;
            if cursor <= after {
                continue;
            }
            out.push(WatchRecord {
                cursor,
                event: decode_watch_event(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?,
            });
            if limit != 0 && out.len() >= limit {
                break;
            }
        }
        Ok(out)
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
        self.body_descriptor_at_version(inode, attr.generation, self.read_version()?)
    }

    fn body_descriptor_at_version(
        &self,
        inode: InodeId,
        generation: u64,
        version: Version,
    ) -> Result<Option<BodyDescriptor>, MetadError> {
        let summary_key =
            chunk_manifest_key(self.mount, inode, generation, BODY_SUMMARY_CHUNK_INDEX);
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
        self.read_file_at_version(inode, &body, offset, len, self.read_version()?)
    }

    fn read_file_at_version(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        if offset >= body.size {
            return Ok(Vec::new());
        }
        let len = len.min((body.size - offset) as usize);
        let plan = self.read_plan(inode, body, offset, len, version)?;
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
            mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                entry.attr.inode,
                body.generation,
                version,
            )?);
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
            watch: vec![self.watch_projection(
                parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(parent),
                    name: Some(name.clone()),
                    inode: entry.attr.inode,
                    version: version.get(),
                },
            )],
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
            watch: vec![self.watch_projection(
                parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(parent),
                    name: Some(name.clone()),
                    inode: entry.attr.inode,
                    version: version.get(),
                },
            )],
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
                mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                    replaced.attr.inode,
                    body.generation,
                    version,
                )?);
            }
        }
        let mut watch = Vec::new();
        if let Some(replaced) = &replaced {
            watch.push(self.watch_projection(
                new_parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(new_parent),
                    name: Some(projection.dentry.name.clone()),
                    inode: replaced.attr.inode,
                    version: version.get(),
                },
            ));
        }
        watch.push(self.watch_projection(
            parent,
            WatchEvent {
                kind: WatchEventKind::Remove,
                parent: Some(parent),
                name: Some(name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ));
        watch.push(self.watch_projection(
            new_parent,
            WatchEvent {
                kind: WatchEventKind::Rename,
                parent: Some(new_parent),
                name: Some(projection.dentry.name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ));

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
            watch,
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
            watch: vec![self.watch_projection(
                projection.dentry.parent,
                WatchEvent {
                    kind: create_watch_kind(kind),
                    parent: Some(projection.dentry.parent),
                    name: Some(projection.dentry.name.clone()),
                    inode,
                    version: version.get(),
                },
            )],
        })?;
        Ok(())
    }

    fn commit_replace_projection_with_chunks(
        &self,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
        dentry_version: Version,
        old_generation: Option<u64>,
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
            if let Some(old_generation) = old_generation {
                mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                    inode,
                    old_generation,
                    version,
                )?);
            }
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
            request_id: request_id(b"replace-artifact", self.mount, inode, version),
            kind: CommandKind::ReplaceArtifact,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry,
                    predicate: Predicate::VersionEquals(dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, inode),
                    predicate: Predicate::Exists,
                },
            ],
            mutations,
            watch: vec![self.watch_projection(
                projection.dentry.parent,
                WatchEvent {
                    kind: WatchEventKind::PublishArtifact,
                    parent: Some(projection.dentry.parent),
                    name: Some(projection.dentry.name.clone()),
                    inode,
                    version: version.get(),
                },
            )],
        })?;
        Ok(())
    }

    fn stage_artifact_body(
        &self,
        request: &PublishArtifact,
        inode: InodeId,
        version: Version,
    ) -> Result<StagedArtifactBody, MetadError> {
        let written = put_chunked_object(
            &self.objects,
            &request.bytes,
            ChunkWriteOptions {
                manifest_id: request.manifest_id.clone(),
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
        let chunks = written
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
        Ok(StagedArtifactBody {
            body: BodyDescriptor {
                producer: request.producer.clone(),
                digest_uri: request.digest_uri.clone(),
                size: written.size,
                content_type: request.content_type.clone(),
                manifest_id: written.manifest_id,
                generation: version.get(),
                chunk_size: written.chunk_size,
                block_size: written.block_size,
            },
            chunks,
            staged,
        })
    }

    fn read_plan(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
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

    fn snapshot_read_version(&self, snapshot_id: u64) -> Result<Version, MetadError> {
        let pin = self
            .snapshot_pin(snapshot_id)?
            .ok_or(MetadError::NotFound)?;
        Version::new(pin.read_version).map_err(Into::into)
    }

    fn has_active_snapshot_pins(&self) -> Result<bool, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Snapshot,
            prefix: snapshot_pin_prefix(self.mount),
            version: self.read_version()?,
            limit: 1,
            purpose: ReadPurpose::UserStrong,
        })?;
        Ok(!rows.is_empty())
    }

    fn watch_projection(&self, scope: InodeId, event: WatchEvent) -> WatchProjection {
        WatchProjection {
            family: RecordFamily::Watch,
            key: watch_log_prefix(self.mount, scope),
            event: encode_watch_event(&event),
        }
    }

    fn chunk_manifest_delete_and_gc_mutations(
        &self,
        inode: InodeId,
        generation: u64,
        enqueue_version: Version,
    ) -> Result<Vec<Mutation>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::ChunkManifest,
            prefix: chunk_manifest_prefix(self.mount, inode, generation),
            version: self.read_version()?,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mut mutations = Vec::new();
        for row in rows {
            if chunk_index_from_manifest_key(&row.key)? != BODY_SUMMARY_CHUNK_INDEX {
                let manifest = decode_chunk_manifest(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                for (block_index, block) in manifest.blocks.iter().enumerate() {
                    let record = ObjectGcRecord {
                        inode,
                        generation,
                        object_key: block.object_key.clone(),
                        size: block.len,
                        digest_uri: block.digest_uri.clone(),
                        enqueue_version: enqueue_version.get(),
                    };
                    mutations.push(Mutation {
                        family: RecordFamily::Gc,
                        key: gc_object_key(
                            self.mount,
                            enqueue_version.get(),
                            inode,
                            generation,
                            manifest.chunk_index,
                            block_index as u64,
                        ),
                        op: MutationOp::Put,
                        value: Some(Value(encode_object_gc_record(&record))),
                    });
                }
            }
            mutations.push(delete_mutation(RecordFamily::ChunkManifest, row.key));
        }
        Ok(mutations)
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

fn create_watch_kind(kind: CommandKind) -> WatchEventKind {
    match kind {
        CommandKind::PublishArtifact => WatchEventKind::PublishArtifact,
        CommandKind::CreateFile | CommandKind::CreateDir => WatchEventKind::Create,
        _ => WatchEventKind::UpdateAttr,
    }
}

fn watch_cursor_from_key(key: &[u8]) -> Result<WatchCursor, MetadError> {
    let cursor_len = std::mem::size_of::<u64>() * 2;
    if key.len() < cursor_len {
        return Err(MetadError::Codec(
            "watch log key is missing cursor suffix".to_owned(),
        ));
    }
    let offset = key.len() - cursor_len;
    Ok(WatchCursor {
        version: u64::from_be_bytes(
            key[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("watch version has fixed width"),
        ),
        event_id: u64::from_be_bytes(
            key[offset + std::mem::size_of::<u64>()..]
                .try_into()
                .expect("watch event id has fixed width"),
        ),
    })
}

fn chunk_index_from_manifest_key(key: &[u8]) -> Result<u64, MetadError> {
    if key.len() < std::mem::size_of::<u64>() {
        return Err(MetadError::Codec(
            "chunk manifest key is truncated".to_owned(),
        ));
    }
    Ok(u64::from_be_bytes(
        key[key.len() - std::mem::size_of::<u64>()..]
            .try_into()
            .expect("chunk index has fixed width"),
    ))
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
        CommandKind::ReplaceArtifact => b"replace-artifact",
        CommandKind::SnapshotSubtree => b"snapshot-subtree",
        CommandKind::RetireSnapshot => b"retire-snapshot",
        CommandKind::WatchSubtree => b"watch-subtree",
        CommandKind::CleanupObjects => b"cleanup-objects",
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
        service_with_objects().0
    }

    fn service_with_objects() -> (
        NoKvFs<HoltMetadataStore, MemoryObjectStore>,
        MemoryObjectStore,
    ) {
        let objects = MemoryObjectStore::new();
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects.clone(),
        );
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        (service, objects)
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

    fn block_key(inode: InodeId, generation: u64, chunk: u64, block: u64) -> ObjectKey {
        ObjectKey::new(format!(
            "blocks/1/{}/{}/{}/{}",
            inode.get(),
            generation,
            chunk,
            block
        ))
        .unwrap()
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
    fn replace_artifact_preserves_inode_and_returns_old_body() {
        let service = service();
        let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
        let first = service
            .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
            .unwrap();
        let replaced = service
            .replace_artifact(artifact_request(
                name.clone(),
                "checkpoint/new",
                b"new-body",
            ))
            .unwrap();

        assert_eq!(replaced.entry.attr.inode, first.attr.inode);
        assert!(replaced.entry.attr.generation > first.attr.generation);
        assert_eq!(replaced.replaced, Some(first.clone()));
        assert_eq!(
            service.lookup_plus(InodeId::root(), &name).unwrap(),
            Some(replaced.entry.clone())
        );
        assert_eq!(
            service.read_artifact(InodeId::root(), &name).unwrap(),
            b"new-body"
        );
        assert_eq!(
            replaced.replaced.unwrap().body.unwrap().manifest_id,
            "checkpoint/old"
        );
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
    fn remove_file_queues_old_body_for_object_cleanup() {
        let (service, objects) = service_with_objects();
        let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
        let published = service
            .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
            .unwrap();
        let body = published.body.clone().unwrap();
        let object = block_key(published.attr.inode, body.generation, 0, 0);
        assert!(objects.head(&object).unwrap().is_some());

        let removed = service.remove_file(InodeId::root(), &name).unwrap();
        assert_eq!(removed, published);
        assert!(objects.head(&object).unwrap().is_some());

        let cleanup = service.cleanup_pending_objects(100).unwrap();
        assert_eq!(cleanup.scanned, 1);
        assert_eq!(cleanup.attempted, 1);
        assert_eq!(cleanup.deleted, 1);
        assert_eq!(cleanup.missing, 0);
        assert_eq!(cleanup.records_removed, 1);
        assert!(objects.head(&object).unwrap().is_none());
        assert_eq!(
            service.cleanup_pending_objects(100).unwrap(),
            PendingObjectCleanupOutcome::default()
        );
    }

    #[test]
    fn replace_artifact_cleanup_deletes_only_old_generation() {
        let (service, objects) = service_with_objects();
        let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
        let first = service
            .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
            .unwrap();
        let old_body = first.body.clone().unwrap();
        let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
        let replaced = service
            .replace_artifact(artifact_request(
                name.clone(),
                "checkpoint/new",
                b"new-body",
            ))
            .unwrap();
        let new_body = replaced.entry.body.clone().unwrap();
        let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);
        assert!(objects.head(&old_object).unwrap().is_some());
        assert!(objects.head(&new_object).unwrap().is_some());

        let cleanup = service.cleanup_pending_objects(100).unwrap();
        assert_eq!(cleanup.deleted, 1);
        assert_eq!(cleanup.records_removed, 1);
        assert!(objects.head(&old_object).unwrap().is_none());
        assert!(objects.head(&new_object).unwrap().is_some());
        assert_eq!(
            service.read_artifact(InodeId::root(), &name).unwrap(),
            b"new-body"
        );
    }

    #[test]
    fn snapshot_preserves_old_artifact_and_blocks_object_gc_until_retired() {
        let (service, objects) = service_with_objects();
        let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
        let first = service
            .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
            .unwrap();
        let old_body = first.body.clone().unwrap();
        let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
        let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();

        let replaced = service
            .replace_artifact(artifact_request(
                name.clone(),
                "checkpoint/new",
                b"new-body",
            ))
            .unwrap();
        let new_body = replaced.entry.body.clone().unwrap();
        let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);

        assert_eq!(
            service
                .read_artifact_at_snapshot(snapshot.snapshot_id, InodeId::root(), &name)
                .unwrap(),
            b"old"
        );
        assert_eq!(
            service
                .get_attr_at_snapshot(snapshot.snapshot_id, first.attr.inode)
                .unwrap(),
            Some(first.attr.clone())
        );
        assert_eq!(
            service
                .read_file_at_snapshot(snapshot.snapshot_id, first.attr.inode, 0, 3)
                .unwrap(),
            b"old"
        );
        assert_eq!(
            service
                .read_dir_plus_at_snapshot(snapshot.snapshot_id, InodeId::root())
                .unwrap(),
            vec![first.clone()]
        );
        assert_eq!(
            service.read_artifact(InodeId::root(), &name).unwrap(),
            b"new-body"
        );
        let blocked = service.cleanup_pending_objects(100).unwrap();
        assert_eq!(blocked.scanned, 1);
        assert_eq!(blocked.blocked_by_snapshots, 1);
        assert_eq!(blocked.attempted, 0);
        assert!(objects.head(&old_object).unwrap().is_some());
        assert!(objects.head(&new_object).unwrap().is_some());

        assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
        assert!(!service.retire_snapshot(snapshot.snapshot_id).unwrap());
        let cleanup = service.cleanup_pending_objects(100).unwrap();
        assert_eq!(cleanup.deleted, 1);
        assert_eq!(cleanup.records_removed, 1);
        assert!(objects.head(&old_object).unwrap().is_none());
        assert!(objects.head(&new_object).unwrap().is_some());
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
    fn watch_replay_returns_typed_events_after_cursor() {
        let service = service();
        let cursor = service.watch_subtree(InodeId::root()).unwrap();
        let name = DentryName::new(b"runs".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
            .unwrap();

        let events = service.replay_watch(InodeId::root(), cursor, 100).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.kind, WatchEventKind::Create);
        assert_eq!(events[0].event.parent, Some(InodeId::root()));
        assert_eq!(events[0].event.name, Some(name.clone()));
        assert_eq!(events[0].event.inode, created.attr.inode);

        let next_name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
        service
            .publish_artifact(artifact_request(
                next_name.clone(),
                "checkpoint.bin",
                b"body",
            ))
            .unwrap();
        let resumed = service
            .replay_watch(InodeId::root(), events[0].cursor, 100)
            .unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].event.kind, WatchEventKind::PublishArtifact);
        assert_eq!(resumed[0].event.name, Some(next_name));
    }

    #[test]
    fn rename_replay_notifies_old_and_new_parent_scopes() {
        let service = service();
        let old_parent_name = DentryName::new(b"old-parent".to_vec()).unwrap();
        let new_parent_name = DentryName::new(b"new-parent".to_vec()).unwrap();
        let old_parent = service
            .create_dir(InodeId::root(), old_parent_name, 0o755, 1000, 1000)
            .unwrap();
        let new_parent = service
            .create_dir(InodeId::root(), new_parent_name, 0o755, 1000, 1000)
            .unwrap();
        let file_name = DentryName::new(b"artifact".to_vec()).unwrap();
        let source = service
            .create_file(old_parent.attr.inode, file_name.clone(), 0o644, 1000, 1000)
            .unwrap();
        let old_cursor = service.watch_subtree(old_parent.attr.inode).unwrap();
        let new_cursor = service.watch_subtree(new_parent.attr.inode).unwrap();

        service
            .rename(
                old_parent.attr.inode,
                &file_name,
                new_parent.attr.inode,
                file_name.clone(),
            )
            .unwrap();

        let old_events = service
            .replay_watch(old_parent.attr.inode, old_cursor, 100)
            .unwrap();
        assert_eq!(old_events.len(), 1);
        assert_eq!(old_events[0].event.kind, WatchEventKind::Remove);
        assert_eq!(old_events[0].event.inode, source.attr.inode);

        let new_events = service
            .replay_watch(new_parent.attr.inode, new_cursor, 100)
            .unwrap();
        assert_eq!(new_events.len(), 1);
        assert_eq!(new_events[0].event.kind, WatchEventKind::Rename);
        assert_eq!(new_events[0].event.name, Some(file_name));
        assert_eq!(new_events[0].event.inode, source.attr.inode);
    }

    #[test]
    fn watch_replay_survives_service_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let objects = MemoryObjectStore::new();
        let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let name = DentryName::new(b"runs".to_vec()).unwrap();
        let created = service
            .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
            .unwrap();
        drop(service);

        let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
        let events = reopened
            .replay_watch(InodeId::root(), WatchCursor::default(), 100)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.kind, WatchEventKind::Create);
        assert_eq!(events[0].event.name, Some(name));
        assert_eq!(events[0].event.inode, created.attr.inode);
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
    fn pending_object_gc_survives_service_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let objects = MemoryObjectStore::new();
        let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
        let published = service
            .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
            .unwrap();
        let body = published.body.clone().unwrap();
        let object = block_key(published.attr.inode, body.generation, 0, 0);
        service.remove_file(InodeId::root(), &name).unwrap();
        drop(service);

        let reopened =
            NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects.clone()).unwrap();
        let cleanup = reopened.cleanup_pending_objects(100).unwrap();
        assert_eq!(cleanup.deleted, 1);
        assert_eq!(cleanup.records_removed, 1);
        assert!(objects.head(&object).unwrap().is_none());
    }

    #[test]
    fn snapshot_pin_survives_service_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let objects = MemoryObjectStore::new();
        let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
        drop(service);

        let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
        assert_eq!(
            reopened.snapshot_pin(snapshot.snapshot_id).unwrap(),
            Some(snapshot)
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
