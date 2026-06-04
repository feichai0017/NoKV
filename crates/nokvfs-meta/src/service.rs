//! In-process NoKV metadata service.
//!
//! This crate owns the first Rust-native service semantics over the
//! storage-neutral metadata command contract. It compiles namespace operations
//! into `MetadataCommand`s and stores file bodies through an object-store
//! boundary. It does not own Holt trees, Raft replication, FUSE, or protobuf.

mod allocator;
mod command;
mod gc;
mod lifecycle;
mod namespace;
mod publish;
mod read;
mod snapshot;
mod watch;

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::{
    CommandKind, CommitResult, HistoryPruneOutcome, HistoryPruneRequest, MetadataCommand,
    MetadataError, MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, Mutation,
    MutationOp, Predicate, PredicateRef, ReadPurpose, ScanRequest, Value, Version, WatchProjection,
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
    delete_staged_objects, put_chunked_object, put_chunked_ranges,
    put_chunked_ranges_with_block_index_base, read_object_blocks, ChunkWriteOptions,
    ChunkWriteRange, ChunkedWrite, MemoryBlockCache, ObjectCleanupOutcome, ObjectError, ObjectKey,
    ObjectReadBlock, ObjectStore, StagedObjectSet, StoredChunk, DEFAULT_BLOCK_SIZE,
    DEFAULT_CHUNK_SIZE,
};
use nokvfs_types::{
    parse_absolute_path, BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName,
    DentryProjection, DentryRecord, FileType, InodeAttr, InodeId, ModelError, MountId,
    ObjectGcRecord, PathError, PathMetadata, RecordFamily, SnapshotPin, WatchCursor, WatchEvent,
    WatchEventKind, WatchRecord,
};
use sha2::{Digest, Sha256};

const BODY_SUMMARY_CHUNK_INDEX: u64 = u64::MAX;
const ALLOCATOR_VERSION_RESERVATION: u64 = 1024;
const ALLOCATOR_INODE_RESERVATION: u64 = 1024;
const BODY_DIGEST_CHUNK_SIZE: usize = 8 * 1024 * 1024;

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
    // These values are durable reservation upper bounds. Recovery may skip
    // unused ids after a crash, but must never reuse a visible version/inode.
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifactRange {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifactSession {
    pub parent: InodeId,
    pub name: DentryName,
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub size: u64,
    pub ranges: Vec<PublishArtifactRange>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifactStagedSession {
    pub parent: InodeId,
    pub name: DentryName,
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub size: u64,
    pub chunks: Vec<StoredChunk>,
    pub staged: StagedObjectSet,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub mtime_ms: Option<u64>,
    pub ctime_ms: Option<u64>,
}

impl UpdateAttr {
    fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.uid.is_none()
            && self.gid.is_none()
            && self.size.is_none()
            && self.mtime_ms.is_none()
            && self.ctime_ms.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifact {
    pub parent: InodeId,
    pub name: DentryName,
    pub inode: InodeId,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
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
pub struct BodyReadPlan {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
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
    InvalidPreparedArtifact(String),
    StaleBodyGeneration {
        expected: u64,
        current: u64,
    },
    AllocatorExhausted,
    InvalidPath(String),
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
    allocator_gate: Mutex<()>,
    clock: AtomicU64,
    reserved_version: AtomicU64,
    next_inode: AtomicU64,
    reserved_next_inode: AtomicU64,
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
    fn resized_body_digest_uri(
        &self,
        inode: InodeId,
        old_body: Option<&BodyDescriptor>,
        new_size: u64,
        read_version: Version,
    ) -> Result<String, MetadError> {
        let mut hasher = Sha256::new();
        let mut offset = 0_u64;
        let old_size = old_body.map(|body| body.size).unwrap_or(0);
        let old_prefix_len = old_size.min(new_size);

        if let Some(body) = old_body {
            while offset < old_prefix_len {
                let requested = usize::try_from((old_prefix_len - offset).min(
                    u64::try_from(BODY_DIGEST_CHUNK_SIZE).map_err(|_| ObjectError::InvalidRange)?,
                ))
                .map_err(|_| ObjectError::InvalidRange)?;
                let bytes =
                    self.read_file_at_version(inode, body, offset, requested, read_version)?;
                if bytes.is_empty() {
                    return Err(ObjectError::InvalidRange.into());
                }
                hasher.update(&bytes);
                offset = offset
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| ObjectError::InvalidRange)?)
                    .ok_or(ObjectError::InvalidRange)?;
            }
        }

        let mut zero_remaining = new_size.saturating_sub(old_prefix_len);
        if zero_remaining > 0 {
            let zeros = vec![0_u8; BODY_DIGEST_CHUNK_SIZE];
            while zero_remaining > 0 {
                let len = usize::try_from(zero_remaining.min(
                    u64::try_from(BODY_DIGEST_CHUNK_SIZE).map_err(|_| ObjectError::InvalidRange)?,
                ))
                .map_err(|_| ObjectError::InvalidRange)?;
                hasher.update(&zeros[..len]);
                zero_remaining -= u64::try_from(len).map_err(|_| ObjectError::InvalidRange)?;
            }
        }

        let digest = hasher.finalize();
        Ok(format!("sha256:{digest:x}"))
    }
}

impl<M, O> NoKvFs<M, O> where M: MetadataStore + MetadataStoreStatsProvider {}

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
                    last_commit_version = last_commit_version.max(attr.generation);
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

fn reservation_upper_bound(required: u64, reservation: u64) -> u64 {
    required.saturating_add(reservation)
}

fn directory_attr(inode: InodeId, mode: u32, uid: u32, gid: u32, version: u64) -> InodeAttr {
    let now_ms = current_time_ms();
    InodeAttr {
        inode,
        file_type: FileType::Directory,
        mode,
        uid,
        gid,
        size: 0,
        generation: version,
        mtime_ms: now_ms,
        ctime_ms: now_ms,
    }
}

fn current_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

fn body_digest_uri(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn delete_mutation(family: RecordFamily, key: Vec<u8>) -> Mutation {
    Mutation {
        family,
        key,
        op: MutationOp::Delete,
        value: None,
    }
}

fn ensure_unique_names(names: &[DentryName]) -> Result<(), MetadError> {
    let mut seen = HashSet::with_capacity(names.len());
    for name in names {
        if !seen.insert(name.as_bytes()) {
            return Err(MetadError::InvalidPath(format!(
                "duplicate dentry name {} in batched create",
                String::from_utf8_lossy(name.as_bytes())
            )));
        }
    }
    Ok(())
}

fn create_watch_kind(kind: CommandKind) -> WatchEventKind {
    match kind {
        CommandKind::PublishArtifact => WatchEventKind::PublishArtifact,
        CommandKind::CreateFile
        | CommandKind::CreateFiles
        | CommandKind::CreateDir
        | CommandKind::CreateSymlink => WatchEventKind::Create,
        _ => WatchEventKind::UpdateAttr,
    }
}

fn validate_prepared_artifact(
    prepared: &PreparedArtifact,
    body: &BodyDescriptor,
    chunks: &[ChunkManifest],
) -> Result<(), MetadError> {
    if body.generation != prepared.generation {
        return Err(MetadError::InvalidPreparedArtifact(format!(
            "body generation {} does not match prepared generation {}",
            body.generation, prepared.generation
        )));
    }
    if body.chunk_size == 0 || body.block_size == 0 {
        return Err(ObjectError::InvalidChunkLayout.into());
    }
    let mut covered = 0_u64;
    for chunk in chunks {
        let chunk_end = chunk
            .logical_offset
            .checked_add(chunk.len)
            .ok_or(ObjectError::InvalidRange)?;
        if chunk_end > body.size {
            return Err(MetadError::InvalidPreparedArtifact(
                "chunk manifest exceeds body size".to_owned(),
            ));
        }
        covered = covered.saturating_add(chunk.len);
        for block in &chunk.blocks {
            let block_end = block
                .logical_offset
                .checked_add(block.len)
                .ok_or(ObjectError::InvalidRange)?;
            if block_end > chunk_end || block.logical_offset < chunk.logical_offset {
                return Err(MetadError::InvalidPreparedArtifact(
                    "block descriptor is outside chunk range".to_owned(),
                ));
            }
        }
    }
    if covered != body.size {
        return Err(MetadError::InvalidPreparedArtifact(format!(
            "chunk manifests cover {covered} bytes but body size is {}",
            body.size
        )));
    }
    Ok(())
}

fn validate_artifact_ranges(request: &PublishArtifactSession) -> Result<(), MetadError> {
    let mut ranges = request
        .ranges
        .iter()
        .filter(|range| !range.bytes.is_empty())
        .collect::<Vec<_>>();
    ranges.sort_by_key(|range| range.offset);
    let mut previous_end = 0_u64;
    for range in ranges {
        let len = u64::try_from(range.bytes.len()).map_err(|_| ObjectError::InvalidRange)?;
        let end = range
            .offset
            .checked_add(len)
            .ok_or(ObjectError::InvalidRange)?;
        if end > request.size {
            return Err(MetadError::InvalidPreparedArtifact(
                "dirty range exceeds session body size".to_owned(),
            ));
        }
        if range.offset < previous_end {
            return Err(MetadError::InvalidPreparedArtifact(
                "dirty ranges must not overlap".to_owned(),
            ));
        }
        previous_end = end;
    }
    Ok(())
}

fn merge_session_chunks(
    size: u64,
    old_chunks: Vec<ChunkManifest>,
    dirty_chunks: Vec<StoredChunk>,
) -> Result<Vec<ChunkManifest>, MetadError> {
    let mut chunks = BTreeMap::<u64, ChunkManifest>::new();
    if size > 0 {
        let last_chunk = (size - 1) / DEFAULT_CHUNK_SIZE;
        for chunk_index in 0..=last_chunk {
            ensure_manifest_chunk(&mut chunks, chunk_index, size);
        }
    }
    for old_chunk in old_chunks {
        for block in old_chunk.blocks {
            let Some(block) = clip_block_to_size(block, size)? else {
                continue;
            };
            let chunk_index = block.logical_offset / DEFAULT_CHUNK_SIZE;
            ensure_manifest_chunk(&mut chunks, chunk_index, size)
                .blocks
                .push(block);
        }
    }
    for dirty_chunk in dirty_chunks {
        for block in dirty_chunk.blocks {
            let block = BlockDescriptor {
                object_key: block.object_key,
                logical_offset: block.logical_offset,
                object_offset: block.object_offset,
                len: block.len,
                digest_uri: block.digest_uri,
            };
            let Some(block) = clip_block_to_size(block, size)? else {
                continue;
            };
            let chunk_index = block.logical_offset / DEFAULT_CHUNK_SIZE;
            ensure_manifest_chunk(&mut chunks, chunk_index, size)
                .blocks
                .push(block);
        }
    }
    Ok(chunks.into_values().collect())
}

fn ensure_manifest_chunk(
    chunks: &mut BTreeMap<u64, ChunkManifest>,
    chunk_index: u64,
    size: u64,
) -> &mut ChunkManifest {
    chunks.entry(chunk_index).or_insert_with(|| {
        let logical_offset = chunk_index.saturating_mul(DEFAULT_CHUNK_SIZE);
        let len = if logical_offset >= size {
            0
        } else {
            DEFAULT_CHUNK_SIZE.min(size - logical_offset)
        };
        ChunkManifest {
            chunk_index,
            logical_offset,
            len,
            blocks: Vec::new(),
        }
    })
}

fn clip_block_to_size(
    mut block: BlockDescriptor,
    size: u64,
) -> Result<Option<BlockDescriptor>, MetadError> {
    if block.logical_offset >= size {
        return Ok(None);
    }
    let max_len = size - block.logical_offset;
    block.len = block.len.min(max_len);
    if block.len == 0 {
        return Ok(None);
    }
    block
        .logical_offset
        .checked_add(block.len)
        .ok_or(ObjectError::InvalidRange)?;
    Ok(Some(block))
}

fn chunk_object_keys(chunks: &[ChunkManifest]) -> HashSet<String> {
    chunks
        .iter()
        .flat_map(|chunk| chunk.blocks.iter().map(|block| block.object_key.clone()))
        .collect()
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

fn allocator_reservation_request_id(
    mount: MountId,
    commit_version: Version,
    reserved_version: u64,
    reserved_next_inode: u64,
) -> Vec<u8> {
    let prefix = b"reserve-allocator";
    let mut out = Vec::with_capacity(prefix.len() + 32);
    out.extend_from_slice(prefix);
    out.extend_from_slice(&mount.get().to_be_bytes());
    out.extend_from_slice(&commit_version.get().to_be_bytes());
    out.extend_from_slice(&reserved_version.to_be_bytes());
    out.extend_from_slice(&reserved_next_inode.to_be_bytes());
    out
}

fn kind_name(kind: CommandKind) -> &'static [u8] {
    match kind {
        CommandKind::ReserveAllocator => b"reserve-allocator",
        CommandKind::CreateFile => b"create-file",
        CommandKind::CreateFiles => b"create-files",
        CommandKind::CreateDir => b"create-dir",
        CommandKind::CreateSymlink => b"create-symlink",
        CommandKind::UpdateAttr => b"update-attr",
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

impl From<PathError> for MetadError {
    fn from(err: PathError) -> Self {
        Self::InvalidPath(err.to_string())
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
            Self::InvalidPreparedArtifact(err) => {
                write!(f, "invalid prepared artifact: {err}")
            }
            Self::StaleBodyGeneration { expected, current } => write!(
                f,
                "body generation {expected} is stale; current generation is {current}"
            ),
            Self::AllocatorExhausted => write!(f, "inode allocator is exhausted"),
            Self::InvalidPath(err) => write!(f, "invalid path: {err}"),
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
mod tests;
